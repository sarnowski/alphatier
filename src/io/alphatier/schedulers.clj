;; ## Schedulers
;;
;; Schedulers are the decision maker in the Alphatier world. They are able to create and kill tasks every time they like
;; to. Possible scenarios are server cluster management where every executor represents one host in your data centers
;; and a "service scheduler" which can let the hosts (executors) start or stop applications on them. Another example
;; scenario is your office kitchen where each coffee machine can brew 4 coffees at a time. Every coffee machine
;; can be represented with an executor and you write a scheduler that is able to manage which coffee machine brews which
;; colleague's coffee.
;;
;;
;; ### ACID without D
;;
;; Alphatier changes are [ACID](http://en.wikipedia.org/wiki/ACID) without the D (durable). **A**tomicity is optional
;; for the scheduler, **C**onsistency and **I**solation are guarenteed. The pools are an in-memory datastructures and
;; therefor aren't durable across JVM restarts.
;;
;; Task changes on the pool happen via a commit. A commit may contain multiple creation or kill requests for tasks.
;; Every accepted action itself fulfills the atomicity criteria. Optionally, a scheduler can force atomicity for the
;; whole commit.
;;
;; Consistency is ensured through various [constraints](#io.alphatier.constraints). Those can also include user defined
;; constraints. Built-in is optimistic locking of executor or task data and protection against resource overbooking.
;;
;; Isolation is provided through [Clojure's STM](http://clojure.org/refs). If writes happen to the state while a
;; transaction runs, the transaction will be repeated with the new state.
(ns io.alphatier.schedulers
  (:require [io.alphatier.pools :as pools]
            [clojure.set :refer [intersection union superset?]]))

;; ## Commits
;;
;; All changes to the pool by a scheduler have to go through a commit. A scheduler can do three actions:
;;
;; * **create** new tasks, assigned to an executor
;; * **update** a task's metadata in order to influence the task during runtime
;; * **kill** a task
(defrecord Commit [scheduler-id actions allow-partial-commit])

(defrecord Result [accepted-actions rejected-actions pre-snapshot post-snapshot])


(defn- create-task
  "Creating a tasks registers it in the pool and assigns it to the given executor."
  [pool scheduler-id action]
  (let [task (pools/map->Task (merge
                                {:scheduler-id scheduler-id
                                 :lifecycle-phase :create
                                 :metadata-version 0}
                                action))]
    (alter pool assoc-in [:tasks (:id task)] task)
    (alter pool update-in [:executors (:executor-id task) :task-ids] conj (:id task))
    (alter pool update-in [:executors (:executor-id task) :task-ids-version] inc)))

(defn- update-task
  "Updating a task modifies the task's metadata."
  [pool scheduler-id task]
  (alter pool update-in [:tasks (:id task) :metadata] merge (:metadata task))
  (alter pool update-in [:tasks (:id task) :metadata-version] inc))

(defn- kill-task
"Killing a tasks sets its lifecycle-phase to `:kill`, so that the executor can actually kill it."
  [pool scheduler-id task]
  (alter pool assoc-in [:tasks (:id task) :lifecycle-phase] :kill))

;; All given tasks must provide the action you want to perform in the `:action` key.
(def ^:private commit-actions {:create create-task
                               :update update-task
                               :kill kill-task})

(defn- validate-commit
"Commits will get validated upon commit. Valid commit satisfy all of the following criteria:

* it doesn't multiple actions for the same task
* it doesn't contain duplicate `:create` actions for the same task
* it doesn't contain attempt to `:update` or `:kill` a missing task
* it doesn't reference a missing executor, i.e. invalid `:executor-id`
* it **does** specify all resources the corresponding executor is providing"
  [commit pre-snapshot]

  (let [task-ids (->> commit :actions (map :id))]
    (when (distinct? (-> task-ids sort) (-> task-ids set sort))
      (throw (ex-info "Commit contains duplicate tasks" {}))))

  (let [create-actions (->> commit :actions (filter (comp (partial = :create) :type)))]
    (when (not-empty (intersection (set (map :id create-actions)) (-> pre-snapshot :tasks keys set)))
      (throw (ex-info "Commit contains duplicate create tasks" {})))
    (when (not-empty (intersection (-> (map keys create-actions) flatten set)
                                   #{:scheduler-id :lifecycle-phase :metadata-version}))
      (throw (ex-info "Commit contains illegal properties in create actions" {}))))

  (doseq [type [:update :kill]]
    (let [given-task-ids (->> commit :actions (filter #(= (:type %) type)) (map :id) set)
          existing-task-ids (->> pre-snapshot :tasks vals (map :id) set)]
      (when (and (not-empty given-task-ids)
                 (not (superset? existing-task-ids given-task-ids)))
        (throw (ex-info (str "Commit contains reference to missing task for " (name type)) {})))))

  (doseq [executor-id (->> commit :actions (filter (comp (partial = :create) :type)) (map :executor-id))]
    (when-not (contains? (-> pre-snapshot :executors) executor-id)
      (throw (ex-info (str "Commit contains reference to missing executor " executor-id) {})))
    (let [executor (-> pre-snapshot :executors (get executor-id))
          actions (->> commit :actions (filter (comp #(= executor-id %) :executor-id)))
          given-resources (->> actions (map (comp keys :resources)) flatten set)
          existing-resources (->> executor :resources keys set)]
      (when-not (= given-resources existing-resources)
        (throw (ex-info "Commit contains missing resource" {}))))))

(defn- reject-if-needed
"Actions may get rejected during commit by different pre/post-commit constraints.
Commits are considered rejected if

* all of its actions are rejected
* at least one of its actions is rejected but partial commits are disabled

In all other cases the commit is accepted."
  [commit result]
  (let [allow-partial-commit? (-> commit :allow-partial-commit)
        total (-> commit :actions count)
        rejects (-> result :rejected-actions vals flatten set count)]
    (if (or (and allow-partial-commit? (= rejects total))
            (and (not allow-partial-commit?) (> rejects 0)))
      (throw (ex-info (str "commit rejected (total: " total " rejects: " rejects " partial: " allow-partial-commit? ")")
                      (map->Result result))))))

;; ### How to implement a scheduler?
;;
;; The basic workflow for a scheduler looks like the following:
;;
;; 1. get a [current snapshot](#io.alphatier.pools) of the pool
;; 2. do some number crunching in order to fulfill your current actions (e.g. find good fitting executors for your new
;;    tasks or some tasks that you want to shut down)
;; 3. commit your decision to the pool

(defn commit
"### Committing

A commit has 3 phases:

* run all pre-commit constraint checks
* apply the requested the actions
* run all post-commit constraint checks

Depending on the `:allow-partial-commit`, the whole commit may get rejected if one actions does not get approved by a
constraint.

Using the `:force` flag disables all constraint checks and effectivly allows the commit to go through. This is mainly
intended to replay already committed commits (e.g. in an replication mode).

You can not issue two actions for the same task at once."
  [pool commit & {:keys [force] :or {force false}}]

  (dosync
    (let [pre-snapshot @pool
          rejections (atom {})]

      (validate-commit commit pre-snapshot)

      ; Phase 1: check pre constraints
      (when-not force
        (swap! rejections
               (partial merge-with into)
               (into {} (map (fn [[name constraint]] [name (constraint commit pre-snapshot)])
                             (-> pre-snapshot :constraints :pre))))
        (reject-if-needed commit {:accepted-actions []
                                  :rejected-actions @rejections
                                  :pre-snapshot pre-snapshot
                                  :post-snapshot nil}))

      (let [pre-rejected-actions (-> @rejections vals flatten set)]
        ; Phase 2: apply the actions
        (doseq [action (->> commit :actions (filter (complement pre-rejected-actions)))]
          (let [commit-action (-> action :type commit-actions)]
            (commit-action pool (:scheduler-id commit) action))))

      (let [post-snapshot @pool]

        ; Phase 3: check post constraints
        (when-not force
          (swap! rejections
                 (partial merge-with into)
                 (into {} (map (fn [[name constraint]] [name (constraint commit pre-snapshot post-snapshot)])
                               (-> post-snapshot :constraints :post)))))

        (let [post-rejected-actions (-> @rejections vals flatten set)
              accepted-actions (->> commit :actions (filter (complement post-rejected-actions)))
              result {:accepted-actions accepted-actions
                      :rejected-actions @rejections
                      :pre-snapshot pre-snapshot
                      :post-snapshot post-snapshot}]

          (reject-if-needed commit result)

          ; accept!
          (map->Result result))))))
