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
  (:require [clojure.core.typed :refer [ann-record defalias ann Any Fn IFn ASeq Map doseq]]
            [clojure.set :refer [intersection union superset?]]
            [io.alphatier.pools :refer :all])
  (:import [io.alphatier.pools Pool Task Commit]))

(ann-record Result [accepted-actions :- (ASeq Action)
                    rejected-actions :- (Map Any (ASeq Action))
                    pre-snapshot :- Snapshot
                    post-snapshot :- Snapshot])
(defrecord Result [accepted-actions rejected-actions pre-snapshot post-snapshot])

(ann ^:no-check add-task [Pool Task -> Pool])
(defn- add-task [pool task]
  (let [task-id (:id task)
        executor-id (:executor-id task)]
    (-> pool
        (assoc-in [:tasks task-id] task)
        (update-in [:executors executor-id :task-ids] conj task-id)
        (update-in [:executors executor-id :task-ids-version] inc))))

(defalias CommitAction [PoolRef Any Action -> Pool])

(ann create-task CommitAction)
(defn- create-task
  "Creating a tasks registers it in the pool and assigns it to the given executor."
  [pool scheduler-id action]
  (let [task (map->Task (merge {:id (:id action)
                                :executor-id (:executor-id action)
                                :scheduler-id scheduler-id
                                :lifecycle-phase :create
                                :resources (:resources action)
                                :metadata (or (:metadata action) {})
                                :metadata-version 0}))]
    (alter pool add-task task)))

(ann ^:no-check update-metadata [Pool Action -> Pool])
(defn- update-metadata [pool action]
  (let [task-id (:id action)]
    (-> pool
        (update-in [:tasks task-id :metadata] merge (:metadata action))
        (update-in [:tasks task-id :metadata-version] inc))))

(ann update-task CommitAction)
(defn- update-task
  "Updating a task modifies the task's metadata."
  [pool scheduler-id action]
  (alter pool update-metadata action))

(ann ^:no-check kill-task CommitAction)
(defn- kill-task
"Killing a tasks sets its lifecycle-phase to `:kill`, so that the executor can actually kill it."
  [pool scheduler-id action]
  (alter pool assoc-in [:tasks (:id action) :lifecycle-phase] :kill))

;; All given tasks must provide the action you want to perform in the `:action` key.
(ann ^:no-check commit-actions [ActionType -> CommitAction])
(def ^:private commit-actions {:create create-task
                               :update update-task
                               :kill kill-task})

(ann ^:no-check validate-commit [Commit Snapshot -> Any])
(defn- validate-commit
"Commits will get validated upon commit. Valid commit satisfy all of the following criteria:

* it doesn't multiple actions for the same task
* it doesn't contain duplicate `:create` actions for the same task
* it doesn't contain attempt to `:update` or `:kill` a missing task
* it doesn't reference a missing executor, i.e. invalid `:executor-id`
* it **does** specify all resources the corresponding executor is providing"
  [commit pre-snapshot]

  (let [task-ids (->> commit :actions (map :id))]
    (when (distinct? (sort task-ids) (-> task-ids set sort))
      (throw (ex-info "Commit contains duplicate tasks" {}))))

  (let [create-actions (->> commit :actions (filter (comp (partial = :create) :type)))]
    (when (not-empty (intersection (set (map :id create-actions)) (-> pre-snapshot :tasks keys set)))
      (throw (ex-info "Commit contains duplicate create tasks" {})))
    (when (not-empty (intersection (-> (map keys create-actions) flatten set)
                                   #{:scheduler-id :lifecycle-phase :metadata-version}))
      (throw (ex-info "Commit contains illegal properties in create actions" {})))

    (doseq [type :- ActionType [:update :kill]]
      (let [given-task-ids (->> commit :actions (filter #(= (:type %) type)) (map :id) set)
            existing-task-ids (->> pre-snapshot :tasks vals (map :id) set)]
        (when (and (not-empty given-task-ids)
                   (not (superset? existing-task-ids given-task-ids)))
          (throw (ex-info (str "Commit contains reference to missing task for " (name type)) {})))))

    (doseq [executor-id (map :executor-id create-actions)]
      (when-not (contains? (:executors pre-snapshot) executor-id)
        (throw (ex-info (str "Commit contains reference to missing executor " executor-id) {})))
      (let [executor (-> pre-snapshot :executors (get executor-id))
            actions (->> commit :actions (filter (comp #(= executor-id %) :executor-id)))
            given-resources (->> actions (map (comp keys :resources)) flatten set)
            existing-resources (->> executor :resources keys set)]
        (when-not (= given-resources existing-resources)
          (throw (ex-info "Commit contains missing resource" {})))))))

(ann ^:no-check reject-if-necessary [Commit Result -> Any])
(defn- reject-if-necessary
"Actions may get rejected during commit by different pre/post-commit constraints.
Commits are considered rejected if

* all of its actions are rejected
* at least one of its actions is rejected but partial commits are disabled

In all other cases the commit is accepted."
  [commit result]
  (let [allow-partial-commit? (:allow-partial-commit commit)
        total (-> commit :actions count)
        rejects (-> result :rejected-actions vals flatten set count)]
    (if (or (and allow-partial-commit? (= rejects total))
            (and (not allow-partial-commit?) (pos? rejects)))
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

(ann ^:no-check commit [PoolRef Commit & :optional {:force Boolean} -> Result])
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
          rejections (atom {})] ;TODO since we only use this locally, there has to be a better way

      (validate-commit commit pre-snapshot)

      ; Phase 1: check pre constraints
      (when-not force
        (swap! rejections
               (partial merge-with into)
               (into {} (map (fn [[name constraint]] [name (constraint commit pre-snapshot)])
                             (-> pre-snapshot :constraints :pre))))
        (reject-if-necessary commit {:accepted-actions []
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

          (reject-if-necessary commit result)

          ; accept!
          (map->Result result))))))
