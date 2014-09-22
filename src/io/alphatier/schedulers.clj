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
  (:require [io.alphatier.pools :as pools]))

;; ## Commits
;;
;; All changes to the pool by a scheduler have to go through a commit. A scheduler can do three actions:
;;
;; * **create** new tasks, assigned to an executor
;; * **update** a task's metadata in order to influence the task during runtime
;; * **kill** a task
(defrecord Commit [scheduler-id tasks allow-partial-commit])

(defrecord Result [rejected-tasks pre-snapshot post-snapshot])


(defn- create-task
  "Creating a tasks registers it in the pool and assigns it to the given executor."
  [pool task]
  (let [task (pools/map->Task (merge
                                {:lifecycle-phase :create
                                 :metadata-version 0}
                                task))]
    (alter pool assoc-in [:tasks (:id task)] task)
    (alter pool update-in [:executors (:executor-id task) :task-ids] conj (:id task))
    (alter pool update-in [:executors (:executor-id task) :tasks-version] inc)))

(defn- update-task
  "Updating a task modifies the task's metadata."
  [pool task]
  (alter pool update-in [:tasks (:id task) :metadata] merge (:metadata task))
  (alter pool update-in [:tasks (:id task) :metadata-version] inc))

(defn- kill-task
  "Killing a tasks sets its lifecycle-phase to `:kill`, so that the executor can actually kill it."
  [pool task]
  (alter pool assoc-in [:tasks (:id task) :lifecycle-phase] :kill))

;; All given tasks must provide the action you want to perform in the `:action` key.
(def ^:private commit-actions {:create create-task
                               :update update-task
                               :kill kill-task})

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

* run all pre constraint checks
* apply the requested the actions
* run all post constraint checks

Depending on the `:allow-partial-commit`, the whole commit may get rejected if one task does not get approved by a
constraint.

Using the `:force` flag disables all constraint checks and effectivly allows the commit to go through. This is mainly
intended to relpay already committed commits (e.g. in an replication mode).

You can not issue two actions for the same task at once."
  [pool commit & {:keys [force]
                  :or {force false}}]

  ; TODO check for duplicate task IDs
  ; TODO validate input - does executor id exist? etc

  (dosync
    (let [pre-snapshot @pool
          rejections (atom [])]

      ; Phase 1: check pre constraints
      (when-not force
        (swap! rejections (partial reduce
                                   (fn [rej [_ constraint]]
                                     (into rej (constraint commit pre-snapshot))))
               (-> pre-snapshot :constraints :pre)))

      ; TODO if not partial commit, throw exception

      ; TODO filter out tasks that got rejected

      ; Phase 2: apply the actions
      (doseq [task (:tasks commit)]
        (let [action (-> task :action commit-actions)]
          (action pool task)))

      (let [post-snapshot @pool]

        ; Phase 3: check post constraints
        (when-not force
          (swap! rejections (partial reduce
                                     (fn [rej [_ constraint]]
                                       (into rej (constraint commit pre-snapshot post-snapshot))))
                            (-> post-snapshot :constraints :post)))

        ; reject?
        (if (not (empty? @rejections))  ; TODO allow partial commits
          (throw (ex-info (str "commit rejected:" @rejections) (map->Result {:accepted-tasks []
                                                          :rejected-tasks @rejections
                                                          :pre-snapshot pre-snapshot
                                                          :post-snapshot post-snapshot}))))

        ; accept!
        (map->Result {:accepted-tasks [] ; TODO real value
                      :rejected-tasks @rejections
                      :pre-snapshot pre-snapshot
                      :post-snapshot post-snapshot})))))
