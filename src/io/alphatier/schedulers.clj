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

; TODO metadata/tasks-version resets
(defn- create-task
  "Creating a tasks registers it in the pool and assigns it to the given executor."
  [pool task]
  (alter pool assoc-in [:tasks (:id task)] task)
  (alter pool update-in [:executors (:executor-id task) :task-ids] conj (:id task))
  (alter pool update-in [:executors (:executor-id task) :tasks-version] inc))

(defn- update-task
  "Updating a task modifies the task's metadata."
  [pool task]
  (alter pool update-in [:tasks (:id task) :metadata] merge (:metadata task))
  (alter pool update-in [:tasks (:id task) :metadata-version] inc))

(defn- kill-task
  "Killing a tasks sets its lifecycle-phase to `:kill`, so that the executor can actually kill it."
  [pool task]
  (alter pool assoc-in [:tasks (:id task) :lifecycle-phase] :kill))


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

To execute a commit, you need to provide at least the identifier for your scheduler. The scheduler ID will be used to
apply user defined constraints to it. The following options are available:

* `:create` contains a list of all tasks to create
* `:update` contains a list of all tasks to update
* `:kill` contains a list of all tasks to kill
* `:allow-partial` is a boolean describing if single tasks should succeed even if other tasks fail
* `:force` disables all constraint checks

All task list entries contain at least the task ID the request is for. Every entry may include the following counters
for optimistic locking:

* `:executor-metadata-version`
* `:executor-tasks-version`
* `:task-metadata-version`

If set, the versions will be checked with the current pool state and rejected if not equal."
  [pool scheduler-id & {:keys [create update kill allow-partial-commit force]
                        :or {create []
                             update []
                             kill []
                             allow-partial-commit false
                             force false}}]
  (dosync
    (let [pre-snapshot (pools/get-snapshot pool)]
      (when-not force
        (comment "TODO run pre-constraints"))
      ; TODO fail fast if rejections and no partial commit

      (doseq [task create] (create-task pool task))
      (doseq [task update] (update-task pool task))
      (doseq [task kill] (kill-task pool task))

      ; TODO postcheck all constrains (resource limits, scheduler limits, ...)
      (when-not force
        (comment "TODO run post-constraints"
                 (throw (ex-info "commit rejected" {:rejected-tasks []
                                                    :pre-snapshot pre-snapshot}))))

      {:rejected-tasks []
       :pre-snapshot pre-snapshot
       :post-snapshot (pools/get-snapshot pool)})))
