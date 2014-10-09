;; ## Basic Concept
;;
;; At its core, Alphatier is a database for keeping executor resource capacities and running tasks. Multiple executors
;; can be registered in an Alphatier pool. On the other side, multiple schedulers can commit tasks to the pool of
;; executors. The following diagram shows the basic workflow:
;;
;; ![Alphatier Workflow](doc/workflow-generated.png)
;;
;; 1. Executors get registered in the Alphatier pool and announce their resource capacities.
;; 2. A scheduler has a new task that requires a list of resources to execute.
;; 3. The scheduler fetches the current state of the pool (which executors are registered, how much resources do they
;;    have currently available, which tasks is an executor already on).
;; 4. Based on the current state, the scheduler computes which executors should execute the task and commits its
;;    decision to Alphatier.
;; 5. The executors receive the request to execute the task and start doing their jobs.
;;
;; Alphatier makes sure, that certain constraints are met when schedulers want to commit new tasks to the pool. At a
;; basic minimum, Alphatier checks that resources are not exceeded. It can also check for other criteria based on
;; [predefined constraints](#io.alphatier.constraints).
;;
(ns io.alphatier.pools
  (:require [clojure.core.typed :refer [defalias ann-record ann Map HMap Any U Ref1 ASeq]]))

;; ### Tasks
;;
;; Tasks are execution requests for executors. A task has an unique identifier. It holds the following information:
;;
;; * `executor-id` is the identifier of the executor this task is assigned to.
;; * `lifecycle-phase` can be one of the following: `:create`, `:created` and `:kill`.
;; * `resources` works similar to the `resources` information on an executor but represents the amount of resources
;;   that must be reserved and guaranteed for this task.
;; * `metadata` and `metadata-version` work exactly like the `metadata` on an executor but provided by the scheduler.

(defalias Resources (Map Any Number))
(defalias Metadata (Map Any Any))

(defalias LifecyclePhase (U ':create ':creating ':created ':kill ':killing))

(ann-record Task [id :- Any
                  executor-id :- Any
                  scheduler-id :- Any
                  lifecycle-phase :- LifecyclePhase
                  resources :- Resources
                  metadata :- Metadata
                  metadata-version :- Number])
(defrecord Task
           [id executor-id scheduler-id lifecycle-phase resources metadata metadata-version])

;; ### Executors
;;
;; Executors are the workforce of a Alphatier. They have an unique identifier and a status that is `:registered` or
;; `:unregistered`. In addition, they keep various information about themself and their current work:
;;
;; * `resources` is a map of resources (strings) with a numeric value. The resource must be countable like "4 slots",
;;   "36 CPU cores", "5 TB disk space", "8 coffee filters", etc.
;; * `metadata` is a free map of information, provided by the executor to provide some more detailed informations about
;;   the executor to the scheduler.
;; * `metadata-version` is a counter that gets incremented every time the `metadata` change. It can be used for
;;   optimistic locking.
;; * `task-ids` is a list of task IDs that are assigned to this executor.
;; * `task-ids-version` works similar to the `metadata-version` as it is a counter thats gets incremented when the
;;   `task-ids` list changes.
(defalias State (U ':registered ':unregistered))
(ann-record Executor [id :- Any
                      status :- State
                      resources :- Resources
                      metadata :- Metadata
                      metadata-version :- Number
                      task-ids :- (ASeq Any)
                      task-ids-version :- Number])
(defrecord Executor
           [id status resources metadata metadata-version task-ids task-ids-version])

;; ## Commits
;;
;; All changes to the pool by a scheduler have to go through a commit. A scheduler can do three actions:
;;
;; * **create** new tasks, assigned to an executor
;; * **update** a task's metadata in order to influence the task during runtime
;; * **kill** a task

(defalias ActionType (U ':create ':update ':kill))
(defalias Action (HMap :mandatory {:id Any
                                   :type ActionType
                                   :executor-id Any
                                   :resources Resources}
                       :optional {:metadata Metadata}
                       :complete? true))

(defrecord Commit [scheduler-id actions allow-partial-commit])
(ann-record Commit [scheduler-id :- Any
                    actions :- (ASeq Action)
                    allow-partial-commit :- Boolean])

(defalias Executors (Map Any Executor))
(defalias Tasks (Map Any Task))
(defalias Snapshot '{:executors Executors :tasks Tasks})

(defalias ConstraintType (U ':pre ':post))
; TODO those should return (Coll Action)
(defalias PreConstraint [Commit Snapshot -> (ASeq Action)])
(defalias PostConstraint [Commit Snapshot Snapshot -> (ASeq Action)])
(defalias Constraint (U PreConstraint PostConstraint))

;; ### Pools
;;
;; A pool represents a snapshot of a set of executors and their tasks. Both executors and tasks are packaged in maps
;; with their IDs as key.
(ann-record Pool [executors :- Executors
                  tasks :- Tasks
                  constraints :- '{:pre (Map Any PreConstraint)
                                   :post (Map Any PostConstraint)}])
(defrecord Pool [executors tasks constraints])

(defalias PoolRef (Ref1 Pool))

;; ## Pool Functions
;;

(ann create [-> PoolRef])
(defn create
  "A new pool keeps track of the state of the pool. Access to it should be done via the `get-snapshot` function.
   The new pool has already all built-in constraints added."
  []
  (ref (map->Pool {:executors {}
                   :tasks {}
                   :constraints {:pre {} :post {}}})))

(ann ^:no-check get-snapshot [PoolRef -> Snapshot])
(defn get-snapshot
  "When getting a snapshot of a pool, you get an immutable view of the current executors and tasks. This view is
   guarenteed to be consistent."
  [pool]
    ; TODO this should be possible with core.typed!
    (select-keys @pool [:executors :tasks]))

(ann ^:no-check with-snapshot [Pool Snapshot -> Pool])
(defn- with-snapshot [pool snapshot]
  (merge pool snapshot))

(ann create-with-snapshot [Snapshot -> PoolRef])
(defn create-with-snapshot
  "It is also possible to create a new pool based on an old state. This can be used to simulate commits based on a real
   pool that should not affect the live system or to make a pool durable and restore it later. The new pool has the
   default constraints added, not necessarily the constraints when the snapshot was made."
  [snapshot]
  (let [pool (create)]
    (dosync
      (alter pool with-snapshot snapshot))
    pool))
