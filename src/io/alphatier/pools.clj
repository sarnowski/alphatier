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
  (:require [io.alphatier.constraints :as constraints]))

;; ### Pools
;;
;; A pool represents a snapshot of a set of executors and their tasks. Both executors and tasks are packaged in maps
;; with their IDs as key.
(defrecord Pool
           [executors tasks constraints])

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
(defrecord Executor
           [id status resources metadata metadata-version task-ids task-ids-version])

;; ### Tasks
;;
;; Tasks are execution requests for executors. A task has an unique identifier. It holds the following information:
;;
;; * `executor-id` is the identifier of the executor this task is assigned to.
;; * `lifecycle-phase` can be one of the following: `:create`, `:created` and `:kill`.
;; * `resources` works similar to the `resources` information on an executor but represents the amount of resources
;;   that must be reserved and guaranteed for this task.
;; * `metadata` and `metadata-version` work exactly like the `metadata` on an executor but provided by the scheduler.
(defrecord Task
           [id executor-id scheduler-id lifecycle-phase resources metadata metadata-version])


;; ## Pool Functions
;;

(defn create
  "A new pool keeps track of the state of the pool. Access to it should be done via the `get-snapshot` function.
   The new pool has already all built-in constraints added."
  []
  (ref (map->Pool {:executors {}
                   :tasks {}
                   :constraints
                     {:pre
                        {:optimistic-locking constraints/optimistic-locking}
                      :post
                        {:no-resource-overbooking constraints/no-resource-overbooking}}})))

(defn get-snapshot
  "When getting a snapshot of a pool, you get an immutable view of the current executors and tasks. This view is
   guarenteed to be consistent."
  [pool]
  (select-keys @pool [:executors :tasks]))

(defn create-with-snapshot
  "It is also possible to create a new pool based on an old state. This can be used to simulate commits based on a real
   pool that should not affect the live system or to make a pool durable and restore it later. The new pool has the
   default constraints added, not necessarily the constraints when the snapshot was made."
  [snapshot]
  (let [pool (create)]
    (dosync
      (alter pool merge snapshot))
    pool))
