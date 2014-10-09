;; ## Executors
;;
;; Executors are on the other side of schedulers. They are the workforce of Alphatier and execute whatever the
;; schedulers wants them to do.
(ns io.alphatier.executors
  (:require [clojure.core.typed :refer [ann Any ASeq IFn HVec doseq]]
            [clojure.core.incubator :refer [dissoc-in]]
            [io.alphatier.pools :refer :all])
  (:import [io.alphatier.pools Pool Executor Task]
           [clojure.lang Keyword]))

;; ### Executor Management
;;

(ann ^:no-check add-executor [Pool Executor -> Pool])
(defn- add-executor [pool executor]
  (assoc-in pool [:executors (:id executor)] executor))

(ann ^:no-check add-task [Pool Task -> Pool])
(defn- add-task [pool task]
  (assoc-in pool [:tasks (:id task)] task))

(ann register [PoolRef Any Resources & :optional {:metadata Metadata
                                                  :metadata-version Number
                                                  :tasks (ASeq Task)
                                                  :task-ids-version Number} -> PoolRef])
(defn register
  "Only registered executors are eligible to receive tasks from schedulers. The registration can also provide
   information about already running tasks. The meta data can signal various constraints to the schedulers like
   environment or operating system information or other capabilities. If an executor gets reregistered, all previous
   information get overwritten."
  [pool executor-id resources & {:keys [metadata
                                        metadata-version
                                        tasks
                                        task-ids-version]
                                 :or {metadata {}
                                      metadata-version 0
                                      tasks []
                                      task-ids-version 0}}]
  (if (empty? resources)
    (throw (IllegalStateException. "at least one resource must be given")))
  (if (not (every? number? (vals resources)))
    (throw (IllegalStateException. "resource values must be numbers")))
  (dosync
    (alter pool add-executor (map->Executor {:id executor-id
                                             :status :registered
                                             :resources resources
                                             :metadata metadata
                                             :metadata-version metadata-version
                                             :task-ids (map :id tasks)
                                             :task-ids-version task-ids-version}))
    (doseq [task :- Task tasks]
      ; TODO do more intelligent merges e.g. use metadata version and add tasks to executor if he doesnt know yet
      (alter pool add-task task)))
  pool)

(ann ^:no-check update-metadata (IFn [Pool ':executors Any Metadata -> Pool]
                                     [Pool ':tasks Any Metadata -> Pool]))
(defn- update-metadata [pool path id metadata]
  (update-in pool [path id :metadata] merge metadata))

(ann ^:no-check bump-version (IFn [Pool ':executors Any -> Pool]
                                  [Pool ':tasks Any -> Pool]
                                  [Pool ':executors Any Keyword -> Pool]))
(defn- bump-version
  ([pool path id] (bump-version pool path id :metadata-version))
  ([pool path id suffix] (update-in pool [path id suffix] inc)))

(ann update [PoolRef Any Metadata -> PoolRef])
(defn update
  "Executors can update their meta data while being registered. This can be used to inform schedulers about general
   executor state. For example, one could set a `maintenance` flag to signal that an executor does not accept new tasks
   until the flag is removed again."
  [pool executor-id metadata]
  (dosync
    (alter pool update-metadata :executors executor-id metadata)
    (alter pool bump-version :executors executor-id))
  pool)

(ann ^:no-check update-status [Pool Any State -> Pool])
(defn- update-status [pool executor-id state]
  (assoc-in pool [:executors executor-id :status] state))

(ann unregister [PoolRef Any -> PoolRef])
(defn unregister
  "If an executor shuts down (expected or unexpectedly), it has to be unregistered, so that schedulers do not use it
   anymore."
  [pool executor-id]
  (dosync
    (alter pool update-status executor-id :unregistered))
  pool)

;; ### Task Management
;;

(ann ^:no-check update-lifecycle-phase [Pool Any LifecyclePhase -> Pool])
(defn- update-lifecycle-phase [pool task-id lifecycle-phase]
  (assoc-in pool [:tasks task-id :lifecycle-phase] lifecycle-phase))

(ann update-task [PoolRef Any LifecyclePhase Metadata -> PoolRef])
(defn update-task
  "The executor has to push the lifecycle-phase to `:created` as soon as the task was created. In addition, the executor
   could update the meta data of a task as well. This could be internal state of the task like a progress state. It is
   possible to do lightweight IPC with the scheduler that way."
  [pool task-id lifecycle-phase metadata]
  (dosync
    ; TODO make sure lifecycle-phase can only go forward, not back
    (alter pool update-lifecycle-phase task-id lifecycle-phase)
    (alter pool update-metadata :tasks task-id metadata)
    (alter pool bump-version :tasks task-id))
  pool)

(ann ^:no-check remove-task [Pool Any Any -> Pool])
(defn- remove-task [pool executor-id task-id]
  (-> pool
      (dissoc-in [:tasks task-id])
      (update-in [:executors executor-id :task-ids] (partial remove #{task-id}))))

(ann kill-task [PoolRef Any -> PoolRef])
(defn kill-task
  "If a task was requested to be killed, the execuor has to kill the task and report back, that the task was actually
   killed. This leads to the complete removal of the task from the pool."
  [pool task-id]
  (dosync
    (let [executor-id (get-in @pool [:tasks task-id :executor-id])]
      (alter pool remove-task executor-id task-id)
      (alter pool bump-version :executors executor-id :task-ids-version)))
  pool)
