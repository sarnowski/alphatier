;; ## Executors
;;
;; Executors are on the other side of schedulers. They are the workforce of Alphatier and execute whatever the
;; schedulers wants them to do.
(ns io.alphatier.executors
  (:import [clojure.lang IFn])
  (:require [clojure.core.incubator :as clojure-incubator]
            [io.alphatier.pools :as core]))


;; ### Executor Management
;;

(defn register
  "Only registered executors are eligible to receive tasks from schedulers. The registration can also provide
   information about already running tasks. The meta data can signal various constraints to the schedulers like
   environment or operating system information or other capabilities. If an executor gets reregistered, all previous
   information get overwritten."
  [pool executor-id resources & {:keys [metadata
                                        metadata-version
                                        tasks
                                        tasks-version]
                                 :or {metadata {}
                                      metadata-version 0
                                      tasks []
                                      tasks-version 0}}]
  (if (empty? resources)
    (throw (IllegalStateException. "at least one resource must be given")))
  (if (not (every? number? (vals resources)))
    (throw (IllegalStateException. "resource values must be numbers")))
  (dosync
    (alter pool assoc-in [:executors executor-id]
           (core/map->Executor {:id executor-id
                                :status :registered
                                :resources resources
                                :metadata metadata
                                :metadata-version metadata-version
                                :task-ids (map :id tasks)
                                :tasks-version tasks-version}))
    (doseq [task tasks]
      ; TODO do more intelligent merges e.g. use metadata version and add tasks to executor if he doesnt know yet
      (alter pool assoc-in [:tasks (:id task)] task)))
  pool)

(defn update
  "Executors can update their meta data while being registered. This can be used to inform schedulers about general
   executor state. For example, one could set a `maintenance` flag to signal that an executor does not accept new tasks
   until the flag is removed again."
  [pool executor-id metadata]
  (dosync
    (alter pool update-in [:executors executor-id :metadata] merge metadata)
    (alter pool update-in [:executors executor-id :metadata-version] inc))
  pool)

(defn unregister
  "If an executor shuts down (expected or unexpectedly), it has to be unregistered, so that schedulers do not use it
   anymore."
  [pool executor-id]
  (dosync
    (alter pool assoc-in [:executors executor-id :status] :unregistered))
  pool)

;; ### Task Management
;;

(defn update-task
  "The executor has to push the lifecycle-phase to `:created` as soon as the task was created. In addition, the executor
   could update the meta data of a task as well. This could be internal state of the task like a progress state. It is
   possible to do lightweight IPC with the scheduler that way."
  [pool task-id lifecycle-phase metadata]
  (dosync
    ; TODO make sure lifecycle-phase can only go forward, not back
    (alter pool assoc-in [:tasks task-id :lifecycle-phase] lifecycle-phase)
    (alter pool update-in [:tasks task-id :metadata] merge metadata)
    (alter pool update-in [:tasks task-id :metadata-version] inc))
  pool)

(defn kill-task
  "If a task was requested to be killed, the execuor has to kill the task and report back, that the task was actually
   killed. This leads to the complete removal of the task from the pool."
  [pool task-id]
  (dosync
    (let [executor-id (get-in @pool [:tasks task-id :executor-id])]
      (alter pool clojure-incubator/dissoc-in [:tasks task-id])
      (alter pool update-in [:executors executor-id :task-ids] #(remove #{task-id} %))
      (alter pool update-in [:executors executor-id :tasks-version] inc)))
  pool)
