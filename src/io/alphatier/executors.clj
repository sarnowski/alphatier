;; ## Executors
;;
;; Executors are on the other side of schedulers. They are the workforce of Alphatier and execute whatever the
;; schedulers wants them to do.
(ns io.alphatier.executors
  (:require [clojure.core.incubator :as clojure-incubator]
            [io.alphatier.pools :as core]))


;; ### Executor Management
;;

(defn register
  "Registers an executor in the pool. Overwrites old entries."
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
  "Updates an executor's metadata and its metadata-version."
  [pool executor-id metadata]
  (dosync
    (alter pool update-in [:executors executor-id :metadata] merge metadata)
    (alter pool update-in [:executors executor-id :metadata-version] inc))
  pool)

(defn unregister
  "Marks an executor as unregistered."
  [pool executor-id]
  (dosync
    (alter pool assoc-in [:executors executor-id :status] :unregistered))
  pool)

;; ### Task Management
;;

(defn update-task
  "Update task lifecycle-phase and metadata with the corresponding metadata-version."
  [pool task-id lifecycle-phase metadata]
  (dosync
    ; TODO make sure lifecycle-phase can only go forward, not back
    (alter pool assoc-in [:tasks task-id :lifecycle-phase] lifecycle-phase)
    (alter pool update-in [:tasks task-id :metadata] merge metadata)
    (alter pool update-in [:tasks task-id :metadata-version] inc))
  pool)

(defn kill-task
  "Removes a task completely."
  [pool task-id]
  (dosync
    (let [executor-id (get-in @(:tasks pool) [task-id :executor-id])]
      (alter pool clojure-incubator/dissoc-in [:tasks task-id])
      (alter pool update-in [:executors executor-id :task-ids] #(remove #{task-id} %))
      (alter pool update-in [:executors executor-id :tasks-version] inc)))
  pool)

;; ### Java usage
;;
;; The here defined functions can be accessed via the `Executors` utility class.
;;
;;     Executors.register(...);
(gen-class
  :name "io.alphatier.Executors"
  :main false
  :prefix "java-")

(defn- java-register [pool executorId resources options] (register pool executorId resources options))
(defn- java-update [pool executorId metadata] (register pool executorId metadata))
(defn- java-update [pool executorId metadata] (register pool executorId metadata))
