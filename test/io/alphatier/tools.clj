(ns io.alphatier.tools
  (:require [clojure.test :refer :all]
            [io.alphatier.pools :as pools]))

(defn fail!! [failure-reason]
  (is (not failure-reason)))

(def ^:private test-executors (atom 0))
(def ^:private test-tasks (atom 0))

(defn create-test-executor []
  (let [id-no (swap! test-executors inc)]
    (pools/map->Executor {:id (str "test-executor-" id-no)
                          :status :registered
                          :resources {:memory 100
                                      :cpu 8}
                          :metadata {}
                          :metadata-version 0
                          :task-ids []
                          :task-ids-version 0})))

(defn create-test-task [executor-id]
  (let [id-no (swap! test-executors inc)]
    (pools/map->Task {:id (str "test-task-" id-no)
                      :executor-id executor-id
                      :lifecycle-phase :create
                      :resources {:memory 10
                                  :cpu 1}
                      :metadata {:type :generated}
                      :metadata-version 0})))

(defn create-test-pool []
  (let [pool (pools/create)
        executor-count 3]
    (dosync
      (doseq [executor (repeatedly executor-count create-test-executor)]
        (alter pool assoc-in [:executors (:id executor)] executor)))
    pool))
