(ns io.alphatier.tools
  (:require [io.alphatier.pools :as pools]))

(def ^:private test-executors (atom 0))

(defn create-test-executor []
  (let [id-no (swap! test-executors inc)]
    (pools/map->Executor {:id (str "test-executor-" id-no)
                    :status :registered
                    :resources {:memory 100
                                :cpu 8}
                    :metadata {}
                    :metadata-version 0
                    :task-ids []
                    :tasks-version 0})))

(defn create-test-pool []
  (let [pool (pools/create)
        executor-count 3]
    (dosync
      (doseq [executor (repeatedly executor-count create-test-executor)]
        (alter pool assoc-in [:executors (:id executor)] executor)))
    pool))
