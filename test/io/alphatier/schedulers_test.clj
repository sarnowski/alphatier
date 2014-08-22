(ns io.alphatier.schedulers-test
  (:require [clojure.test :refer :all]
            [io.alphatier.schedulers :refer :all]
            [io.alphatier.tools :as tools]
            [io.alphatier.pools :as pools]))

(deftest commit-test
  (let [pool (tools/create-test-pool)
        executor (-> pool pools/get-snapshot :executors vals first)]

    (testing "creation"

      (testing "simple task creation"
        (commit pool "test-scheduler"
                :create [{:id "test-task"
                          :executor-id (:id executor)
                          :resources {:memory 50}}])

        (let [{:keys [executors tasks]} (pools/get-snapshot pool)
              used-executor (get executors (:id executor))
              task (get tasks "test-task")]
          (is task)
          (is (= "test-task") (:id task))
          (is (contains? (into #{} (:task-ids used-executor)) "test-task"))))

      (testing "multiple task creation"
        (commit pool "test-scheduler"
               :create [{:id "test-task-1"
                         :executor-id (:id executor)
                         :resources {:memory 50}}
                        {:id "test-task-2"
                         :executor-id (:id executor)
                         :resources {:memory 50}}])

        (let [{:keys [executors tasks]} (pools/get-snapshot pool)
              task1 (get tasks "test-task-1")
              task2 (get tasks "test-task-2")]
          (is task1)
          (is task2)
          (is (= "test-task-1") (:id task1))
          (is (= "test-task-2") (:id task2))))

      (comment
      (testing "simple task rejection"
        (try
          (commit pool "test-scheduler"
                  :create [{:id "test-task"
                            :executor-id (:id executor)
                            :resources {:memory (inc (get-in executor [:resources :memory]))}}])
          (tools/fail!! "no rejection")
          (catch clojure.lang.ExceptionInfo e
            (is (ex-data e))))))

      )))
