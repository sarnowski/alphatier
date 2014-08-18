(ns alphatier.core-test
  (:require [clojure.test :refer :all]
            [alphatier.core :refer :all]))

(def test-executors (atom 0))

(defn create-test-executor []
  (let [id-no (swap! test-executors inc)]
    (map->Executor {:id (str "test-executor-" id-no)
                    :status :registered
                    :resources {:memory 100
                                :cpu 8}
                    :metadata {}
                    :metadata-version 0
                    :task-ids []
                    :tasks-version 0})))

(defn create-test-pool []
  (let [pool (create-pool)
        executor-count 3]
    (dosync
      (doseq [executor (repeatedly executor-count create-test-executor)]
        (alter (:executors pool) assoc (:id executor) executor)))
    pool))


(deftest register-executor-test
  (let [pool (create-test-pool)]

    (testing "new registration"
      (register-executor pool "new-reg-exec" {:memory 100})

      (let [{:keys [executors tasks]} (get-state pool)
            registered-executor (get executors "new-reg-exec")]
        (is registered-executor)
        (is (= "new-reg-exec" (:id registered-executor)))
        (is (= 100 (get-in registered-executor [:resources :memory])))))

    (testing "update registration"
      (register-executor pool "new-reg-exec" {:memory 200})

      (let [{:keys [executors tasks]} (get-state pool)
            registered-executor (get executors "new-reg-exec")]
        (is (= 200 (get-in registered-executor [:resources :memory])))))))

(deftest update-executor-test
  (let [pool (create-test-pool)
        executor (first (vals @(:executors pool)))]

    (testing "update executor metadata merging"
      (update-executor pool (:id executor) {:foo :bar})
      (update-executor pool (:id executor) {:foo2 :bar2})

      (let [{:keys [executors tasks]} (get-state pool)
            updated-executor (get executors (:id executor))]
        (is (= :bar (get-in updated-executor [:metadata :foo])))
        (is (= :bar2 (get-in updated-executor [:metadata :foo2])))))

    (testing "update executor metadata with version"
      (update-executor pool (:id executor) {:foo :bar3})

      (let [{:keys [executors tasks]} (get-state pool)
            updated-executor (get executors (:id executor))]
        (is (= :bar3 (get-in updated-executor [:metadata :foo])))
        (is (= (+ 3 (:metadata-version executor)) (:metadata-version updated-executor)))))))

(deftest unregister-executor-test
  (let [pool (create-test-pool)
        executor (first (vals @(:executors pool)))]

    (testing "unregister executor"
      (unregister-executor pool (:id executor))

      (let [{:keys [executors tasks]} (get-state pool)
            unregistered-executor (get executors (:id executor))]
        (is unregistered-executor)
        (is (= :unregistered (:status unregistered-executor)))))))

(deftest commit-test
  (let [pool (create-test-pool)
        executor (first (vals @(:executors pool)))]

    (testing "creation"

      (testing "simple task creation"
        (commit pool "simple-test-commit" "test-scheduler"
                :create [{:id "test-task"
                          :executor-id (:id executor)
                          :resources {:memory 50}}])

        (let [{:keys [executors tasks]} (get-state pool)
              used-executor (get executors (:id executor))
              task (get tasks "test-task")]
          (is task)
          (is (= "test-task") (:id task))
          (is (contains? (into #{} (:task-ids used-executor)) "test-task"))))

      (testing "multiple task creation"
        (commit pool "multiple-test-commit" "test-scheduler"
               :create [{:id "test-task-1"
                         :executor-id (:id executor)
                         :resources {:memory 50}}
                        {:id "test-task-2"
                         :executor-id (:id executor)
                         :resources {:memory 50}}])

        (let [{:keys [executors tasks]} (get-state pool)
              task1 (get tasks "test-task-1")
              task2 (get tasks "test-task-2")]
          (is task1)
          (is task2)
          (is (= "test-task-1") (:id task1))
          (is (= "test-task-2") (:id task2))))

      (testing "simple task rejection"
        (is (thrown? alphatier.core.RejectedException
              (commit pool "simple-test-commit" "test-scheduler"
                     :create [{:id "test-task"
                               :executor-id (:id executor)
                               :resources {:memory (inc (get-in executor [:resources :memory]))}}])))))))
