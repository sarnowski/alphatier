(ns io.alphatier.executors-test
  (:require [clojure.test :refer :all]
            [io.alphatier.executors :refer :all]
            [io.alphatier.tools :as tools]
            [io.alphatier.pools :as pools]
            [io.alphatier.schedulers :as schedulers]))


(deftest register-executor-test
  (let [pool (tools/create-test-pool)]

    (testing "new registration"
      (register pool "new-reg-exec" {:memory 100})

      (let [{:keys [executors tasks]} (pools/get-snapshot pool)
            registered-executor (get executors "new-reg-exec")]
        (is registered-executor)
        (is (= "new-reg-exec" (:id registered-executor)))
        (is (= 100 (get-in registered-executor [:resources :memory])))))

    (testing "update registration"
      (register pool "new-reg-exec" {:memory 200})

      (let [{:keys [executors tasks]} (pools/get-snapshot pool)
            registered-executor (get executors "new-reg-exec")]
        (is (= 200 (get-in registered-executor [:resources :memory])))))))

(deftest update-executor-test
  (let [pool (tools/create-test-pool)
        executor (-> pool pools/get-snapshot :executors vals first)]

    (testing "update executor metadata merging"
      (update pool (:id executor) {:foo :bar})
      (update pool (:id executor) {:foo2 :bar2})

      (let [{:keys [executors tasks]} (pools/get-snapshot pool)
            updated-executor (get executors (:id executor))]
        (is (= :bar (get-in updated-executor [:metadata :foo])))
        (is (= :bar2 (get-in updated-executor [:metadata :foo2])))))

    (testing "update executor metadata with version"
      (update pool (:id executor) {:foo :bar3})

      (let [{:keys [executors tasks]} (pools/get-snapshot pool)
            updated-executor (get executors (:id executor))]
        (is (= :bar3 (get-in updated-executor [:metadata :foo])))
        (is (= (+ 3 (:metadata-version executor)) (:metadata-version updated-executor)))))))

(deftest unregister-executor-test
  (let [pool (tools/create-test-pool)
        executor (-> pool pools/get-snapshot :executors vals first)]

    (testing "unregister executor"
      (unregister pool (:id executor))

      (let [{:keys [executors tasks]} (pools/get-snapshot pool)
            unregistered-executor (get executors (:id executor))]
        (is unregistered-executor)
        (is (= :unregistered (:status unregistered-executor)))))))

(deftest update-instance-metadata-test
  (let [pool (tools/create-test-pool)
        [executor-id executor] (-> pool pools/get-snapshot :executors first)
        task (tools/create-test-task executor-id)]

    (schedulers/commit pool (schedulers/map->Commit {:scheduler-id "test-scheduler"
                                                     :actions [(merge {:type :create}
                                                                      (select-keys task [:id
                                                                                         :executor-id
                                                                                         :resources
                                                                                         :metadata]))]
                                                     :allow-partial-commit false}))

    (testing "lifecycle update"
      (update-task pool (:id task) :creating {:some :test})

      (let [{:keys [executors tasks]} (pools/get-snapshot pool)
            updated-task (get tasks (:id task))]

        (is (:resources updated-task))
        (is (= 10 (get-in updated-task [:resources :memory])))

        (is (:metadata updated-task))
        (is (= :generated (get-in updated-task [:metadata :type])))
        (is (= :test (get-in updated-task [:metadata :some])))))))
