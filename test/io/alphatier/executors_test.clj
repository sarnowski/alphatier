(ns io.alphatier.executors-test
  (:require [clojure.test :refer :all]
            [io.alphatier.executors :refer :all]
            [io.alphatier.tools :as tools]
            [io.alphatier.pools :as pools]))


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
