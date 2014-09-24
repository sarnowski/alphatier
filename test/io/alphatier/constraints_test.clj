(ns io.alphatier.constraints-test
  (:import [clojure.lang ExceptionInfo])
  (:require [clojure.test :refer :all]
            [io.alphatier.constraints :refer :all]
            [io.alphatier.tools :as tools]
            [io.alphatier.executors :as executors]
            [io.alphatier.pools :as pools]
            [io.alphatier.schedulers :as schedulers]))

(deftest add-test
  (let [pool (tools/create-test-pool)]

    (testing "adding pre constraints"
      (add pool :pre :test (fn [_ _] nil))
      (is (get-in @pool [:constraints :pre :test])))

    (testing "adding post constraints"
      (add pool :post :test (fn [_ _ _] []))
      (is (get-in @pool [:constraints :post :test])))))

(deftest del-test
  (let [pool (tools/create-test-pool)]

    (testing "adding post constraints"
      (add pool :post :test (fn [_ _ _] []))
      (del pool :post :test)
      (is (not (get-in @pool [:constraints :pre :test]))))

    (testing "adding post constraints"
      (add pool :post :test (fn [_ _ _] []))
      (del pool :post :test)
      (is (not (get-in @pool [:constraints :pre :test]))))))

(defn- testies []
  (let [pool (tools/create-test-pool)
        executor (-> pool pools/get-snapshot :executors vals first)
        commit (schedulers/map->Commit {:scheduler-id "test-scheduler"
                                        :actions [{:id "my-task"
                                                 :type :create
                                                 :executor-id (:id executor)
                                                 :resources {:memory 50 :cpu 1}}]
                                        :allow-partial-commit false})
        create-commit (schedulers/map->Commit {:scheduler-id "test-scheduler"
                                               :actions [{:id "my-task-1"
                                                        :type :create
                                                        :executor-id (:id executor)
                                                        :resources {:memory 50 :cpu 1}},
                                                       {:id "my-task-2"
                                                        :type :create
                                                        :executor-id (:id executor)
                                                        :resources {:memory 50 :cpu 1}}]
                                               :allow-partial-commit true})]
  [pool executor commit create-commit]))

(deftest pre-commit-test

    (testing "executing accepting pre-commit constraint"
      (let [[pool _ commit _] (testies)]
        (add pool :pre :succeed (fn [_ _] []))
        (let [result (schedulers/commit pool commit)]
          (is (:accepted-actions result))
          (is (every? empty? (vals (:rejected-actions result)))))))

    (testing "executing accepting post-commit constraint"
      (let [[pool _ commit _] (testies)]
        (add pool :post :succeed (fn [_ _ _] []))
        (let [result (schedulers/commit pool commit)]
          (is (:accepted-actions result))
          (is (every? empty? (vals (:rejected-actions result)))))))

    (testing "execute rejecting pre-commit constraint"
      (let [[pool _ commit _] (testies)]
        (add pool :pre :fail (fn [commit _] (:actions commit)))
        (try
          (schedulers/commit pool commit)
          (is false "Expected rejection")
          (catch ExceptionInfo e
            (let [result (ex-data e)]
              (is result)
              (is (empty? (:accepted-actions result)))
              (is (:rejected-actions result)))))))

    (testing "executing rejecting post-commit constraint"
      (let [[pool _ commit _] (testies)]
        (add pool :post :fail (fn [commit _ _] (:actions commit)))
        (try
          (schedulers/commit pool commit)
          (is false "Expected rejection")
          (catch ExceptionInfo e
            (let [result (ex-data e)]
              (is result)
              (is (empty? (:accepted-actions result)))
              (is (:rejected-actions result)))))))

      (testing "executing partially rejecting pre-commit constraint with partial commit allowed"
        (let [[pool _ _ commit] (testies)]
          (add pool :pre :fail (fn [commit _] [(first (:actions commit))]))
          (let [result (schedulers/commit pool commit)]
            (is result)
            (is (:accepted-actions result))
            (is (:rejected-actions result)))))

      (testing "executing fully rejecting pre-commit constraints with partial commit allowed"
        (let [[pool _ _ commit] (testies)]
          (add pool :pre :fail (fn [commit _] (:actions commit)))
          (try
            (schedulers/commit pool commit)
            (is false "Expected rejection")
            (catch ExceptionInfo e
              (let [result (ex-data e)]
                (is result)
                (is (empty? (:accepted-actions result)))
                (is (:rejected-actions result)))))))

      )

(deftest no-resource-overbooking-test
  (testing "overbook memory"
    (let [[pool executor _ _] (testies)
          commit (schedulers/map->Commit {:scheduler-id "test-scheduler"
                                          :actions [{:id "my-task-1"
                                                   :type :create
                                                   :executor-id (:id executor)
                                                   :resources {:memory 50 :cpu 1}},
                                                  {:id "my-task-2"
                                                   :type :create
                                                   :executor-id (:id executor)
                                                   :resources {:memory 50 :cpu 1}},
                                                  {:id "my-task-3"
                                                   :type :create
                                                   :executor-id (:id executor)
                                                   :resources {:memory 1 :cpu 1}}]
                                          :allow-partial-commit false})]
      (try
        (schedulers/commit pool commit)
        (is false "Expected rejection")
        (catch ExceptionInfo e
          (is (.contains (.getMessage e) "commit rejected"))
          (is (-> e ex-data :rejected-actions :no-resource-overbooking) (:actions commit))))))
  (testing "overbook cpu"
    (let [[pool executor _ _] (testies)
          commit (schedulers/map->Commit {:scheduler-id "test-scheduler"
                                          :actions [{:id "my-task-1"
                                                   :type :create
                                                   :executor-id (:id executor)
                                                   :resources {:memory 25 :cpu 4}},
                                                  {:id "my-task-2"
                                                   :type :create
                                                   :executor-id (:id executor)
                                                   :resources {:memory 25 :cpu 4}},
                                                  {:id "my-task-3"
                                                   :type :create
                                                   :executor-id (:id executor)
                                                   :resources {:memory 25 :cpu 1}}]
                                          :allow-partial-commit false})]
      (try
        (schedulers/commit pool commit)
        (is false "Expected rejection")
        (catch ExceptionInfo e
          (is (.contains (.getMessage e) "commit rejected"))
          (is (-> e ex-data :rejected-actions :no-resource-overbooking count (= 1))))))))

(deftest no-resource-overbooking-allow-partial-test
  (testing "overbook memory but allow"
    (let [[pool executor _ _] (testies)
          commit (schedulers/map->Commit {:scheduler-id "test-scheduler"
                                          :actions [{:id "my-task-1"
                                                   :type :create
                                                   :executor-id (:id executor)
                                                   :resources {:memory 50 :cpu 1}},
                                                  {:id "my-task-2"
                                                   :type :create
                                                   :executor-id (:id executor)
                                                   :resources {:memory 50 :cpu 1}},
                                                  {:id "my-task-3"
                                                   :type :create
                                                   :executor-id (:id executor)
                                                   :resources {:memory 1 :cpu 1}}]
                                          :allow-partial-commit true})]
      (let [result (schedulers/commit pool commit)]
        (is (= 1 (count (-> result :rejected-actions :no-resource-overbooking))))))))

(deftest optimistic-locking-test
  (testing "executor metadata-version"
    (let [[pool executor _ _] (testies)
          commit (schedulers/map->Commit {:scheduler-id "test-scheduler"
                                          :actions [{:id "my-task-1"
                                                   :type :create
                                                   :executor-id (:id executor)
                                                   :executor-metadata-version 0
                                                   :resources {:memory 50 :cpu 1}}]
                                          :allow-partial-commit false})]
      (executors/update pool (:id executor) {:foo "bar"})
      (try
        (schedulers/commit pool commit)
        (is false "Expected rejection")
        (catch ExceptionInfo e
          (is (.contains (.getMessage e) "commit rejected"))
          (is (-> e ex-data :rejected-actions :optimistic-locking count (= 1)))))))

  (testing "executor task-ids-version"
    (let [[pool executor _ _] (testies)
          commit-1 (schedulers/map->Commit {:scheduler-id "test-scheduler"
                                            :actions [{:id "my-task-1"
                                                     :type :create
                                                     :executor-id (:id executor)
                                                     :resources {:memory 50 :cpu 1}}]
                                            :allow-partial-commit false})
          commit-2 (schedulers/map->Commit {:scheduler-id "test-scheduler"
                                            :actions [{:id "my-task-2"
                                                     :type :create
                                                     :executor-id (:id executor)
                                                     :executor-task-ids-version 0
                                                     :resources {:memory 50 :cpu 1}}]
                                            :allow-partial-commit false})]
      (schedulers/commit pool commit-1)
      (try
        (schedulers/commit pool commit-2)
        (is false "Expected rejection")
        (catch ExceptionInfo e
          (is (.contains (.getMessage e) "commit rejected"))
          (is (-> e ex-data :rejected-actions :optimistic-locking count (= 1)))))))

  (testing "task metadata-version"
    (let [[pool executor _ _] (testies)
          create-commit (schedulers/map->Commit {:scheduler-id "test-scheduler"
                                                 :actions [{:id "my-task-1"
                                                          :type :create
                                                          :executor-id (:id executor)
                                                          :resources {:memory 10 :cpu 1}}]
                                                 :allow-partial-commit false})
          update-commit-1 (schedulers/map->Commit {:scheduler-id "test-scheduler"
                                                   :actions [{:id "my-task-1"
                                                            :type :update
                                                            :executor-id (:id executor)
                                                            :metadata {:foo "foo"}
                                                            :resources {:memory 10 :cpu 1}}]
                                                   :allow-partial-commit false})
          update-commit-2 (schedulers/map->Commit {:scheduler-id "test-scheduler"
                                                   :actions [{:id "my-task-1"
                                                            :type :update
                                                            :executor-id (:id executor)
                                                            :metadata-version 0
                                                            :metadata {:foo "bar"}
                                                            :resources {:memory 10 :cpu 1}}]
                                                   :allow-partial-commit false})]
      (schedulers/commit pool create-commit)
      (schedulers/commit pool update-commit-1)
      (try
        (schedulers/commit pool update-commit-2)
        (is false "Expected rejection")
        (catch ExceptionInfo e
          (is (.contains (.getMessage e) "commit rejected"))
          (is (-> e ex-data :rejected-actions :optimistic-locking count (= 1))))))))