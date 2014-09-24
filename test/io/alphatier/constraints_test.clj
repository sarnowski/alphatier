(ns io.alphatier.constraints-test
  (:import [clojure.lang ExceptionInfo])
  (:require [clojure.test :refer :all]
            [io.alphatier.constraints :refer :all]
            [io.alphatier.tools :as tools]
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
                                        :tasks [{:id "my-task"
                                                 :action :create
                                                 :executor-id (:id executor)
                                                 :resources {:memory 50 :cpu 1}}]
                                        :allow-partial-commit false})
        create-commit (schedulers/map->Commit {:scheduler-id "test-scheduler"
                                               :tasks [{:id "my-task-1"
                                                        :action :create
                                                        :executor-id (:id executor)
                                                        :resources {:memory 50 :cpu 1}},
                                                       {:id "my-task-2"
                                                        :action :create
                                                        :executor-id (:id executor)
                                                        :resources {:memory 50 :cpu 1}}]
                                               :allow-partial-commit true})]
  [pool executor commit create-commit]))

(deftest pre-commit-test

    (testing "executing accepting pre-commit constraint"
      (let [[pool _ commit _] (testies)]
        (add pool :pre :succeed (fn [_ _] []))
        (let [result (schedulers/commit pool commit)]
          (is (:accepted-tasks result))
          (is (every? empty? (vals (:rejected-tasks result)))))))

    (testing "executing accepting post-commit constraint"
      (let [[pool _ commit _] (testies)]
        (add pool :post :succeed (fn [_ _ _] []))
        (let [result (schedulers/commit pool commit)]
          (is (:accepted-tasks result))
          (is (every? empty? (vals (:rejected-tasks result)))))))

    (testing "execute rejecting pre-commit constraint"
      (let [[pool _ commit _] (testies)]
        (add pool :pre :fail (fn [commit _] (:tasks commit)))
        (try
          (schedulers/commit pool commit)
          (is false "Expected rejection")
          (catch ExceptionInfo e
            (let [result (ex-data e)]
              (is result)
              (is (empty? (:accepted-tasks result)))
              (is (:rejected-tasks result)))))))

    (testing "executing rejecting post-commit constraint"
      (let [[pool _ commit _] (testies)]
        (add pool :post :fail (fn [commit _ _] (:tasks commit)))
        (try
          (schedulers/commit pool commit)
          (is false "Expected rejection")
          (catch ExceptionInfo e
            (let [result (ex-data e)]
              (is result)
              (is (empty? (:accepted-tasks result)))
              (is (:rejected-tasks result)))))))

      (testing "executing partially rejecting pre-commit constraint with partial commit allowed"
        (let [[pool _ _ commit] (testies)]
          (add pool :pre :fail (fn [commit _] [(first (:tasks commit))]))
          (let [result (schedulers/commit pool commit)]
            (is result)
            (is (:accepted-tasks result))
            (is (:rejected-tasks result)))))

      (testing "executing fully rejecting pre-commit constraints with partial commit allowed"
        (let [[pool _ _ commit] (testies)]
          (add pool :pre :fail (fn [commit _] (:tasks commit)))
          (try
            (schedulers/commit pool commit)
            (is false "Expected rejection")
            (catch ExceptionInfo e
              (let [result (ex-data e)]
                (is result)
                (is (empty? (:accepted-tasks result)))
                (is (:rejected-tasks result)))))))

      )