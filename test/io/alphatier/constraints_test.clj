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
      (add pool :pre :test (fn [commit pre-snapshot] nil))
      (is (get-in @pool [:constraints :pre :test])))

    (testing "adding post constraints"
      (add pool :post :test (fn [commit pre-snapshot post-snapshot] []))
      (is (get-in @pool [:constraints :post :test])))))

(deftest del-test
  (let [pool (tools/create-test-pool)]

    (testing "adding post constraints"
      (add pool :post :test (fn [commit pre-snapshot post-snapshot] []))
      (del pool :post :test)
      (is (not (get-in @pool [:constraints :pre :test]))))

    (testing "adding post constraints"
      (add pool :post :test (fn [commit pre-snapshot post-snapshot] []))
      (del pool :post :test)
      (is (not (get-in @pool [:constraints :pre :test]))))))

(deftest pre-commit-test
  (let [pool (tools/create-test-pool)
        executor (-> pool pools/get-snapshot :executors vals first)
        commit (schedulers/map->Commit {:scheduler-id "test-scheduler"
                                        :tasks [{:id "test-task"
                                                 :action :create
                                                 :executor-id (:id executor)
                                                 :resources {:memory 50}}]
                                        :allow-partial-commit false})]

    (testing "executing accepting pre-commit constraint"
      (add pool :pre :succeed (fn [commit pre-snapshot] []))
      (let [result (schedulers/commit pool commit)]
        (is (:accepted-tasks result))
        (is (every? empty? (vals (:rejected-tasks result))))))

    (testing "executing accepting post-commit constraint"
      (add pool :post :succeed (fn [commit pre-snapshot post-snapshot] []))
      (let [result (schedulers/commit pool commit)]
        (is (:accepted-tasks result))
        (is (every? empty? (vals (:rejected-tasks result))))))

    (testing "execute rejecting pre-commit constraint"
      (add pool :pre :fail (fn [commit pre-snapshot] (:tasks commit)))
      (try
        (schedulers/commit pool commit)
        (is false "Expected rejection")
        (catch ExceptionInfo e
          (let [result (ex-data e)]
            (is result)
            (is (empty? (:accepted-tasks result)))
            (is (:rejected-tasks result))))))

    (testing "executing rejecting post-commit constraint"
      (add pool :post :fail (fn [commit pre-snapshot post-snapshot] (:tasks commit)))
      (try
        (schedulers/commit pool commit)
        (is false "Expected rejection")
        (catch ExceptionInfo e
          (let [result (ex-data e)]
            (is result)
            (is (empty? (:accepted-tasks result)))
            (is (:rejected-tasks result))))))

    (let [commit (schedulers/map->Commit {:scheduler-id "test-scheduler"
                                          :tasks [{:id "test-task"
                                                   :action :create
                                                   :executor-id (:id executor)
                                                   :resources {:memory 50}},
                                                  {:id "test-task"
                                                   :action :create
                                                   :executor-id (:id executor)
                                                   :resources {:memory 50}}]
                                          :allow-partial-commit true})]
      (testing "executing partially rejecting pre-commit constraint with partial commit allowed"
        (add pool :pre :fail (fn [commit pre-snapshot] [(first (:tasks commit))]))
        (let [result (schedulers/commit pool commit)]
          (is result)
          (is (:accepted-tasks result))
          (is (:rejected-tasks result))))

      (testing "executing fully rejecting pre-commit constraints with partial commit allowed"
        (add pool :pre :fail (fn [commit pre-snapshot] (:tasks commit)))
        (try
          (schedulers/commit pool commit)
          (is false "Expected rejection")
          (catch ExceptionInfo e
            (let [result (ex-data e)]
              (is result)
              (is (empty? (:accepted-tasks result)))
              (is (:rejected-tasks result))))))

      )))