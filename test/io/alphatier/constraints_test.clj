(ns io.alphatier.constraints-test
  (:import [clojure.lang ExceptionInfo])
  (:require [clojure.test :refer :all]
            [io.alphatier.setup :refer :all]
            [io.alphatier.assert :refer :all]
            [io.alphatier.tools :as tools]
            [io.alphatier.constraints :as constraints]
            [io.alphatier.executors :as executors]
            [io.alphatier.pools :as pools]
            [io.alphatier.schedulers :as schedulers]))

;; crud

(deftest pre-constraint-add-test
  (let [pool (default-pool)]
    (constraints/add pool :pre :test pre-pass)
    (is (get-in (deref pool) [:constraints :pre :test]))))

(deftest post-constraint-add-test
  (let [pool (default-pool)]
    (constraints/add pool :post :test post-pass)
    (is (get-in (deref pool) [:constraints :post :test]))))

(deftest pre-constraint-del-test
  (let [pool (default-pool)]
    (constraints/add pool :pre :test pre-pass)
    (constraints/del pool :pre :test)
    (is (not (get-in (deref pool) [:constraints :pre :test])))))

(deftest post-constraint-del-test
  (let [pool (default-pool)]
    (constraints/add pool :post :test post-pass)
    (constraints/del pool :post :test)
    (is (not (get-in (deref pool) [:constraints :post :test])))))

(deftest pre-constraint-pass-test
  (let [pool (default-pool)
        executor-id (executor-id-of pool first)
        commit (create-commit "test" (create-actions executor-id :size 1))]
    (constraints/add pool :pre :pass pre-pass)
    (let [result (schedulers/commit pool commit)]
      (is (:accepted-actions result))
      (is (every? empty? (vals (:rejected-actions result)))))))

;; passing constraints

(deftest post-constraint-pass-test
  (let [pool (default-pool)
        executor-id (executor-id-of pool first)
        commit (create-commit "test" (create-actions executor-id :size 1))]
    (constraints/add pool :post :pass post-pass)
    (let [result (schedulers/commit pool commit)]
      (is (:accepted-actions result))
      (is (every? empty? (vals (:rejected-actions result)))))))

(deftest pre-constraint-reject-test
  (let [pool (default-pool)
        executor-id (executor-id-of pool first)
        commit (create-commit "test" (create-actions executor-id :size 1))]
    (constraints/add pool :pre :reject pre-reject)
    (try
      (schedulers/commit pool commit)
      (fail "Expected rejection")
      (catch ExceptionInfo e
        (let [result (ex-data e)]
          (is result)
          (is (empty? (:accepted-actions result)))
          (is (:rejected-actions result)))))))

;; rejecting

(deftest post-constraint-reject-test
  (let [pool (default-pool)
        executor-id (executor-id-of pool first)
        commit (create-commit "test" (create-actions executor-id :size 1))]
    (constraints/add pool :post :reject post-reject)
    (try
      (schedulers/commit pool commit)
      (fail "Expected rejection")
      (catch ExceptionInfo e
        (let [result (ex-data e)]
          (is result)
          (is (empty? (:accepted-actions result)))
          (is (:rejected-actions result)))))))

(deftest pre-constraint-partial-reject-test
  (let [pool (empty-pool)
        executor-id (executor-id-of pool first)
        commit (create-commit "test" (create-actions executor-id :size 2) :allow-partial-commit true)]
    (constraints/add pool :pre :reject-first (pre-reject-only first))
    (let [result (schedulers/commit pool commit)]
      (is result)
      (is (:accepted-actions result))
      (is (= 1 (count (-> result :rejected-actions :reject-first vals)))))))

(deftest post-constraint-partial-reject-test
  (let [pool (empty-pool)
        executor-id (executor-id-of pool first)
        commit (create-commit "test" (create-actions executor-id :size 2) :allow-partial-commit true)]
    (constraints/add pool :post :reject-first (post-reject-only first))
    (let [result (schedulers/commit pool commit)]
      (is result)
      (is (:accepted-actions result))
      (is (= 1 (count (-> result :rejected-actions :reject-first vals)))))))

(deftest pre-constraint-partial-reject-full-test
  "Verifies that a full-rejection even if :allow-partial-commit results in an exception"
  (let [pool (default-pool)
        executor-id (executor-id-of pool first)
        commit (create-commit "test" (create-actions executor-id :size 2) :allow-partial-commit true)]
    (constraints/add pool :pre :reject pre-reject)
    (try
      (schedulers/commit pool commit)
      (fail "Expected rejection")
      (catch ExceptionInfo e
        (let [result (ex-data e)]
          (is result)
          (is (empty? (:accepted-actions result)))
          (is (:rejected-actions result)))))))

(deftest post-constraint-partial-reject-full-test
  "Verifies that a full-rejection even if :allow-partial-commit results in an exception"
  (let [pool (default-pool)
        executor-id (executor-id-of pool first)
        commit (create-commit "test" (create-actions executor-id :size 2) :allow-partial-commit true)]
    (constraints/add pool :post :reject post-reject)
    (try
      (println (:accepted-actions (schedulers/commit pool commit)))
      (fail "Expected rejection")
      (catch ExceptionInfo e
        (let [result (ex-data e)]
          (is result)
          (is (empty? (:accepted-actions result)))
          (is (:rejected-actions result)))))))

;; specific constraints

(deftest no-resource-overbooking-test
  (let [pool (default-pool)
        executor-id (executor-id-of pool first)
        commit (overbooking-commit "test" executor-id)]
    (try
      (schedulers/commit pool commit)
      (fail "Expected rejection")
      (catch ExceptionInfo e
        (is (.contains (.getMessage e) "commit rejected"))
        (is (-> e ex-data :rejected-actions :no-resource-overbooking) (:actions commit))))))

(deftest no-resource-overbooking-partial-test
  (let [pool (default-pool)
        executor-id (executor-id-of pool first)
        commit (overbooking-commit "test" executor-id :allow-partial-commit true)]
    (let [result (schedulers/commit pool commit)]
      (is (= 2 (count (-> result :rejected-actions :no-resource-overbooking)))))))

(deftest optimistic-locking-executor-metadata-test
  (let [pool (default-pool)
        executor-id (executor-id-of pool first)
        commit (create-commit "test" [(create-action executor-id :executor-metadata-version 0)])]
    (executors/update pool executor-id {:foo "bar"})
    (try
      (schedulers/commit pool commit)
      (fail "Expected rejection")
      (catch ExceptionInfo e
        (is (.contains (.getMessage e) "commit rejected"))
        (is (-> e ex-data :rejected-actions :optimistic-locking count (= 1)))))))

(deftest optimistic-locking-executor-task-ids-test
  (let [pool (default-pool)
        executor-id (executor-id-of pool first)
        commit (create-commit "test" [(create-action executor-id :executor-task-ids-version 0)])]
    (try
      (schedulers/commit pool commit)
      (fail "Expected rejection")
      (catch ExceptionInfo e
        (is (.contains (.getMessage e) "commit rejected"))
        (is (-> e ex-data :rejected-actions :optimistic-locking count (= 1)))))))

(deftest optimistic-locking-task-metadata-test
  (let [pool (default-pool)
        executor-id (executor-id-of pool first)
        task-id (gen-task-id)
        create (create-commit "test" [(create-action executor-id :id task-id)])
        update (create-commit "test" [(create-action executor-id :id task-id :type :update :metadata {:foo "bar"})])
        commit (create-commit "test" [(create-action executor-id :id task-id :type :update :metadata-version 0)])]
    (schedulers/commit pool create)
    (schedulers/commit pool update)
    (try
      (schedulers/commit pool commit)
      (fail "Expected rejection")
      (catch ExceptionInfo e
        (is (.contains (.getMessage e) "commit rejected"))
        (is (-> e ex-data :rejected-actions :optimistic-locking count (= 1)))))))
