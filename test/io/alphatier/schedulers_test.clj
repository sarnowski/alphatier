(ns io.alphatier.schedulers-test
  (:import [clojure.lang ExceptionInfo])
  (:require [clojure.test :refer :all]
            [io.alphatier.assert :refer :all]
            [io.alphatier.setup :refer :all]
            [io.alphatier.tools :as tools]
            [io.alphatier.schedulers :as schedulers]
            [io.alphatier.executors :as executors]
            [io.alphatier.pools :as pools]))

(deftest commit-duplicate-actions-should-fail
  (let [pool (empty-pool)
        executor-id (executor-id-of pool first)
        commit (create-commit "test" (create-actions executor-id
                                                     :action #(create-action % :id "same-id")
                                                     :size 2))]
    (try
      (schedulers/commit pool commit)
      (fail "Expected rejection")
      (catch ExceptionInfo e
        (is (.contains (.getMessage e) "duplicate tasks"))))))

(deftest commit-duplicate-create-actions-should-fail
  (let [pool (empty-pool)
        executor-id (executor-id-of pool first)
        commit (create-commit "test" [(create-action executor-id :id "same-id")])]
    (schedulers/commit pool commit)
    (try
      (schedulers/commit pool commit)
      (fail "Expected rejection")
      (catch ExceptionInfo e
        (is (.contains (.getMessage e) "duplicate create tasks"))))))

(deftest commit-action-for-unknown-task-should-fail
  (let [pool (empty-pool)
        executor-id (executor-id-of pool first)
        commit (create-commit "test" [(create-action executor-id :id "unknown-id" :type :update)])]
    (try
      (schedulers/commit pool commit)
      (fail "Expected rejection")
      (catch ExceptionInfo e
        (is (.contains (.getMessage e) "missing task for update"))))))

(deftest create-action-with-scheduler-id-should-fail
  (let [pool (empty-pool)
        executor-id (executor-id-of pool first)
        commit (create-commit "test" [(create-action executor-id :scheduler-id "foo")])]
    (try
      (schedulers/commit pool commit)
      (fail "Expected rejection")
      (catch ExceptionInfo e
        (is (.contains (.getMessage e) "illegal properties in create actions"))))))

(deftest create-action-with-lifecycle-phase-should-fail
  (let [pool (empty-pool)
        executor-id (executor-id-of pool first)
        commit (create-commit "test" [(create-action executor-id :lifecycle-phase :created)])]
    (try
      (schedulers/commit pool commit)
      (fail "Expected rejection")
      (catch ExceptionInfo e
        (is (.contains (.getMessage e) "illegal properties in create actions"))))))

(deftest create-action-with-metadata-version-should-fail
  (let [pool (empty-pool)
        executor-id (executor-id-of pool first)
        commit (create-commit "test" [(create-action executor-id :metadata-version 1)])]
    (try
      (schedulers/commit pool commit)
      (fail "Expected rejection")
      (catch ExceptionInfo e
        (is (.contains (.getMessage e) "illegal properties in create actions"))))))

(comment ; TODO migrate
(deftest commit-test

      (testing "kill for missing task"
        (let [[pool executor] (testies)
              create-commit (map->Commit {:scheduler-id "test-scheduler"
                                          :actions [{:id "my-task"
                                                   :type :kill
                                                   :executor-id (:id executor)
                                                   :resources {:memory 50 :cpu 1}}]
                                          :allow-partial-commit false})]
          (try
            (commit pool create-commit)
            (is false "Expected rejection")
          (catch ExceptionInfo e
            (is (.contains (.getMessage e) "missing task for kill"))))))

      (testing "task with missing executor"
        (let [[pool _] (testies)
              create-commit (map->Commit {:scheduler-id "test-scheduler"
                                          :actions [{:id "my-task"
                                                   :type :create
                                                   :executor-id "no-such-executor"
                                                   :resources {:memory 50}}]
                                          :allow-partial-commit false})]
          (try
            (commit pool create-commit)
            (is false "Expected rejection")
          (catch ExceptionInfo e
            (is (.contains (.getMessage e) "missing executor"))))))

      (testing "task with missing resources"
        (let [[pool executor] (testies)
              create-commit (map->Commit {:scheduler-id "test-scheduler"
                                          :actions [{:id "my-task"
                                                   :type :create
                                                   :executor-id (:id executor)
                                                   :resources {:memory 50}}]
                                          :allow-partial-commit false})]
          (try
            (commit pool create-commit)
            (is false "Expected rejection")
          (catch ExceptionInfo e
            (is (.contains (.getMessage e) "missing resource"))))))

      (testing "simple task creation"
        (let [[pool executor] (testies)]
          (commit pool (map->Commit {:scheduler-id "test-scheduler"
                                     :actions [{:id "my-task"
                                              :type :create
                                              :executor-id (:id executor)
                                              :resources {:memory 50 :cpu 1}}]
                                     :allow-partial-commit false}))

          (let [{:keys [executors tasks]} (pools/get-snapshot pool)
                used-executor (get executors (:id executor))
                task (get tasks "my-task")]
            (is task)
            (is (= "my-task") (:id task))
            (is (contains? (into #{} (:task-ids used-executor)) "my-task")))))

      (testing "multiple task creation"
        (let [[pool executor] (testies)]
          (commit pool (map->Commit {:scheduler-id "test-scheduler"
                                     :actions [{:id "my-task-1"
                                              :type :create
                                              :executor-id (:id executor)
                                              :resources {:memory 50 :cpu 1}}
                                             {:id "my-task-2"
                                              :type :create
                                              :executor-id (:id executor)
                                              :resources {:memory 50 :cpu 1}}]
                                     :allow-partial-commit false}))

        (let [{:keys [tasks]} (pools/get-snapshot pool)
              task1 (get tasks "my-task-1")
              task2 (get tasks "my-task-2")]
          (is task1)
          (is task2)
          (is (= "my-task-1") (:id task1))
          (is (= "my-task-2") (:id task2)))))

      ))
