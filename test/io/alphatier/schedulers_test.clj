(ns io.alphatier.schedulers-test
  (:import [clojure.lang ExceptionInfo])
  (:require [clojure.test :refer :all]
            [io.alphatier.schedulers :refer :all]
            [io.alphatier.tools :as tools]
            [io.alphatier.pools :as pools]))

(defn- testies []
  (let [pool (tools/create-test-pool)
        executor (-> pool pools/get-snapshot :executors vals first)]
    [pool executor]))

(deftest commit-test

    (testing "creation"
        (testing "duplicate task ids"
          (let [[pool executor] (testies)]
            (try
              (commit pool (map->Commit {:scheduler-id "test-scheduler"
                                         :tasks [{:id "my-task"
                                                  :action :create
                                                  :executor-id (:id executor)
                                                  :resources {:memory 50 :cpu 1}}
                                                 {:id "my-task"
                                                  :action :create
                                                  :executor-id (:id executor)
                                                  :resources {:memory 50 :cpu 1}}]
                                         :allow-partial-commit false}))
              (is false "Expected rejection")
            (catch ExceptionInfo e
              (is (.contains (.getMessage e) "duplicate tasks"))))))

      (testing "duplicate create tasks"
        (let [[pool executor] (testies)
              create-commit (map->Commit {:scheduler-id "test-scheduler"
                                          :tasks [{:id "my-task"
                                                   :action :create
                                                   :executor-id (:id executor)
                                                   :resources {:memory 50 :cpu 1}}]
                                          :allow-partial-commit false})]
          (commit pool create-commit)
            (try
              (commit pool create-commit)
              (is false "Expected rejection")
            (catch ExceptionInfo e
              (is (.contains (.getMessage e) "duplicate create tasks"))))))

      (testing "update for missing task"
        (let [[pool executor] (testies)
              create-commit (map->Commit {:scheduler-id "test-scheduler"
                                          :tasks [{:id "my-task"
                                                   :action :update
                                                   :executor-id (:id executor)
                                                   :resources {:memory 50 :cpu 1}}]
                                          :allow-partial-commit false})]
          (try
            (commit pool create-commit)
            (is false "Expected rejection")
          (catch ExceptionInfo e
            (is (.contains (.getMessage e) "missing task for update"))))))

      (testing "kill for missing task"
        (let [[pool executor] (testies)
              create-commit (map->Commit {:scheduler-id "test-scheduler"
                                          :tasks [{:id "my-task"
                                                   :action :kill
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
                                          :tasks [{:id "my-task"
                                                   :action :create
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
                                          :tasks [{:id "my-task"
                                                   :action :create
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
                                     :tasks [{:id "my-task"
                                              :action :create
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
                                     :tasks [{:id "my-task-1"
                                              :action :create
                                              :executor-id (:id executor)
                                              :resources {:memory 50 :cpu 1}}
                                             {:id "my-task-2"
                                              :action :create
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
