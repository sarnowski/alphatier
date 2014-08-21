(ns io.alphatier.pools-test
  (:require [clojure.test :refer :all]
            [io.alphatier.pools :refer :all]
            [io.alphatier.tools :as tools]))

(deftest create-test

  (testing "new pool"
    (let [pool (create)]
      (is pool)
      (is (get-snapshot pool))
      (is (contains? (get-snapshot pool) :executors))
      (is (empty? (:executors (get-snapshot pool))))
      (is (contains? (get-snapshot pool) :tasks))
      (is (empty? (:tasks (get-snapshot pool))))))

  (testing "new pool from old snapshot"
    (let [pool (create-with-state (map->Pool {:executors {:k1 :v1}}))]
      (is pool)
      (is (get-snapshot pool))
      (is (contains? (get-snapshot pool) :executors))
      (is (not (empty? (:executors (get-snapshot pool)))))
      (is (= :v1 (:k1 (:executors (get-snapshot pool))))))))
