(ns io.alphatier.constraints-test
  (:require [clojure.test :refer :all]
            [io.alphatier.constraints :refer :all]
            [io.alphatier.tools :as tools]
            [io.alphatier.pools :as pools]))

(deftest add-test
  (let [pool (tools/create-test-pool)]

    (testing "adding pre constraints"
      (add pool :pre :test (fn [commit pre-snapshot] nil))
      (is (get-in @pool [:constraints :pre :test])))

    (testing "adding post constraints"
      (add pool :post :test (fn [commit pre-snapshot post-snapshot] nil))
      (is (get-in @pool [:constraints :post :test])))))

(deftest del-test
  (let [pool (tools/create-test-pool)]

    (testing "adding post constraints"
      (add pool :post :test (fn [commit pre-snapshot post-snapshot] nil))
      (del pool :post :test)
      (is (not (get-in @pool [:constraints :pre :test]))))

    (testing "adding post constraints"
      (add pool :post :test (fn [commit pre-snapshot post-snapshot] nil))
      (del pool :post :test)
      (is (not (get-in @pool [:constraints :pre :test]))))))
