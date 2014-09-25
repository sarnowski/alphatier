(ns io.alphatier.assert
  (:require [clojure.test :refer :all]))

(defn fail
  ([] (fail ""))
  ([msg] (is false msg)))