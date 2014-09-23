;; ## Constraints
;;
;; In order to prevent total chaos with multiple schedulers, one can define constraints to limit the possibilities of
;; schedulers running amok. Some constraints are already built in like forbidding resource overbooking. Others can be
;; defined by yourself.
;;
;; In general, there are two kinds of constraints: **pre-constraints** that run before the commit is applied and
;; **post-constraints** that are run after the commit. All constraints can return a list of task IDs that should get
;; rejected. All constraints run within the commit transaction and can lead to atomic rejections of the commit.
;;
;; **pre-constraints** are functions with a signature `(fn [commit pre-snapshot])`, while **post-constraints** require
;; the following signature: `(fn [commit pre-snapshot post-snapshot])`.
(ns io.alphatier.constraints
  (:require [clojure.core.incubator :as clojure-incubator]))

(defn add
  "To add a constraint to a pool, you have to provide its type which is `:pre` or `:post`."
  [pool type name fn]
  (dosync
    (alter pool assoc-in [:constraints type name] fn)))

(defn del
  "Constraints are identified by their name and can be removed by it."
  [pool type name]
  (dosync
    (alter pool clojure-incubator/dissoc-in [:constraints type name])))

;; ### Built-in constraints
;;
;; This list of built-in constraints is automatically added to all new pools.

(defn optimistic-locking
  "Every task can use optimistic locking semantic to be only applied if the state, used by the scheduler, is still the
   current one. In order to achieve the optimistic locking, you can add the following parameters to the supplied tasks:

   * `:executor-metadata-version` referencing the executor's metadata version
   * `:executor-task-ids-version` referencing the executor's tasks version
   * `:task-metadata-version` referencing the task's metadata version"
  [commit pre-snapshot]
  ; TODO
  [])

(comment "old stuff"
  (defn- resource-exceeded?
    "The sum of the reserved resources of one type should not be higher than the given resource of the executor."
    [pool executor key]
    (let [tasks (map #(get-in @pool [:tasks %]) (:task-ids executor))]
      (> (reduce + (map #(get-in % [:resources key]) tasks))
         (get (:resources executor) key))))

  (defn- resources-exceeded?
    "Not a single resource of an executor must be exceeded."
    [pool executor]
    (some #(resource-exceeded? pool executor %) (keys (:resources executor))))

  (defn- executors-exceeding-resource-limits [pool]
    (filter #(resources-exceeded? pool %) (:executors @pool))))

(defn no-resource-overbooking [commit pre-snapshot post-snapshot]
  ; TODO
  [])
