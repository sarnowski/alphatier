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
   current one. In order to achieve the optimistic locking, you can add the following parameters to the supplied actions:

   * `:executor-metadata-version` referencing the executor's metadata version
   * `:executor-task-ids-version` referencing the executor's tasks version
   * `:metadata-version` referencing the task's metadata version"
  [commit pre-snapshot]

  (let [versions {:executor-metadata-version #(get-in pre-snapshot [:executors (:executor-id %) :metadata-version])
                  :executor-task-ids-version #(get-in pre-snapshot [:executors (:executor-id %) :task-ids-version])
                  :metadata-version #(get-in pre-snapshot [:tasks (:id %) :metadata-version])}
        actions (->> commit :actions (filter (comp not-empty #(select-keys % (keys versions)))))]
    (->> actions
         (reduce
           (fn [rejected-actions action]
             (let [dirty? (fn [[field extractor]]
                            (and (contains? action field)
                                 (not= (field action) (extractor action))))]
               (if (some dirty? versions)
                 (conj rejected-actions action)
                 rejected-actions)))
           []))))

(defn no-resource-overbooking [commit pre-snapshot post-snapshot]
  (let [sum (fn [resources] (reduce (partial merge-with +) resources))
        actions (->> commit :actions (filter (comp #{:create} :type)) (group-by :executor-id))
        tasks (->> pre-snapshot :tasks vals (group-by :executor-id))
        reserved-resources (->> tasks
                                (map (fn [[k v]] [k (->> v (map :resources) sum)]))
                                (into {}))
        executors (-> pre-snapshot :executors vals)]

    (->> executors
         (map (fn [executor]
                (reduce (fn [result action]
                          (let [new-resources (merge-with + (:resources action) (:resources result))
                                free-resources (merge-with - (:resources executor) new-resources)]
                            (if (some #(< % 0) (vals free-resources))
                              (update-in result [:rejected] conj action)
                              (assoc result :resources new-resources))))
                        {:resources (-> executor :id reserved-resources)
                         :rejected  []}
                        (actions (:id executor)))))
         (map :rejected)
         flatten)))
