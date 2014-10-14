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
  (:require [clojure.core.typed :refer [ann defalias Any List fn IFn U Map All Coll let ASeq Vec Seqable HMap Seq]]
            [clojure.core.incubator :refer [dissoc-in]]
            [io.alphatier.pools :refer :all])
  (:import [io.alphatier.pools Pool Commit Task Executor]))

(ann ^:no-check add-constraint [Pool ConstraintType Any Constraint -> Pool])
(defn- add-constraint [pool type name constraint]
  (assoc-in pool [:constraints type name] constraint))

(ann add (IFn [PoolRef ':pre Any PreConstraint -> Pool]
              [PoolRef ':post Any PostConstraint -> Pool]))
(defn add
  "To add a constraint to a pool, you have to provide its type which is `:pre` or `:post`."
  [pool type name constraint]
  (dosync
    (alter pool add-constraint type name constraint)))

(ann ^:no-check del-constraint [Pool ConstraintType Any -> Pool])
(defn- del-constraint [pool type name]
  (dissoc-in pool [:constraints type name]))

(ann del [PoolRef ConstraintType Any -> Pool])
(defn del
  "Constraints are identified by their name and can be removed by it."
  [pool type name]
  (dosync
    (alter pool del-constraint type name)))

;; ### Built-in constraints
;;
;; This list of built-in constraints is automatically added to all new pools.

(defalias VersionType (U ':executor-metadata-version ':executor-task-ids-version ':metadata-version))
(defalias VersionExtractor [Snapshot Action -> Number])

(ann ^:no-check versions (Map VersionType VersionExtractor))
(def ^:private versions
  {:executor-metadata-version #(get-in %1 [:executors (:executor-id %2) :metadata-version])
   :executor-task-ids-version #(get-in %1 [:executors (:executor-id %2) :task-ids-version])
   :metadata-version #(get-in %1 [:tasks (:id %2) :metadata-version])})

(ann versioned? [Action -> Boolean])
(defn- versioned? [action]
  (boolean (seq (select-keys action (keys versions)))))

(ann version-outdated? [Snapshot Action -> ['[VersionType VersionExtractor] -> Boolean]])
(defn- version-outdated? [snapshot action]
  (fn [[type extractor]]
    (and (contains? action type)
         (not= (type action) (extractor snapshot action)))))

(ann outdated? [Snapshot Action -> Boolean])
(defn- outdated? [snapshot action]
  (boolean (some (version-outdated? snapshot action) versions)))

(ann collect-if (All [x] [[x -> Boolean] -> [(ASeq x) x -> (ASeq x)]]))
(defn- collect-if [pred]
  (fn [coll x]
    (if (pred x)
      (conj coll x)
      coll)))

(ann optimistic-locking PreConstraint)
(defn optimistic-locking
  "Every task can use optimistic locking semantic to be only applied if the state, used by the scheduler, is still the
   current one. In order to achieve the optimistic locking, you can add the following parameters to the supplied actions:

   * `:executor-metadata-version` referencing the executor's metadata version
   * `:executor-task-ids-version` referencing the executor's tasks version
   * `:metadata-version` referencing the task's metadata version"
  [commit pre-snapshot]

  (let [actions (->> commit :actions (filter versioned?))
        outdated-now? (partial outdated? pre-snapshot)
        collect :- [(ASeq Action) Action -> (ASeq Action)] (collect-if outdated-now?)
        rejected-actions :- (ASeq Action) '()]
    (reduce collect rejected-actions actions)))

(ann ^:no-check create-action? [Action -> Boolean])
(def ^:private create-action? (comp #{:create} :type))

(ann ^:no-check resources-of [Any -> Resources])
(def ^:private resources-of :resources)

; TODO can we check this?
(ann ^:no-check sum-resources ['[Any (Seqable Task)] -> '[Any Resources]])
(defn- sum-resources [[executor-id tasks]]
  [executor-id (->> tasks
                    (map resources-of)
                    (reduce (partial merge-with +)))])

(ann calculate-consumed-resources [(Map Any (Seqable Task)) -> (Map Any Resources)])
(defn- calculate-consumed-resources [tasks]
  (->> tasks (map sum-resources) (into {})))

(ann ^:no-check executor-id (IFn [Action -> Any]
                                 [Task -> Any]))
(def ^:private executor-id :executor-id)

; TODO move somewhere else
(ann clojure.core/group-by (All [x y] [[x -> y] (U nil (Seqable x)) -> (Map y (Vec x))]))

(defalias IntermediateResult (HMap :mandatory {:resources Resources
                                               :rejected (Vec Action)}))

(ann ^:no-check reject [IntermediateResult Action -> IntermediateResult])
(defn- reject [result action]
  (update-in result [:rejected] conj action))

(ann simulate-commit [Executor IntermediateResult Action -> IntermediateResult])
(defn simulate-commit [executor result action]
  (let [new-resources (merge-with + (resources-of result) (resources-of action))
        free-resources (merge-with - (resources-of executor) new-resources)]
    (if (some (partial > 0) (vals free-resources))
      (reject result action)
      (assoc result :resources new-resources))))

(ann no-resource-overbooking PostConstraint)
(defn no-resource-overbooking [commit pre-snapshot post-snapshot]
  (let [actions :- (Map Any (Vec Action)) (->> commit :actions (filter create-action?) (group-by executor-id))
        tasks :- (Map Any (Vec Task)) (->> pre-snapshot :tasks vals (group-by executor-id))
        reserved-resources (calculate-consumed-resources tasks)
        executors (-> pre-snapshot :executors vals)]

    (->> executors
         (map (fn [executor :- Executor]
                (reduce (partial simulate-commit executor)
                        {:resources (get reserved-resources (:id executor) {})
                         :rejected  []}
                        (get actions (:id executor) []))))
         (map :rejected)
         flatten)))

(ann with-defaults [PoolRef -> PoolRef])
(defn with-defaults [pool]
  (add pool :pre :optimistic-locking optimistic-locking)
  (add pool :post :no-resource-overbooking no-resource-overbooking)
  pool)
