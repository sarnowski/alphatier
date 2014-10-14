(ns io.alphatier.setup
  (:require [clojure.test :refer :all]
            [io.alphatier.executors :as executors]
            [io.alphatier.pools :as pools]
            [io.alphatier.schedulers :as schedulers]
            [io.alphatier.constraints :as constraints]))

(def ^:private executor-id-seq (atom 0))
(def ^:private scheduler-id-seq (atom 0))
(def ^:private task-id-seq (atom 0))

(defn gen-executor-id []
  (str "executor-" (swap! executor-id-seq inc)))

(defn gen-scheduler-id []
  (str "scheduler-" (swap! scheduler-id-seq inc)))

(defn gen-task-id []
  (str "task-" (swap! task-id-seq inc)))

(defn- default-executor-resources []
  {:cpu 8 :memory 100})

(defn- default-task-resources []
  {:cpu 2 :memory 25})

(defn create-action [executor-id & {:as options}]
  (merge {:id (gen-task-id)
          :type :create
          :executor-id executor-id
          :resources (merge (default-task-resources) (:resources options))}
         options))

(defn create-actions [executor-id & {:keys [size action]
                                     :or {action create-action}}]
  {:pre [(number? size)]}
  (repeatedly size (partial action executor-id)))


(defn create-commit-internal [scheduler-id actions options]
  (pools/map->Commit (merge {:scheduler-id scheduler-id
                             :actions actions
                             :allow-partial-commit false}
                            options)))

(defn create-commit [scheduler-id actions & {:as options}]
  (create-commit-internal scheduler-id actions options))

(defn- register-executor
  ([pool executor-id] (register-executor pool executor-id (default-executor-resources)))
  ([pool executor-id resources](executors/register pool executor-id resources)))

(defn default-commit [scheduler-id executor-id]
  (create-commit scheduler-id (create-actions executor-id :size 3)))

(defn overbooking-commit [scheduler-id executor-id & {:as options}]
  (let [heavy-action #(create-action % :resources {:memory 50})
        heavy-actions (create-actions executor-id :action heavy-action :size 2)
        lightweight-action (create-action executor-id)
        actions (conj (vec heavy-actions) lightweight-action)]
    (create-commit-internal scheduler-id actions options)))

(defn- register-default-executor [pool executor-id]
  (register-executor pool executor-id))

(defn- register-default-tasks [pool executor-id]
  (schedulers/commit pool (default-commit (gen-scheduler-id) executor-id))
  pool)

(defn- register-defaults [pool]
  (let [executor-id (gen-executor-id)]
    (-> pool
        (register-default-executor executor-id)
        (register-default-tasks executor-id))
    pool))

(defn executor-id-of [pool f]
  (-> pool pools/get-snapshot :executors vals f :id))

(defn- new-pool []
  (constraints/with-defaults (pools/create)))

(defn default-pool
  "Creates a default pool containing:
  
  * 1 executor with the following resources
    * :cpu 8
    * :memory 100
  * 3 tasks with the following resources:
    * :cpu 2
    * :memory 25"
  []
  (let [pool (new-pool)]
    (register-defaults pool)))

(defn empty-pool
  "Creates an empty pool containing only:

  * 1 executor with the following resources
    * :cpu 8
    * :memory 100"
  []
  (let [pool (new-pool)]
    (register-default-executor pool (gen-executor-id))))

(defn exhausted-pool
  "Creates an exhausted pool containing:

  * 1 executor with the following resources
    * :cpu 8
    * :memory 100
  * 4 tasks with the following resources:
    * :cpu 2
    * :memory 25"
  []
  (let [pool (empty-pool)
        executor-id (executor-id-of pool first)]
    (schedulers/commit pool (create-commit (gen-scheduler-id) (create-actions executor-id :size 4)))
    pool))

;; constraints

(def pre-pass (constantly []))
(def post-pass (constantly []))

(defn pre-reject [commit _]
  (:actions commit))

(defn post-reject [commit _ _]
  (:actions commit))

(defn reject [commit f]
  (let [rejections (-> commit :actions f)]
    (if (sequential? rejections)
      (vec rejections)
      [rejections])))

(defn pre-reject-only [f]
  (fn [commit _]
    (reject commit f)))

(defn post-reject-only [f]
  (fn [commit _ _]
    (reject commit f)))