;; ## Schedulers
;;
;; Schedulers are the decision maker in the Alphatier world. They are able to create and kill tasks every time they like
;; to. Possible scenarios are server cluster management where every executor represents one host in your data centers
;; and a "service scheduler" which can let the hosts (executors) start or stop applications on them. Another example
;; scenario is your office kitchen where each coffee machine can brew 4 coffees at a time. Every coffee machine
;; can be represented with an executor and you write a scheduler that is able to manage which coffee machine brews which
;; colleague's coffee.
(ns io.alphatier.schedulers)


;; ### ACID without D
;;
;; Alphatier changes are [ACID](http://en.wikipedia.org/wiki/ACID) without the D (durable). **A**tomicity is optional
;; for the scheduler, **C**onsistency and **I**solation are guarenteed. The pools are an in-memory datastructure and
;; therefor aren't durable across JVM restarts.
;;
;; Task changes on the pool happen via a commit. A commit may contain multiple creation or kill requests for tasks.
;; Every accepted action itself fulfills the atomicity criteria. Optionally, a scheduler can force atomicity for the
;; whole commit.
;;
;; Consistency is ensured through various [constraints](#io.alphatier.constraints). Those can also include user defined
;; constraints. Built-in is optimistic locking of executor or task data and protection against resource overbooking.
;;
;; Isolation is provided through [Clojure's STM](http://clojure.org/refs). If writes happen to the state while a
;; transaction runs, the transaction will be repeated with the new state.
(gen-class :name "io.alphatier.schedulers.RejectedException"
           :extends IllegalStateException)

;; ## Commits
;;
;; All changes to the pool by a scheduler have to go through a commit. A scheduler can do three actions:
;;
;; * **create** new tasks, assigned to an executor
;; * **update** a task's metadata in order to influence the task during runtime
;; * **kill** a task

; TODO metadata/tasks-version resets
(defn- create-task
  "Creating a tasks registers it in the pool and assigns it to the given executor."
  [pool task]
  (alter pool assoc-in [:tasks (:id task)] task)
  (alter pool update-in [:executors (:executor-id task) :task-ids] conj (:id task))
  (alter pool update-in [:executors (:executor-id task) :tasks-version] inc))

(defn- update-task
  "Updating a task modifies the task's metadata."
  [pool task]
  (alter pool update-in [:tasks (:id task) :metadata] merge (:metadata task))
  (alter pool update-in [:tasks (:id task) :metadata-version] inc))

(defn- kill-task
  "Killing a tasks sets its lifecycle-phase to `:kill`, so that the executor can actually kill it."
  [pool task]
  (alter pool assoc-in [:tasks (:id task) :lifecycle-phase] :kill))


;; ### Built-in constraints
;;
;; Currently, schedulers are not allowed to exceed resources of executors when creating new tasks.

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
  (filter #(resources-exceeded? pool %) (:executors @pool)))


;; ### How to implement a scheduler?
;;
;; The basic workflow for a scheduler looks like the following:
;;
;; 1. get a [current snapshot](#io.alphatier.pools) of the pool
;; 2. do some number crunching in order to fulfill your current actions (e.g. find good fitting executors for your new
;;    tasks or some tasks that you want to shut down)
;; 3. commit your decision to the pool

(defn commit
"### Committing

To execute a commit, you need to provide at least a commit identifier and an identifier for your scheduler. The
commit ID is used for debugging purposes (e.g. logs). The scheduler ID will be used to apply user defined constraints
to it. The following options are available:

* `:create` contains a list of all tasks to create
* `:update` contains a list of all tasks to update
* `:kill` contains a list of all tasks to kill
* `:allow-partial` is a boolean describing if single tasks should succeed even if other tasks fail
* `:force` disables all constraint checks

All task list entries contain at least the task ID the request is for. Every entry may include the following counters
for optimistic locking:

* `:executor-metadata-version`
* `:executor-tasks-version`
* `:task-metadata-version`

If set, the versions will be checked with the current pool state and rejected if not equal."
  [pool commit-id scheduler-id & {:keys [create update kill allow-partial-commit force]
                                  :or {create []
                                       update []
                                       kill []
                                       allow-partial-commit false
                                       force false}}]
  (dosync
    ; TODO precheck correct metadata and tasks versions
    ; (filter matches-version? tasks) (no-partial? (throw))

    (dorun (map #(create-task pool %) create))
    (dorun (map #(update-task pool %) update))
    (dorun (map #(kill-task pool %) kill))

    ; TODO postcheck all constrains (resource limits, scheduler limits, ...)
    (let [broken-executors (executors-exceeding-resource-limits pool)]
      (when (not (empty? broken-executors))
        (throw (io.alphatier.schedulers.RejectedException. (pr-str "broken executors" broken-executors))))))
  pool)

;; ### Java usage
;;
;; The `commit` function can be accessed via the `Schedulers` utility class.
;;
;;     Schedulers.commit(commitId, schedulerId, commitOptions);
(gen-class
  :name "io.alphatier.Schedulers"
  :main false
  :prefix "java-"
  :methods [#^{:static true} [commit [String String java.util.Map] void]])

(defn- java-commit [pool commitId schedulerId options] (commit pool commitId, schedulerId, options))
