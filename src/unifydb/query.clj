(ns unifydb.query
  (:refer-clojure :exclude [var?])
  (:require [clojure.core.match :refer [match]]
            [manifold.stream :as s]
            [taoensso.timbre :as log]
            [unifydb.binding :as binding :refer [var? var-name]]
            [unifydb.comparison :as comparison]
            [unifydb.facts :refer [fact-entity fact-attribute fact-added?]]
            [unifydb.id :as id]
            [unifydb.messagequeue :as queue]
            [unifydb.query.pull :as pull]
            [unifydb.rules :refer [rule-body rule-conclusion]]
            [unifydb.schema :as schema]
            [unifydb.service :as service]
            [unifydb.statistics :as stats]
            [unifydb.storage :as store]
            [unifydb.unify :as unify]
            [unifydb.util :as u])
  (:import [clojure.lang ExceptionInfo]))

(declare qeval do-query)

(defn conjoin
  "Evaluates the conjunction (logical AND) of all `conjuncts` in the context of `frames`.
   Returns a seq of frames."
  [db conjuncts rules frames]
  (log/debug "Executing conjoin" :conjuncts conjuncts :rules rules :frames frames)
  (if (empty? conjuncts)
    frames
    (conjoin db
             (rest conjuncts)
             rules
             (qeval db (first conjuncts) rules frames))))

(defn disjoin
  "Evaluates the disjunctions (logical OR) of all `disjuncts` in the context of `frames`.
   Returns a seq of frames."
  [db disjuncts rules frames]
  (log/debug "Executing disjoin" :disjuncts disjuncts :rules rules :frames frames)
  (if (empty? disjuncts)
    []
    (concat (qeval db (first disjuncts) rules frames)
            (disjoin db (rest disjuncts) rules frames))))

;; This is a shitty implementation of negation because it acts only as
;; a filter, meaning it is only valid as one of the subsequent clauses
;; in an :and query.  In other words, [:not [?e :name "Foo"]] always
;; returns the empty stream, even if there are entities in the
;; database whose :name is not "Foo". To get this to work right you'd
;; need to do [:and [?e ?a ?v] [:not [?e :name "Foo"]]] - in other
;; words, generating a stream of every fact in the database and then
;; passing it through the :not filter. This is an acceptable solution
;; for now but it should be clearly documented and hopefully one day
;; improved.
;;
;; See also: https://en.wikipedia.org/wiki/Negation_as_failure
;;           https://en.wikipedia.org/wiki/Closed-world_assumption
(defn negate
  "Evaluates the `negatee` in the context of `frames`, returning a seq of only
   those frames for which evaluation fails (i.e. for which the logic query cannot be made true)."
  [db negatee rules frames]
  (log/debug "Executing negate" :negatee negatee :rules rules :frames frames)
  (mapcat
   (fn [frame]
     (if (empty? (qeval db negatee rules [frame]))
       [frame]
       []))
   frames))

(defn safe-ns-resolve
  "Like ns-resolve but never returns `'clojure.core/eval`."
  [ns sym]
  (let [resolved (ns-resolve ns sym)]
    (when (not= resolved #'clojure.core/eval)
      resolved)))

(defn apply-predicate
  "Applies the `pred` to the `args` in the context of `frames`,
  returning a seq of frames."
  [_db pred args _rules frames]
  (mapcat
   (fn [frame]
     (let [instantiated (binding/instantiate
                         frame args
                         (fn [v _f]
                           (let [var (second v)
                                 msg (format "Unbound variable %s" var)]
                             (throw (ex-info msg
                                             {:code :unbound-variable
                                              :variable (str var)
                                              :message msg})))))
           operator (or (safe-ns-resolve 'clojure.core pred)
                        (match pred
                          '!= not=
                          other (let [msg (format "Unknown predicate %s"
                                                  other)]
                                  (throw (ex-info msg
                                                  {:code :unknown-predicate
                                                   :predicate (str other)
                                                   :message msg})))))]
       (if (apply operator instantiated)
         [frame]
         [])))
   frames))

(defn apply-func
  "Calls `func` on the `args` instantiated against the `frames`,
  unifying the result with the `binding-var` and returning a seq of
  new frames."
  [func args binding-var frames]
  (map
   (fn [frame]
     (let [instantiated (binding/instantiate
                         frame args
                         (fn [v _f]
                           (let [var (var-name v)
                                 msg (format "Unbound variable %s" var)]
                             (throw (ex-info msg {:code :unbound-variable
                                                  :variable (str var)
                                                  :message msg})))))
           func (or (safe-ns-resolve 'clojure.core func)
                    (let [msg (format "Unknown function %s"
                                      func)]
                      (throw (ex-info msg
                                      {:code :unknown-function
                                       :function (str func)
                                       :message msg}))))
           result (apply func instantiated)]
       (unify/unify-match result binding-var frame)))
   frames))

(defn cmp-fact-versions
  "Like compare, but gives false a higher priority than true."
  [f1 f2]
  (match [f1 f2]
    [false true] 1
    [true false] -1
    [(true :<< coll?) (true :<< coll?)]
    (if (= (count f1) (count f2))
      (if (empty? f1)
        0
        (let [cmp (cmp-fact-versions (first f1)
                                     (first f2))]
          (if (zero? cmp)
            (cmp-fact-versions (rest f1) (rest f2))
            cmp)))
      (- (count f1) (count f2)))
    :else (if (= (type f1) (type f2))
            (compare f1 f2)
            (- (hash f1) (hash f2)))))

(defn filter-sorted-facts
  "Filters out facts that have been retracted."
  [acc sorted-facts]
  (if (empty? sorted-facts)
    acc
    (if (and (not (fact-added? (first sorted-facts)))
             (and (= (fact-attribute (first sorted-facts))
                     (fact-attribute (second sorted-facts)))
                  (fact-added? (second sorted-facts))))
      (recur acc (rest (rest sorted-facts)))
      (recur (if (fact-added? (first sorted-facts))
               (conj acc (first sorted-facts))
               acc)
             (rest sorted-facts)))))

(defn get-cardinalities
  [db attrs]
  (let [schemas (u/join
                 (do-query db (schema/make-schema-query attrs)))]
    (reduce (fn [acc attr]
              (if-let [cardinality
                       (get-in schemas [attr :unifydb/cardinality])]
                (assoc acc attr cardinality)
                acc))
            {}
            (keys schemas))))

(defn process-facts
  "Filters out any facts that have been retracted and
   handles attribute cardinality."
  [db facts]
  (if (empty? facts)
    []
    (let [_ (log/debug "Processing facts" :facts facts)
          grouped (group-by
                   (juxt fact-entity fact-attribute)
                   facts)
          attrs (set (map #'fact-attribute facts))
          cardinalities (get-cardinalities db attrs)]
      (mapcat
       (fn [attr-group]
         (let [sorted-facts (reverse (sort cmp-fact-versions attr-group))
               filtered-facts (if (:historical db)
                                sorted-facts
                                (filter-sorted-facts
                                 []
                                 sorted-facts))
               cardinality (get cardinalities
                                (fact-attribute (first attr-group)))]
           (if (or (:historical db) (= cardinality :cardinality/many))
             filtered-facts
             (take 1 filtered-facts))))
       (vals grouped)))))

(defn concrete?
  "Whether `o` is a real value, as opposed to a variable or wildcard."
  [o]
  (and (not (var? o))
       (not (= o '_))))

(defn match-facts
  "Returns a seq of frames obtained by pattern-matching the `query`
   against the facts in `db` in the context of `frame`."
  [db query frame]
  (let [[eid attr val] (binding/instantiate frame query)
        tx-id (if (= (:tx-id db) :latest)
                (id/id Integer/MAX_VALUE)
                (id/id (:tx-id db)))
        facts (log/spy
               :debug :processed-facts
               (process-facts
                db
                (log/spy :debug :facts
                         (store/get-matching-facts (:storage-backend db)
                                                   {:entity-id (when (concrete? eid) eid)
                                                    :attribute (when (concrete? attr) attr)
                                                    :value (when (concrete? val) val)
                                                    :tx-id tx-id}))))]
    (log/spy
     :debug :match-results
     (filter
      #(not= :failed %)
      (map
       #(unify/unify-match query % frame)
       facts)))))

(defn rename-vars
  "Gives all the variables in the rule globally unique names
   to prevent name collisions during unification."
  [rule]
  (let [bindings (atom {})
        rename-var (fn [var]
                     (let [v (var-name var)
                           binding (get @bindings v)]
                       (if binding
                         ['? binding]
                         (let [new-binding (gensym v)]
                           (swap! bindings (fn [m] (assoc m v new-binding)))
                           ['? new-binding]))))
        rename-vars (fn rename-vars [exp]
                      (cond
                        (var? exp) (rename-var exp)
                        (and (sequential? exp) (seq exp))
                        (cons (rename-vars (first exp))
                              (rename-vars (rest exp)))
                        :else exp))]
    (rename-vars rule)))

(defn apply-rule [db query rules rule frame]
  (log/debug "Applying rule" :query query :rule rule :rules rules :frame frame)
  (let [clean-rule (rename-vars rule)
        unify-result (unify/unify-match query
                                        (rule-conclusion clean-rule)
                                        frame)]
    (if (= unify-result :failed)
      []
      (qeval db (rule-body clean-rule) rules [unify-result]))))

(defn apply-rules [db query rules frame]
  ;; TODO parallelize this across queue backend as well
  (mapcat #(apply-rule db query rules % frame)
          ;; Only apply rules whose names explicitly match the query
          (filter #(= (first query) (first (rule-conclusion %))) rules)))

(defn simple-query
  "Evaluates a non-compound query, returning a seq of frames."
  [db query rules frames]
  (log/debug "Executing simple query" :query query :rules rules :frames frames)
  (mapcat
   (fn [frame]
     (concat (match-facts db query frame)
             (apply-rules db query rules frame)))
   frames))

(defn qeval
  "Evaluates a logic query given by `query` in the context of `frames`.
   Returns a seq of frames."
  [db query rules frames]
  (log/debug "Evaluating query" :query query :rules rules :frames frames)
  (log/spy :debug :query-results
           (match (vec query)
             [:and & conjuncts] (conjoin db conjuncts rules frames)
             [:or & disjuncts] (disjoin db disjuncts rules frames)
             [:not negatee] (negate db negatee rules frames)
             [([func & args] :seq) binding-var] (apply-func func args binding-var frames)
             [([pred & args] :seq)] (apply-predicate db pred args rules frames)
             [:always-true] frames
             _ (simple-query db query rules frames))))

(defn pad-clause
  "Ensures the clause is a 5-tuple by padding it out with _s if necessary."
  [clause]
  (take 5 (concat clause (repeat '_))))

(defn expand-question-marks [where]
  (u/map-over-symbols #'u/expand-question-mark where pull/pull-exp?))

(defn process-clause
  "Processes a clause by expanding variables and padding it out to a 5-tuple."
  [clause]
  (match (vec clause)
    [:and & conjuncts] `[:and ~@(map process-clause conjuncts)]
    [:or & disjuncts] `[:or ~@(map process-clause disjuncts)]
    [:not negatee] `[:not ~(process-clause negatee)]
    [([func & args] :seq) binding-var] `[(~func ~@(map expand-question-marks args))
                                         ~(expand-question-marks binding-var)]
    [([op & args] :seq)] `[(~op ~@(map expand-question-marks args))]
    _ (-> clause (pad-clause) (expand-question-marks))))

(defn process-where
  "Processes a where clause by wrapping the whole clause in an :and
   and making sure each clause is a 5-tuple, padding them out with _s
   if necessary."
  [where]
  (conj
   (map process-clause where)
   :and))

(defn process-rules
  "Processes rules by wrapping the rule bodies in an :and and
   expanding all ?vars to [? var]."
  [rules]
  (map
   (fn [rule]
     (if (empty? (rest rule))
       (vector (process-clause (first rule)))
       `[~(process-clause (first rule))
         [:and ~@(map process-clause (rest rule))]]))
   rules))

(defn process-bind
  "Processes the bind clause by transforming it into a map.
  `bind` must be a map or a sequence of pairs."
  [bind]
  (into {} bind))

(defn aggregate?
  "Whether `exp` is an aggregatation expression in a find clause."
  [exp]
  (list? exp))

(defn aggregate
  "Given an aggregation expression `agg` and a list of frames
  `frames`, returns the result of applying the aggregration expression
  to the frames."
  [agg frames]
  (let [apply-agg (fn [agg-fn arg]
                    (agg-fn (map #(binding/instantiate % arg)
                                 frames)))]
    (match agg
      (['sum arg] :seq) (apply-agg (partial apply +) arg)
      (['min arg] :seq) (apply-agg (partial apply min) arg)
      (['max arg] :seq) (apply-agg (partial apply max) arg)
      (['mean arg] :seq) (apply-agg stats/mean arg)
      ;; avg is an alias for mean
      (['avg arg] :seq) (apply-agg stats/mean arg)
      (['median arg] :seq) (apply-agg stats/median arg)
      (['mode arg] :seq) (apply-agg #(vec (stats/mode %)) arg)
      (['stddev arg] :seq) (apply-agg stats/standard-deviation arg)
      (['count arg] :seq) (apply-agg #(count (filter some? %)) arg)
      (['count-distinct arg] :seq) (apply-agg #(count (set (filter some? %))) arg)
      (['distinct arg] :seq) (apply-agg (partial apply (comp set list)) arg)
      ([exp & _] :seq) (let [msg (format "Unknown aggregation expression %s" exp)]
                         (throw (ex-info msg
                                         {:code :unknown-aggregation
                                          :aggregation (str exp)
                                          :message msg}))))))

(defn process-results
  "Returns a map where the keys are expressions and the values are
  their values for the `frame-group`."
  [find sort-by frame-group]
  (into {} (map
            (fn [exp]
              (cond
                (pull/pull-exp? exp) [exp (binding/instantiate (first frame-group)
                                                               (pull/pull-entity exp))]
                (aggregate? exp) [exp (aggregate exp frame-group)]
                (var? exp) [exp (binding/instantiate (first frame-group) exp)]))
            (set (concat find sort-by)))))

(defn realize-find
  "Given a group of frames and a find clause, returns a result row."
  [find result-map]
  (vec (map (partial get result-map) find)))

(defn sort-results
  "Sorts a list of result maps `results` by the `sort-clause`."
  [sort-clause sort-direction results]
  (if (nil? sort-clause)
    results
    (sort-by (apply juxt (map (fn [exp] #(get % exp)) sort-clause))
             (if (= sort-direction :desc)
               #(comparison/cc-cmp %2 %1)
               comparison/cc-cmp)
             results)))

(defn do-pull
  [db find result-maps]
  (let [pull-exps (filter pull/pull-exp? find)]
    (if (seq pull-exps)
      (let [ids (into {} (map (fn [exp]
                                [exp (map #(get % exp) result-maps)])
                              pull-exps))
            queries (u/mapm (fn [exp ids]
                              [exp (pull/make-pull-query (pull/pull-query exp) ids)])
                            ids)
            pull-results (u/mapm (fn [exp query]
                                   [exp (do-query db (:query query))])
                                 queries)
            pull-attrs (set (mapcat pull/pull-exp-attrs pull-exps))
            cardinalities (get-cardinalities db pull-attrs)
            parsed (u/mapm (fn [exp results]
                             (let [{:keys [query depth]} (get queries exp)]
                               [exp (pull/parse-pull-rows results
                                                          (expand-question-marks (:find query))
                                                          depth
                                                          cardinalities)]))
                           pull-results)]
        (map (fn [result-map]
               (apply assoc result-map
                      (mapcat (fn [pull-exp]
                                (let [id (get result-map pull-exp)
                                      results (get parsed pull-exp)]
                                  [pull-exp (get results id)]))
                              pull-exps)))
             result-maps))
      result-maps)))

(defn process-frames
  "Processes `frames` returns from a `qeval` against the `find`
  clause, returning a vector of query results. Results are sorted by
  `sort-by`, it it's not nil. Aggregates frames based on any
  aggregation expressions in the `find` and the `sort-by`."
  [db find sort-by sort-direction limit frames]
  (let [grouping-vars (set (concat (filter var? find)
                                   (map pull/pull-entity
                                        (filter pull/pull-exp? find))
                                   (filter var? sort-by)))
        groupings (if (empty? grouping-vars)
                    ;; If there's nothing to group by, just have one big group
                    {:all frames}
                    (group-by (apply juxt
                                     (map (fn [var]
                                            (fn [frame]
                                              (binding/instantiate frame var)))
                                          grouping-vars))
                              frames))
        result-maps (map (partial process-results find sort-by) (vals groupings))
        ;; TODO apply limit and sort before pulling or realizing results
        result-maps (do-pull db find result-maps)
        results (map (partial realize-find find)
                     (sort-results sort-by sort-direction result-maps))]
    (if limit
      (vec (take limit results))
      (vec results))))

(defn process-sort-by
  [sort-by]
  (if-not (nil? sort-by)
    (let [sort-by (if (vector? sort-by) sort-by [sort-by])
          direction (if (= (last sort-by) :desc) :desc :asc)
          sort-by (if (#{:desc :asc} (last sort-by))
                    (subvec sort-by 0 (dec (count sort-by)))
                    sort-by)]
      [direction (vec (expand-question-marks sort-by))])
    [:asc nil]))

(defn process-find [find]
  (vec (map (fn [exp]
              (if (pull/pull-exp? exp)
                (pull/pull-exp (expand-question-marks (pull/pull-entity exp))
                               (pull/pull-query exp))
                (expand-question-marks exp)))
            find)))

(defn do-query
  "Runs the query `q` against `db`, returning a seq of instantiated
  find clauses for each frame."
  [db q]
  (let [{:keys [find where rules sort-by limit bind]} q
        processed-where (process-where where)
        processed-find (process-find find)
        processed-rules (process-rules rules)
        [sort-direction processed-sort-by] (process-sort-by sort-by)
        processed-bind (process-bind bind)]
    (process-frames db
                    processed-find
                    processed-sort-by
                    sort-direction
                    limit
                    (qeval db processed-where processed-rules [processed-bind]))))

(defn query-callback [queue-backend storage-backend msg]
  (log/debug "Received query message" :message msg)
  (let [db (-> (:db msg)
               (assoc :queue-backend queue-backend)
               (assoc :storage-backend storage-backend))
        result (assoc (try
                        {:results (do-query db (:query msg))}
                        (catch ExceptionInfo e
                          {:error (ex-data e)}))
                      :id (:id msg)
                      :db (:db msg)
                      :query (:query msg))]
    (queue/publish queue-backend :query/results result)))

(defrecord QueryService [queue-backend storage-backend state]
  service/IService
  (start! [self]
    (let [query-sub (queue/subscribe queue-backend :query :query/queryers)]
      (swap! (:state self) #(assoc % :query-sub query-sub))
      (s/consume
       #(query-callback queue-backend storage-backend %)
       query-sub)))
  (stop! [self]
    (s/close! (:query-sub @(:state self)))))

(defn new [queue-backend storage-backend]
  (->QueryService queue-backend storage-backend (atom {})))
