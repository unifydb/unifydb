(ns unifydb.query
  (:refer-clojure :exclude [var?])
  (:require [clojure.core.match :refer [match]]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [unifydb.binding :as binding :refer [var? var-name]]
            [unifydb.facts :refer [fact-entity fact-attribute fact-value fact-added?]]
            [unifydb.messagequeue :as queue]
            [unifydb.rules :refer [rule-body rule-conclusion]]
            [unifydb.service :as service]
            [unifydb.storage :as store]
            [unifydb.unify :as unify]
            [unifydb.util :as util :refer [when-let* take-n!]])
  (:import [java.util UUID]))

(declare qeval)

(defn conjoin [db conjuncts rules frames]
  "Evaluates the conjunction (logical AND) of all `conjuncts` in the context of `frames`.
   Returns a seq of frames."
  (if (empty? conjuncts)
    frames
    (conjoin db
             (rest conjuncts)
             rules
             (qeval db (first conjuncts) rules frames))))

(defn disjoin [db disjuncts rules frames]
  "Evaluates the disjunctions (logical OR) of all `disjuncts` in the context of `frames`.
   Returns a seq of frames."
  (if (empty? disjuncts)
    []
    (concat (qeval db (first disjuncts) rules frames)
            (disjoin db (rest disjuncts) rules frames))))

;; This is a shitty implementation of negation because it acts only as a filter,
;; meaning it is only valid as one of the subsequent clauses in an :and query.
;; In other words, [:not [?e :name "Foo"]] always returns the empty stream, even if
;; there are entities in the database whose :name is not "Foo". To get this to work
;; right you'd need to do [:and [?e ?a ?v] [:not [?e :name "Foo"]]] - in other words,
;; generating a stream of every fact in the database and then passing it through the
;; :not filter. This is an acceptable solution for now but it should be clearly documented
;; and hopefully one day improved.
;;
;; See also: https://en.wikipedia.org/wiki/Negation_as_failure
;;           https://en.wikipedia.org/wiki/Closed-world_assumption
(defn negate [db negatee rules frames]
  "Evaluates the `negatee` in the context of `frames`, returning a seq of only
   those frames for which evaluation fails (i.e. for which the logic query cannot be made true)."
  (mapcat
   (fn [frame]
     (if (empty? (qeval db negatee rules [frame]))
       [frame]
       []))
   frames))

(defn cmp-fact-versions [f1 f2]
  "Like compare, but gives false a higher priority than true"
  (match [f1 f2]
    [false true] 1
    [true false] -1
    [(true :<< coll?) (true :<< coll?)]
    (if (= (count f1) (count f2))
      (if (empty? f1)
        0
        (let [cmp (cmp-fact-versions (first f1)
                                     (first f2))]
          (if (= cmp 0)
            (cmp-fact-versions (rest f1) (rest f2))
            cmp)))
      (- (count f1) (count f2)))
    :else (if (= (type f1) (type f2))
            (compare f1 f2)
            (- (hash f1) (hash f2)))))

(defn process-facts [facts]
  "Filters out any facts that have been retracted."
  (let [grouped (group-by
                 (juxt fact-entity fact-attribute fact-value)
                 facts)]
    (filter #(not (nil? %))
     (map
      (fn [fact-versions]
        (let [most-recent (first (reverse (sort cmp-fact-versions fact-versions)))]
          (when (fact-added? most-recent) most-recent)))
      (vals grouped)))))

(defn match-fact [query frame fact]
  (let [match-result (unify/unify-match query fact frame)]
    (if (= match-result :failed)
      nil
      match-result)))

(defn match-callback [queue-backend message]
  (let [{:keys [match-id query frame fact]} message
        match-result (match-fact query frame fact)]
    (queue/publish queue-backend :query/match-results {:match-id match-id
                                                       :result match-result})))

(defn match-facts [db query frame]
  "Returns a seq of frames obtained by pattern-matching the `query`
   against the facts in `db` in the context of `frame`.

   The pattern-matching is done in parallel via the queue backend."
  (let [facts (process-facts
               (store/fetch-facts (:storage-backend db)
                                  query
                                  (:tx-id db)
                                  frame))
        match-id (UUID/randomUUID)
        match-results (queue/subscribe (:queue-backend db) :query/match-results :query/matchers)]
    (doseq [fact facts]
      (queue/publish (:queue-backend db) :query/match {:match-id match-id
                                                       :query query
                                                       :frame frame
                                                       :fact fact}))
    (->> match-results
         (s/filter #(= match-id (:match-id %)))
         (take-n! (count facts))
         (map :result)
         (filter #(not (nil? %))))))

(defn rename-vars [rule]
  "Gives all the variables in the rule globally unique names
   to prevent name collisions during unification."
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
                        (and (sequential? exp) (not (empty? exp)))
                        (cons (rename-vars (first exp))
                              (rename-vars (rest exp)))
                        :else exp))]
    (rename-vars rule)))

(defn apply-rule [db query rules rule frame]
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

(defn simple-query [db query rules frames]
  "Evaluates a non-compound query, returning a seq of frames."
  (mapcat
   (fn [frame]
     (concat (match-facts db query frame)
             (apply-rules db query rules frame)))
   frames))

(defn qeval [db query rules frames]
  "Evaluates a logic query given by `query` in the context of `frames`.
   Returns a seq of frames."
  (match (vec query)
         [:and & conjuncts] (conjoin db conjuncts rules frames)
         [:or & disjuncts] (disjoin db disjuncts rules frames)
         [:not negatee] (negate db negatee rules frames)
         ;; TODO support lisp-value?
         [:always-true] frames
         _ (simple-query db query rules frames)))

;; A query is a map with the following structure:
;;    {:find [?user ?tweet]
;;     :where [[:likes ?user ?tweet]
;;             (presidential ?tweet)]
;;     :rules [[(presidential ?tweet)
;;              [:author ?tweet ?author]
;;              [:job ?author "president"]
;;              (:not [:hand-size ?author "small"])]]}

(defn pad-clause [clause]
  "Ensures the clause is a 5-tuple by padding it out with _s if necessary"
  (take 5 (concat clause (repeat '_))))

(defn map-over-symbols [proc exp]
  (cond
    (and (sequential? exp) (not (empty? exp)))
    (conj (map-over-symbols proc (rest exp))
          (map-over-symbols proc (first exp)))
    (symbol? exp) (proc exp)
    :else exp))

(defn expand-question-mark [sym]
  (let [chars (str sym)]
    (if (= "?" (subs chars 0 1))
      ['? (symbol (subs chars 1))]
      sym)))

(defn expand-question-marks [where]
  (map-over-symbols #'expand-question-mark where))

(defn process-clause [clause]
  "Processes a clause by expanding variables and padding it out to a 5-tuple"
  (match (vec clause)
         [:and & conjuncts] `[:and ~@(map process-clause conjuncts)]
         [:or & disjuncts] `[:or ~@(map process-clause disjuncts)]
         [:not negatee] `[:not ~(process-clause negatee)]
         _ (-> clause (pad-clause) (expand-question-marks))))

(defn process-where [where]
  "Processes a where clause by wrapping the whole clause in an :and
   and making sure each clause is a 5-tuple, padding them out with _s
   if necessary"
  (conj
   (map process-clause where)
   :and))

(defn process-rules [rules]
  "Processes rules by wrapping the rule bodies in an :and and
   expanding all ?vars to [? var]."
  (map
   (fn [rule]
     (if (empty? (rest rule))
       (vector (process-clause (first rule)))
       `[~(process-clause (first rule))
         [:and ~@(map process-clause (rest rule))]]))
   rules))

(defn query [db q]
  "Runs the query `q` against `db`, returning a seq of
   frames with variables bindings."
  (let [{:keys [find where rules]} q
        processed-where (process-where where)
        processed-find (vec (expand-question-marks find))
        processed-rules (process-rules rules)]
    (->> (qeval db processed-where processed-rules [{}])
         (map
          (fn [frame]
            (vec (binding/instantiate frame processed-find (fn [v f] v))))))))

(defn query-callback [queue-backend msg]
  (->> (query (:db msg) (:query msg))
       (assoc msg :results)
       (queue/publish queue-backend :query/results)))

(defn new [queue-backend]
  (service/make-service
   queue-backend
   {:query (partial #'query-callback queue-backend)
    :query/match (partial #'match-callback queue-backend)}
   :query/queryers))
