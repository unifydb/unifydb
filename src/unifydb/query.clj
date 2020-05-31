(ns unifydb.query
  (:refer-clojure :exclude [var?])
  (:require [clojure.core.match :refer [match]]
            [manifold.stream :as s]
            [taoensso.timbre :as log]
            [unifydb.binding :as binding :refer [var? var-name]]
            [unifydb.facts :refer [fact-entity fact-attribute fact-added?]]
            [unifydb.messagequeue :as queue]
            [unifydb.rules :refer [rule-body rule-conclusion]]
            [unifydb.schema :as schema]
            [unifydb.service :as service]
            [unifydb.storage :as store]
            [unifydb.unify :as unify]
            [unifydb.util :as util]))

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

;; This is a shitty implementation of negation because it acts only as a filter,
;; meaning it is only valid as one of the subsequent clauses in an :and query.
;; In other words, [:not [?e :name "Foo"]] always returns the empty stream, even if
;; there are entities in the database whose :name is not "Foo". To get this to work
;; right you'd need to do [:and [?e ?a ?v] [:not [?e :name "Foo"]]] - in other words,
;; generating a stream of every fact in the database and then passing it through the
;; :not filter. This is an acceptable solution for now but it should be clearly
;; documented and hopefully one day improved.
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

(defn cmp-fact-versions
  "Like compare, but gives false a higher priority than true"
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
          schemas (util/join
                   (do-query db (schema/make-schema-query attrs)))
          cardinalities
          (reduce (fn [acc attr]
                    (if-let [cardinality
                             (get-in schemas [attr :unifydb/cardinality])]
                      (assoc acc attr cardinality)
                      acc))
                  {}
                  (keys schemas))]
      (mapcat
       (fn [attr-group]
         (let [filtered-facts (filter-sorted-facts
                               []
                               (reverse
                                (sort cmp-fact-versions attr-group)))
               cardinality (get cardinalities
                                (fact-attribute (first attr-group)))]
           (if (= cardinality :cardinality/many)
             filtered-facts
             (take 1 filtered-facts))))
       (vals grouped)))))

(defn match-facts
  "Returns a seq of frames obtained by pattern-matching the `query`
   against the facts in `db` in the context of `frame`."
  [db query frame]
  (let [facts (log/spy
               :debug :processed-facts
               (process-facts
                db
                (log/spy :debug :facts
                         (store/fetch-facts (:storage-backend db)
                                            query
                                            (:tx-id db)
                                            frame))))]
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
          ;; TODO support lisp-value?
             [:always-true] frames
             _ (simple-query db query rules frames))))

;; A query is a map with the following structure:
;;    {:find [?user ?tweet]
;;     :where [[:likes ?user ?tweet]
;;             (presidential ?tweet)]
;;     :rules [[(presidential ?tweet)
;;              [:author ?tweet ?author]
;;              [:job ?author "president"]
;;              (:not [:hand-size ?author "small"])]]}

(defn pad-clause
  "Ensures the clause is a 5-tuple by padding it out with _s if necessary"
  [clause]
  (take 5 (concat clause (repeat '_))))

(defn map-over-symbols [proc exp]
  (cond
    (and (sequential? exp) (seq exp))
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

(defn process-clause
  "Processes a clause by expanding variables and padding it out to a 5-tuple"
  [clause]
  (match (vec clause)
    [:and & conjuncts] `[:and ~@(map process-clause conjuncts)]
    [:or & disjuncts] `[:or ~@(map process-clause disjuncts)]
    [:not negatee] `[:not ~(process-clause negatee)]
    _ (-> clause (pad-clause) (expand-question-marks))))

(defn process-where
  "Processes a where clause by wrapping the whole clause in an :and
   and making sure each clause is a 5-tuple, padding them out with _s
   if necessary"
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

(defn do-query
  "Runs the query `q` against `db`, returning a seq of
   frames with variables bindings."
  [db q]
  (let [{:keys [find where rules]} q
        processed-where (process-where where)
        processed-find (vec (expand-question-marks find))
        processed-rules (process-rules rules)]
    (map
     (fn [frame]
       (vec (binding/instantiate frame processed-find (fn [v _f] v))))
     (qeval db processed-where processed-rules [{}]))))

(defn query-callback [queue-backend storage-backend msg]
  (let [db (-> (:db msg)
               (assoc :queue-backend queue-backend)
               (assoc :storage-backend storage-backend))]
    (log/debug "Received query message" :message msg)
    (->> (do-query db (:query msg))
         (assoc msg :results)
         (queue/publish queue-backend :query/results))))

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
