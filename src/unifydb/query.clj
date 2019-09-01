(ns unifydb.query
  (:require [clojure.core.match :refer [match]]
            [unifydb.facts :refer [fact-entity fact-attribute fact-value fact-added?]]
            [unifydb.storage :as store]
            [unifydb.streaming :as streaming]
            [unifydb.unify :as unify]
            [unifydb.util :as util]))

(declare qeval)

(defn empty-stream []
  [])

(defn conjoin [db conjuncts frames]
  "Evaluates the conjunction (logical AND) of all `conjuncts` in the context of `frames`.
   Returns a stream of frames."
  (if (empty? conjuncts)
    frames
    (conjoin db
             (rest conjuncts)
             (qeval db (first conjuncts) frames))))

(defn disjoin [db disjuncts frames]
  "Evaluates the disjunctions (logical OR) of all `disjuncts` in the context of `frames`.
   Returns a stream of frames."
  (if (empty? disjuncts)
    (empty-stream)
    (concat (qeval db (first disjuncts) frames)
            (disjoin db (rest disjuncts) frames))))

;; This is a shitty implementation of negation because it acts only as a filter,
;; meaning it is only valid as one of the subsequent clauses in an :and query.
;; In other words, [:not [?e :name "Foo"]] always returns the empty stream, even if
;; there are entities in the database whose :name is not "Foo". To get this to work
;; right you'd need to do [:and [?e ?a ?v] [:not [?e :name "Foo"]]] - in other words,
;; generating a stream of every fact in the database and then passing it through the
;; :not filter. This is an acceptable solution for now but it should be clearly documented
;; and hopefully one day improved.
(defn negate [db operands frames]
  "Evaluates the `operands` in the context of `frames`, returning a stream of only 
   those frames for which evaluation fails (i.e. for which the logic query cannot be made true)."
  (mapcat
   (fn [frame]
     (if (empty? (qeval db (first operands) [frame]))
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
  "Filters out any facts that have been retracted and
   returns facts in the format [entity attribute value]
   suitable for being passed to unify-match."
  (let [grouped (group-by
                 (juxt fact-entity fact-attribute fact-value)
                 facts)]
    (filter
     (fn [fact]
       (let [fact-versions (get grouped fact)]
         (fact-added? (first (reverse (sort cmp-fact-versions fact-versions))))))
     (keys grouped))))

(defn match-facts [db query frame]
  "Returns a stream of frames obtained by pattern-matching the `query`
   against the facts in `db` in the context of `frame`.

   The pattern-matching is done in parallel via the streaming backend."
  (streaming/mapcat
   (:streaming-backend db)
   (fn [fact]
     (let [match-result (unify/unify-match query fact frame)]
       (if (= match-result :failed)
         (empty-stream)
         [match-result])))
   (process-facts
    (store/fetch-facts (:storage-backend db) query (:tx-id db) frame))))

(defn simple-query [db query frames]
  "Evaluates a non-compound query, returning a stream of frames."
  ;; TODO apply rules here as well as matching facts
  (mapcat
   (fn [frame] (match-facts db query frame))
   frames))

(defn qeval [db query frames]
  "Evaluates a logic query given by `query` in the context of `frames`.
   Returns a stream of frames."
  (match query
    [:and & conjuncts] (conjoin db conjuncts frames)
    [:or & disjuncts] (disjoin db disjuncts frames)
    [:not & operands] (negate db operands frames)
    ;; TODO support lisp-value?
    [:always-true & _] frames
    _ (simple-query db query frames)))

(defn query [db q]
  "Runs the query `q` against `db`, returning a stream of frames with variables bindings."
  (qeval db q [{}]))
