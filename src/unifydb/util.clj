(ns unifydb.util
  (:require [manifold.deferred :as d]
            [manifold.stream :as s]
            [unifydb.messagequeue :as queue])
  (:import [java.util UUID]))

(defn not-nil-seq? [obj]
  (and (sequential? obj) (seq obj)))

(defmacro when-let*
  "Multiple binding version of when-let"
  [bindings & body]
  (if (seq bindings)
    `(when-let [~(first bindings) ~(second bindings)]
       (when-let* ~(vec (drop 2 bindings)) ~@body))
    `(do ~@body)))

(defn take-n!
  "Returns a lazy seq consisting of the results
   of calling take! n times on the stream"
  [n stream]
  (if (<= n 0)
    '()
    (lazy-seq
     (cons (deref (s/take! stream))
           (take-n! (dec n) stream)))))

(defn join
  "Takes a list of entity-attribute-value tuples
   and returns a list of maps, where each map represents
   all the facts about a particular entity."
  [facts]
  (reduce
   (fn [acc v]
     (assoc-in acc [(first v) (second v)] (nth v 2)))
   {}
   facts))

(defn query
  "Runs the query `q` by submitting it to the query service(s) running
  on the `queue-backend`. `db` is a map containing the key `tx-id`,
  designating the point in time against which to run the
  query. `bindings`, if given, parameterize the query.  Returns a
  Manifold deferred with the query results."
  ([queue-backend db q]
   (query queue-backend db q {}))
  ([queue-backend db q bindings]
   (let [results (queue/subscribe queue-backend :query/results)
         id (str (UUID/randomUUID))
         bindings (into {} (for [[k v] bindings]
                             [(symbol (name k)) v]))
         query (assoc q :bind (merge bindings (:bind q)))]
     (queue/publish queue-backend :query {:id id :db db :query query})
     (as-> results v
       (s/filter #(= (:id %) id) v)
       (s/take! v)
       (d/chain v #(do (s/close! results) %))))))

(defn map-over-symbols
  ([proc exp] (map-over-symbols proc exp (fn [_exp] false)))
  ([proc exp skip-fn]
   (cond
     (and (sequential? exp) (seq exp) (not (skip-fn exp)))
     (conj (map-over-symbols proc (rest exp) skip-fn)
           (map-over-symbols proc (first exp) skip-fn))
     (symbol? exp) (proc exp)
     :else exp)))

(defn expand-question-mark [sym]
  (let [chars (str sym)]
    (if (= "?" (subs chars 0 1))
      ['? (symbol (subs chars 1))]
      sym)))

(defn deep-merge [a & maps]
  (if (map? a)
    (apply merge-with deep-merge a maps)
    (apply merge-with deep-merge maps)))

(defn mapm
  "Maps `f` over the map `m`, returning a new map. `f` should take a
  key and value and return a key-value tuple."
  [f m]
  (into {} (map (fn [[k v]] (f k v)) m)))
