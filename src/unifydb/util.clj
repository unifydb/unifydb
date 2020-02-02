(ns unifydb.util
  (:require [manifold.deferred :as d]
            [manifold.stream :as s]
            [unifydb.messagequeue :as queue])
  (:import [java.util UUID]))

(defn not-nil-seq? [obj]
  (and (sequential? obj) (not (empty? obj))))

(defmacro when-let*
  "Multiple binding version of when-let"
  [bindings & body]
  (if (seq bindings)
    `(when-let [~(first bindings) ~(second bindings)]
       (when-let* ~(vec (drop 2 bindings)) ~@body))
    `(do ~@body)))

(defn take-n! [n stream]
  "Returns a lazy seq consisting of the results
   of calling take! n times on the stream"
  (if (<= n 0)
    '()
    (lazy-seq
     (cons (deref (s/take! stream))
           (take-n! (dec n) stream)))))

(defn join [facts]
  "Takes a list of entity-attribute-value tuples
   and returns a list of maps, where each map represents
   all the facts about a particular entity."
  (reduce
   (fn [acc v]
     (assoc-in acc [(first v) (second v)] (nth v 2)))
   {}
   facts))

(defn query [queue-backend db q]
  "Runs the query `q` by submitting it to the query service(s)
   running on the `queue-backend`. `db` is a map containing
   the key `tx-id`, designating the point in time against which
   to run the query. Returns a Manifold deferred with the query results."
  (let [results (queue/subscribe queue-backend :query/results)
        id (str (UUID/randomUUID))]
    (queue/publish queue-backend :query {:id id :db db :query q})
    (as-> results v
      (s/filter #(= (:id %) id) v)
      (s/take! v)
      (d/chain v
               #(assoc {} :results (:results %))
               #(do (s/close! results) %)))))
