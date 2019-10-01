(ns unifydb.util
  (:require [manifold.stream :as s]))

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
