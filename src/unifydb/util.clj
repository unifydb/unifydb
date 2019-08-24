(ns unifydb.util)

(defn not-nil-seq? [obj]
  (and (sequential? obj) (not (empty? obj))))
