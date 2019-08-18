(ns unifydb.util
  (:require [manifold.stream :as s]))

(defn not-nil-seq? [obj]
  (and (sequential? obj) (not (empty? obj))))

(defn stream-mapcat [f stream]
  "Why doesn't manifold's `mapcat` work like this? No one knows..."
  (s/concat (s/map f stream)))
