(ns unifydb.streaming
  (:require [manifold.stream :as s]))

(defprotocol IStreamingBackend
  "The streaming backend provides asynchronous stream operations."
  (map [backend f stream]
    "Returns stream consisting of the result of applying f
     to each item in `stream`."))

(defn mapcat [backend f stream]
  (s/concat (map backend f stream)))















