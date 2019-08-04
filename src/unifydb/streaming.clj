(ns unifydb.streaming
  (:require [manifold.stream :as s]))

(defprotocol StreamingBackend
  "The streaming backend provides asynchronous stream operations."
  (map [backend f stream]
    "Returns stream consisting of the result of applying f
     to the set of first items of each stream, followed by applying f
     to the set of second items in each stream, until any one of the
     streams is exhausted. Any remaining items in other streams are ignored.
     Function f should accept number-of-streams arguments"))

(defn mapcat [backend f stream]
  (apply s/concat (map backend f stream)))
