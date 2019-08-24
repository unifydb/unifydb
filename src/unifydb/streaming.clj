(ns unifydb.streaming)

(defprotocol IStreamingBackend
  "The streaming backend provides parallel stream operations."
  (map [backend f stream]
    "Returns a seq consisting of the result of applying f
     to each item in `stream`."))

(defn mapcat [backend f stream]
  (apply concat (map backend f stream)))
