(ns unifydb.streaming.threadpool
  (:require [clojure.core :as core]
            [manifold.stream :as s]
            [unifydb.streaming :refer [StreamingBackend]]))

(defrecord ThreadpoolStreamingBackend []
  StreamingBackend
  (map [backend f stream]
    (pmap f (s/stream->seq stream))))

(defn new []
  (->ThreadpoolStreamingBackend))
