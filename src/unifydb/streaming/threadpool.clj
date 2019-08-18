(ns unifydb.streaming.threadpool
  (:require [clojure.core :as core]
            [manifold.stream :as s]
            [unifydb.streaming :as streaming]))

(defrecord ThreadpoolStreamingBackend []
  streaming/IStreamingBackend
  (map [backend f stream]
    (s/->source (pmap f (s/stream->seq stream)))))

(defn new []
  (->ThreadpoolStreamingBackend))
