(ns unifydb.streaming.threadpool
  (:require [unifydb.streaming :as streaming]))

(defrecord ThreadpoolStreamingBackend []
  streaming/IStreamingBackend
  (map [backend f stream]
    (pmap f stream)))

(defn new []
  (->ThreadpoolStreamingBackend))
