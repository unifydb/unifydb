(ns unifydb.cache.memory
  (:require [unifydb.cache :as cache]
            [unifydb.datetime :as dt]))

(defn cache-record [value ttl]
  {:value value
   :ttl ttl
   :timestamp (dt/utc-now)})

(defrecord InMemoryCacheBackend [state]
  cache/ICacheBackend
  (cache-set! [cache key value]
    (cache/cache-set! cache key value nil))
  (cache-set! [cache key value ttl]
    (swap! (:state cache) #(assoc % key (cache-record value ttl))))
  (cache-get [cache key]
    (when-let [record (get @(:state cache) key)]
      (when (or (nil? (:ttl record))
                (> (:ttl record)
                   (dt/between (dt/chrono-unit :seconds)
                               (:timestamp record)
                               (dt/utc-now))))
        (:value record)))))

(defn new
  "Returns a new, empty in-memory cache."
  []
  (->InMemoryCacheBackend (atom {})))
