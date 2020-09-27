(ns unifydb.cache)

(defprotocol ICacheBackend
  (cache-set! [cache key value] [cache key value ttl])
  (cache-get [cache key]))
