(ns unifydb.cache
  {:clj-kondo/config
   '{:linters
     {:redefined-var {:level :off}}}})

(defprotocol ICacheBackend
  (cache-set! [cache key value] [cache key value ttl])
  (cache-get [cache key]))
