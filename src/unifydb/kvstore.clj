(ns unifydb.kvstore
  "Key-value store protocol. Implementations must support string keys
  and arbitrary EDN values."
  (:refer-clojure :exclude [get assoc! dissoc! contains?]))

(defprotocol IKeyValueStore
  (store-get [store key] "Retrieves the value associated with `key` in `store`.")
  (assoc! [store key val] "Associates `key` with `val` in `store`.")
  (dissoc! [store key] "Deletes `key` from `store`.")
  (contains? [store key] "Whether the `store` contains the `key`."))

(defn get
  "Retrieves the value associated with `key` in `store`."
  [store key]
  (store-get store key))
