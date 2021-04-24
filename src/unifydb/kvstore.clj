(ns unifydb.kvstore
  "Key-value store protocol. Implementations must support string keys
  and arbitrary EDN values."
  (:refer-clojure :exclude [get assoc! dissoc! contains?]))

(defprotocol IKeyValueStore
  (get-batch [store keys] "Retrieves the value associated with `key` in `store`.")
  (write-batch! [store operations]
    "Performs multiple operations on `store` atomically, where each
    operation is either [:assoc! <key> <value>] or [:dissoc! <key>]")
  (contains-batch? [store keys] "Whether the `store` contains the `keys`."))

(defn assoc! [store key value]
  (write-batch! store [[:assoc! key value]]))

(defn dissoc! [store key]
  (write-batch! store [[:dissoc! key]]))

(defn get
  "Retrieves the value associated with `key` in `store`."
  [store key]
  (first (get-batch store [key])))

(defn contains? [store key]
  (contains-batch? store [key]))
