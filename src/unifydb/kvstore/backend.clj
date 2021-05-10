(ns unifydb.kvstore.backend)

(defprotocol IKeyValueStoreBackend
  (get-all [store keys] "Retrieves the value associated with `key` in `store`.")
  (write-all! [store operations]
    "Performs multiple operations on `store` atomically, where each
    operation is either [:assoc! <key> <value>] or [:dissoc! <key>]")
  (contains-all? [store keys] "Whether the `store` contains the `keys`."))
