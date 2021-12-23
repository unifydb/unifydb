(ns unifydb.kvstore.backend)

(defprotocol IKeyValueStoreBackend
  (get-all [store keys]
    "Returns a deferred containing a seq of the values that correspond
    with `keys` in the `store`.")
  (write-all! [store operations]
    "Performs multiple operations on `store` atomically, where each
    operation is either [:assoc! <key> <value>] or [:dissoc!
    <key>]. Returns a deferred that is `true` if the operations
    succeeded, or contains an error if something went wrong.")
  (contains-all? [store keys]
    "Returns a deferred whose value is whether the `store` contains
    the `keys`."))
