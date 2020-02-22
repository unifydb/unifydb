(ns unifydb.storage)

(defprotocol IStorageBackend
  (transact-facts! [this facts]
    "Stores `facts` in `store`.")
  (fetch-facts [this query tx-id frame]
    "Retrieves the facts persisted as of the transaction
     denoted by `tx-id` that might unify with `query` from
     the `store` given the bindings in `frame`. Does not return
     facts that have been retracted.")
  (get-next-id [this]
    "Retrieves the next sequential id from the `store`."))
