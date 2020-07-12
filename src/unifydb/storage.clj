(ns unifydb.storage
  (:require [unifydb.binding :as bind]))

(defprotocol IStorageBackend
  (transact-facts! [this facts]
    "Stores `facts` in `store`.")
  (fetch-facts-internal [this query tx-id]
    "Retrieves the facts persisted as of the transaction
     denoted by `tx-id` that might unify with `query` from
     the `store` given the bindings in `frame`. Does not return
     facts that have been retracted.")
  (get-next-id [this]
    "Retrieves the next sequential id from the `store`."))

(defn fetch-facts [store query tx-id frame]
  (let [tx-id (if (= tx-id :latest)
                Integer/MAX_VALUE
                tx-id)
        query (bind/instantiate frame query (fn [v _f] v))]
    (fetch-facts-internal store query tx-id)))
