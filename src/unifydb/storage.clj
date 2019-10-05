(ns unifydb.storage)

(defmulti transact-facts-impl! (fn [store facts] (:type store)))

(defmulti fetch-facts-impl (fn [store query tx-id frame] (:type store)))

(defmulti get-next-id-impl (fn [store] (:type store)))

(defn transact-facts! [store facts]
  "Stores `facts` in `store`."
  (transact-facts-impl! store facts))

(defn fetch-facts [store query tx-id frame]
  "Retrieves the facts persisted as of the transaction
   denoted by `tx-id` that might unify with `query` from
   the `store` given the bindings in `frame`. Does not return
   facts that have been retracted."
  (fetch-facts-impl store query tx-id frame))

(defn get-next-id [store]
  "Retrieves the next sequential id from the `store`."
  (get-next-id-impl store))
