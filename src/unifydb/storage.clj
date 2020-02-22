(ns unifydb.storage
  (:require [taoensso.timbre :as log]))

(defmulti transact-facts-impl! (fn [store facts] (:type store)))

(defmulti fetch-facts-impl (fn [store query tx-id frame] (:type store)))

(defmulti get-next-id-impl :type)

(defn transact-facts!
  "Stores `facts` in `store`."
  [store facts]
  (transact-facts-impl! store facts))

(defn fetch-facts
  "Retrieves the facts persisted as of the transaction
   denoted by `tx-id` that might unify with `query` from
   the `store` given the bindings in `frame`. Does not return
   facts that have been retracted."
  [store query tx-id frame]
  (log/debug "Fetching facts" :store store :query query)
  (fetch-facts-impl store query tx-id frame))

(defn get-next-id
  "Retrieves the next sequential id from the `store`."
  [store]
  (get-next-id-impl store))
