(ns unifydb.transact.transforms
  "Apply transformations to incoming transactions.
  Right now these are hardcoded but eventually we can allow them to be
  user-created"
  (:require [clojure.core.match :refer [match]]
            [unifydb.user :as user]))

(defn tx-stmt->map
  "Returns the components of `tx-stmt` as a map"
  [tx-stmt]
  (match tx-stmt
    [op e a v] {:operation op
                :entity e
                :attribute a
                :value v}))

(defn select-tx-stmt
  "Returns the map form of the tx-stmt whose attribute matches `attr`"
  [attr tx-stmts]
  (->> tx-stmts
       (map tx-stmt->map)
       (filter #(= (:attribute %) attr))
       (first)))

(defn new-user-transform [tx-stmts]
  (let [{username :value
         username-eid :entity} (select-tx-stmt :unifydb/username tx-stmts)
        {password :value
         password-eid :entity} (select-tx-stmt :unifydb/password tx-stmts)]
    (if (and username password (= username-eid password-eid))
      (let [user-data (user/make-user username password)
            user-stmts (map (fn [[attr val]]
                              [:unifydb/add username-eid attr val])
                            user-data)]
        (->> tx-stmts
             (remove #(= :unifydb/username (:attribute (tx-stmt->map %))))
             (remove #(= :unifydb/password (:attribute (tx-stmt->map %))))
             (into (vec user-stmts))))
      tx-stmts)))

(defn apply-transforms
  "Applies transformations to `tx-stmts`.
  At this point, `tx-stmts` includes the transaction facts but has not
  yet had its ids resolved or been transformed into raw facts"
  [tx-stmts]
  (-> tx-stmts
      (new-user-transform)))
