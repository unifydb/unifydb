(ns unifydb.transact.filters
  "Apply filters to the tx-report coming back from a transaction."
  (:require [clojure.core.match :refer [match]]))

(defn fact->map
  [fact]
  (match fact
    [e a v tx-id added?] {:entity e
                          :attribute a
                          :value v
                          :tx-id tx-id
                          :added? added?}))

(defn filter-auth-facts
  "Filters out sensitive auth data from the tx-report"
  [tx-report]
  (let [auth-attrs #{:unifydb/salt
                     :unifydb/i
                     :unifydb/server-key
                     :unifydb/stored-key}
        filtered-facts (->> tx-report
                            (:tx-data)
                            (remove #(auth-attrs (:attribute (fact->map %))))
                            (vec))]
    (assoc tx-report :tx-data filtered-facts)))

(defn apply-filters [tx-report]
  (-> tx-report
      (filter-auth-facts)))
