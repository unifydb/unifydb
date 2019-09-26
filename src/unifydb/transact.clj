(ns unifydb.transact
  (:require [clojure.core.match :refer [match]]
            [clojure.tools.logging :as log]
            [unifydb.facts :refer [fact-entity
                                   fact-attribute
                                   fact-value
                                   fact-tx-id
                                   fact-added?]]
            [unifydb.messagequeue :as queue]
            [unifydb.service :as service]
            [unifydb.storage :as storage]
            [unifydb.util :as util]))

(defn make-new-tx-facts []
  "Returns the list of database operations to make a new transaction entity."
  [[:unifydb/add "unifydb.tx" :unifydb/txInstant (System/currentTimeMillis)]])

(defn process-tx-data [tx-data]
  "Turns a list of transaction statements in the form
   [<db operation> <entity> <attribute> <value>] into
   a list of facts ready to be transacted of the form
   [<entity> <attribute> <value> \"unifydb.tx\" <added?>]"
  (map
   (fn [tx-stmt]
     (match tx-stmt
            [:unifydb/add e a v] [e a v "unifydb.tx" true]
            [:unifydb/retract e a v] [e a v "unifydb.tx" false]))
   tx-data))

(defn gen-temp-ids [storage-backend facts]
  "Returns a map of temporray ids to actual database ids based on the facts"
  (reduce
   (fn [ids fact]
     (let [eid (fact-entity fact)]
       (if (string? eid)
         (let [resolved-id (get ids eid)]
           (if-not resolved-id
             (assoc ids eid (storage/get-next-id storage-backend))
             ids))
         ids)))
   {}
   facts))

(defn resolve-temp-ids [ids facts]
  "Resolves temporary ids in facts to the actual database ids."
  (map
   (fn [fact]
     (let [e (or (get ids (fact-entity fact)) (fact-entity fact))
           v (or (get ids (fact-value fact)) (fact-value fact))
           tx-id (get ids (fact-tx-id fact))]
       [e (fact-attribute fact) v tx-id (fact-added? fact)]))
   facts))

(defn do-transaction [_ conn tx-data result-promise]
  "Does all necessary processing of `tx-data` and sends it off to the storage backend."
  (let [with-tx (into tx-data (make-new-tx-facts))
        raw-facts (process-tx-data with-tx)
        ids (gen-temp-ids (:storage-backend conn) raw-facts)
        facts (resolve-temp-ids ids raw-facts)
        tx-id (get ids "unifydb.tx")
        tx-report {:db-after (assoc conn :tx-id tx-id)
                   :tx-data facts
                   :tempids ids}]
    (storage/transact-facts! (:storage-backend conn) facts)
    (deliver result-promise tx-report)
    nil))

(def tx-agent
  "The agent responsible for processing transactions serially."
  (let [a (agent nil)]
    (set-error-mode! a :continue)
    (set-error-handler!
     a
     (fn [a err]
       (log/error err "Transact agent error occurred")))
    a))

(defn transact [conn tx-data]
  "Transacts `tx-data` into the database represented by `conn`."
  (let [result (promise)]
    (send-off tx-agent do-transaction conn tx-data result)
    result))

(defn new [queue-backend]
  "Returns a new transact component instance."
  (service/make-service
   queue-backend
   {:transact
    (fn [message]
      (let [tx-report @(transact (:conn message) (:tx-data message))]
        (queue/publish queue-backend
                       :transact-results
                       (assoc message :tx-report tx-report))))}))
