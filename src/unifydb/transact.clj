(ns unifydb.transact
  (:require [clojure.core.match :refer [match]]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [unifydb.facts :refer [fact-entity
                                   fact-attribute
                                   fact-value
                                   fact-tx-id
                                   fact-added?]]
            [unifydb.messagequeue :as queue]
            [unifydb.service :as service]
            [unifydb.storage :as storage]
            [unifydb.transact.filters :as filters]
            [unifydb.transact.transforms :as transforms])
  (:import [java.util UUID]))

(defn make-new-tx-facts
  "Returns the list of database operations to make a new transaction entity."
  []
  ;; TODO add current user to tx facts
  [[:unifydb/add "unifydb.tx" :unifydb/txInstant (System/currentTimeMillis)]])

(defn map-form->add-forms
  "Transforms a map-form transaction statement into a list of add operations."
  [map-form]
  (let [id (or (:unifydb/id map-form)
               (str (UUID/randomUUID)))
        map-form (dissoc map-form :unifydb/id)]
    (mapcat (fn [[attr val]]
              (cond
                (map? val) (let [child-id (or (:unifydb/id val)
                                              (str (UUID/randomUUID)))
                                 child (assoc val :unifydb/id child-id)]
                             (conj (map-form->add-forms child)
                                   [:unifydb/add id attr child-id]))
                (and (sequential? val)
                     (every? map? val)) (mapcat
                                         (fn [child]
                                           (let [child-id (or (:unifydb/id child)
                                                              (str (UUID/randomUUID)))
                                                 child (assoc child :unifydb/id child-id)]
                                             (conj (map-form->add-forms child)
                                                   [:unifydb/add id attr child-id])))
                                         val)
                :else [[:unifydb/add id attr val]]))
            map-form)))

(defn expand-map-forms
  "Expands map items in `tx-data` to `:unifydb/add` statements."
  [tx-data]
  (mapcat (fn [tx-stmt]
            (if (map? tx-stmt)
              (map-form->add-forms tx-stmt)
              [tx-stmt]))
          tx-data))

(defn process-tx-data
  "Turns a list of transaction statements in the form
   [<db operation> <entity> <attribute> <value>] into
   a list of facts ready to be transacted of the form
   [<entity> <attribute> <value> \"unifydb.tx\" <added?>]"
  [tx-data]
  (map
   (fn [tx-stmt]
     (match tx-stmt
       [:unifydb/add e a v] [e a v "unifydb.tx" true]
       [:unifydb/retract e a v] [e a v "unifydb.tx" false]))
   tx-data))

(defn gen-temp-ids
  "Returns a map of temporary ids to actual database ids based on the facts"
  [storage-backend facts]
  ;; TODO transaction ids should be the current unix timestamp, not
  ;; sequential, to allow for easy temporal querying
  (reduce
   (fn [ids fact]
     (let [eid (fact-entity fact)]
       (if (string? eid)
         (let [resolved-id (get ids eid)]
           (if-not resolved-id
             (assoc ids eid (storage/get-next-id! storage-backend))
             ids))
         ids)))
   {}
   facts))

(defn resolve-temp-ids
  "Resolves temporary ids in facts to the actual database ids."
  [ids facts]
  (map
   (fn [fact]
     (let [e (or (get ids (fact-entity fact)) (fact-entity fact))
           v (or (get ids (fact-value fact)) (fact-value fact))
           tx-id (get ids (fact-tx-id fact))]
       [e (fact-attribute fact) v tx-id (fact-added? fact)]))
   facts))

(defn do-transaction
  "Does all necessary processing of `tx-data` and sends it off to the storage backend."
  [storage-backend tx-data]
  (let [with-tx (into tx-data (make-new-tx-facts))
        expanded (expand-map-forms with-tx)
        transformed (transforms/apply-transforms expanded)
        raw-facts (process-tx-data transformed)
        ids (gen-temp-ids storage-backend raw-facts)
        facts (resolve-temp-ids ids raw-facts)
        tx-id (get ids "unifydb.tx")
        ;; TODO get rest of conn info in :db-after
        tx-report (filters/apply-filters
                   {:db-after (assoc {} :tx-id tx-id)
                    :tx-data (vec facts)
                    :tempids ids})]
    (storage/store-facts! storage-backend facts)
    tx-report))

(defn transact-loop [queue-backend storage-backend state]
  (when-let [message (and
                      (not (s/drained? (:subscription @state)))
                      (deref (s/take! (:subscription @state))))]
    (let [{:keys [tx-data]} message
          tx-report (do-transaction storage-backend tx-data)]
      (queue/publish queue-backend
                     :transact/results
                     (assoc message :tx-report tx-report))
      (recur queue-backend storage-backend state))))

;; TODO add check to make sure there is only ever one transact service running

(defrecord TransactService [queue-backend storage-backend state]
  service/IService
  (start! [self]
    (let [subscription (queue/subscribe queue-backend :transact)
          transact-thread (Thread. #(transact-loop queue-backend
                                                   storage-backend
                                                   (:state self)))]
      (swap! (:state self) #(assoc % :subscription subscription))
      (.start transact-thread)))
  (stop! [self]
    (s/close! (:subscription @(:state self)))))

(defn new
  "Returns a new transact component instance."
  [queue-backend storage-backend]
  (->TransactService queue-backend storage-backend (atom {})))

(defn transact
  "Transacts `tx-data` into the DB via `queue-backend`.
   Returns a Manifold deferred containing the tx-report."
  [queue-backend tx-data]
  (let [id (str (UUID/randomUUID))
        results (queue/subscribe queue-backend :transact/results)]
    (queue/publish queue-backend :transact {:id id :tx-data tx-data})
    (as-> results v
      (s/filter #(= (:id %) id) v)
      (s/take! v)
      (d/chain v
               #(assoc {} :tx-report (:tx-report %))
               #(do (s/close! results) %)))))
