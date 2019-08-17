(ns unifydb.storage.memory
  (:require [clojure.core.match :refer [match]]
            [manifold.stream :as stream]
            [me.tonsky.persistent-sorted-set :as set]
            [unifydb.binding :as binding :refer [var?]]
            [unifydb.storage :as storage]))

;; A fact is a map with the keys
;; [:entity :attribute :value :tx-id :added?]

(defn fact-entity [fact]
  "Returns the entity component of an in-memory Fact"
  (:entity fact))

(defn fact-attribute [fact]
  "Returns the attribute component of an in-memory Fact"
  (:attribute fact))

(defn fact-value [fact]
  "Returns the value component of an in-memory Fact"
  (:value fact))

(defn fact-tx-id [fact]
  "Returns the transaction-id component of an in-memory Fact"
  (:tx-id fact))

(defn fact-added? [fact]
  "Returns the added? component of an in-memory Fact"
  (:added? fact))

(defn vec->fact [vfact]
  (let [attr-to-idxs {:entity 0
                      :attribute 1
                      :value 2
                      :tx-id 3
                      :added? 4}]
    (into {}
          (map (fn [[attr idx]] [attr (nth vfact idx)])
               attr-to-idxs))))

(defn fact->vec [mfact]
  [(fact-entity mfact)
   (fact-attribute mfact)
   (fact-value mfact)
   (fact-tx-id mfact)
   (fact-added? mfact)])

(defn compare-fact-by [fact1 fact2 & fns]
  (compare (vec (map #(% fact1) fns))
           (vec (map #(% fact2) fns))))

(defn cmp-fact-eavt [fact1 fact2]
  (compare-fact-by fact1 fact2
                   fact-entity
                   fact-attribute
                   fact-value
                   fact-tx-id
                   fact-added?))

(defn cmp-fact-aevt [fact1 fact2]
  (compare-fact-by fact1 fact2
                   fact-attribute
                   fact-entity
                   fact-value
                   fact-tx-id
                   fact-added?))

(defn cmp-fact-avet [fact1 fact2]
  (compare-fact-by fact1 fact2
                   fact-attribute
                   fact-value
                   fact-entity
                   fact-tx-id
                   fact-added?))

(defn filter-by-tx [facts tx-id]
  "Filters out all `facts` with tx-id less than or equal to `tx-id`."
  (filter #(<= (fact-tx-id %1) tx-id) facts))

(defn fetch-facts-eavt [eavt query tx-id]
  (let [[entity attribute value] query]
    (match [entity attribute value]
           ;; e a v
           [(false :<< var?) (false :<< var?) (false :<< var?)]
           (set/slice eavt
                      {:entity entity :attribute attribute :value value :tx-id 0}
                      {:entity entity :attribute attribute :value value :tx-id tx-id})
           ;; e a ?
           [(false :<< var?) (false :<< var?) (true :<< var?)]
           (filter-by-tx (set/slice eavt
                                    {:entity entity :attribute attribute}
                                    {:entity entity :attribute attribute})
                         tx-id)
           ;; e ? ?
           [(false :<< var?) (true :<< var?) (true :<< var?)]
           (filter-by-tx (set/slice eavt {:entity entity} {:entity entity}) tx-id)
           ;; ? ? ?
           [_ _ _]
           (filter-by-tx eavt tx-id))))

(defn fetch-facts-aevt [aevt query tx-id]
  (let [[entity attribute value] query]
    (match [entity attribute value]
           ;; a e v
           [(false :<< var?) (false :<< var?) (false :<< var?)]
           (set/slice aevt
                      {:attribute attribute :entity entity :value value :tx-id 0}
                      {:attribute attribute :entity entity :value value :tx-id tx-id})
           ;; a e ?
           [(false :<< var?) (false :<< var?) (true :<< var?)]
           (filter-by-tx (set/slice aevt
                                    {:attribute attribute :entity entity}
                                    {:attribute attribute :entity entity})
                         tx-id)
           ;; a ? ?
           [(false :<< var?) (true :<< var?) (true :<< var?)]
           (filter-by-tx (set/slice aevt {:attribute attribute} {:attribute attribute}) tx-id)
           ;; ? ? ?
           [_ _ _]
           (filter-by-tx aevt tx-id))))

(defn fetch-facts-avet [avet query tx-id]
  (let [[entity attribute value] query]
    (match [entity attribute value]
           ;; a v e
           [(false :<< var?) (false :<< var?) (false :<< var?)]
           (set/slice avet
                      {:attribute attribute :value value :entity entity :tx-id 0}
                      {:attribute attribute :value value :entity entity :tx-id tx-id})
           ;; a v ?
           [(false :<< var?) (false :<< var?) (true :<< var?)]
           (filter-by-tx (set/slice avet
                                    {:attribute attribute :value value}
                                    {:attribute attribute :value value})
                         tx-id)
           ;; a ? ?
           [(false :<< var?) (true :<< var?) (true :<< var?)]
           (filter-by-tx (set/slice avet {:attribute attribute} {:attribute attribute}) tx-id)
           ;; ? ? ?
           [_ _ _]
           (filter-by-tx avet tx-id))))

(defn fetch-facts-from-index [db query tx-id]
  (let [eavt @(:eavt db)
        aevt @(:aevt db)
        avet @(:avet db)
        [entity attribute] query]
    (match [entity attribute]
           ;; e a
           [(false :<< var?) (false :<< var?)]
           (fetch-facts-aevt aevt query tx-id)
           ;; e ?
           [(false :<< var?) (true :<< var?)]
           (fetch-facts-eavt eavt query tx-id)
           ;; ? a
           [(true :<< var?) (false :<< var?)]
           (fetch-facts-avet avet query tx-id)
           ;; ? ?
           [_ _]
           (fetch-facts-eavt eavt query tx-id))))

(defrecord InMemoryStorageBackend [eavt aevt avet id-counter]
  storage/IStorageBackend
  (transact-facts! [self facts]
    (let [mfacts (map vec->fact facts)]
      (doseq [idx [(:eavt self) (:aevt self) (:avet self)]]
        (swap! idx #(into %1 mfacts))))
    self)
  (transact-rules! [self rules])
  (fetch-facts [self query tx-id frame]
    (let [instantiated (binding/instantiate query frame (fn [v f] v))]
      (stream/->source
       (map fact->vec
            (fetch-facts-from-index self query tx-id)))))
  (fetch-rules [self query tx-id frame])
  (get-next-id [self] (swap! (:id-counter self) inc)))

(defn new []
  (->InMemoryStorageBackend
   (atom (set/sorted-set-by cmp-fact-eavt))
   (atom (set/sorted-set-by cmp-fact-aevt))
   (atom (set/sorted-set-by cmp-fact-avet))
   (atom 0)))
