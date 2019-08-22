(ns unifydb.storage.memory
  (:require [clojure.core.match :refer [match]]
            [manifold.stream :as stream]
            [me.tonsky.persistent-sorted-set :as set]
            [unifydb.binding :as binding :refer [var?]]
            [unifydb.util :as util]
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
  (map
   #(when-not (has-var? %1) %1)
   [(fact-entity mfact)
    (fact-attribute mfact)
    (fact-value mfact)
    (fact-tx-id mfact)
    (fact-added? mfact)]))

(defn cmp-fact-vec [fact1 fact2]
  "Like `compare`, but nil always has 0 (equal) priority instead of the least"
  (cond
    (nil? fact1) 0
    (nil? fact2) 0
    (and (coll? fact1) (coll? fact2)) (if (= (count fact1) (count fact2))
                                        (if (empty? fact1)
                                          0
                                          (let [cmp (cmp-fact-vec (first fact1)
                                                                  (first fact2))]
                                            (if (= cmp 0)
                                              (cmp-fact-vec (rest fact1) (rest fact2))
                                              cmp)))
                                        (- (count fact1) (count fact2)))
    (not (= (type fact1) (type fact2))) (- (hash fact1) (hash fact2))
    :else (compare fact1 fact2)))

(defn compare-fact-by [fact1 fact2 & fns]
  (cmp-fact-vec (vec (map #(% fact1) fns))
                (vec (map #(% fact2) fns))))

(defn cmp-fact-eavt [fact1 fact2]
  (compare-fact-by fact1 fact2
                   fact-entity
                   fact-attribute
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

(defn has-var? [exp]
  "Returns true if the expression variables"
  (letfn [(tree-walk [node]
            (cond
              (var? node) true
              (util/not-nil-seq? node) (or (tree-walk (first node))
                                           (tree-walk (rest node)))
              :else false))]
    (tree-walk exp)))

(defn fetch-facts-eavt [eavt query tx-id]
  (let [[entity attribute value] query]
    (match [entity attribute value]
           ;; e a v
           [(false :<< has-var?) (false :<< has-var?) (false :<< has-var?)]
           (set/slice eavt
                      {:entity entity :attribute attribute :value value :tx-id 0}
                      {:entity entity :attribute attribute :value value :tx-id tx-id})
           ;; e a ?
           [(false :<< has-var?) (false :<< has-var?) (true :<< has-var?)]
           (filter-by-tx (set/slice eavt
                                    {:entity entity :attribute attribute}
                                    {:entity entity :attribute attribute})
                         tx-id)
           ;; e ? ?
           [(false :<< has-var?) (true :<< has-var?) (true :<< has-var?)]
           (filter-by-tx (set/slice eavt
                                    {:entity entity}
                                    {:entity entity})
                         tx-id)
           ;; ? ? ?
           [_ _ _]
           (filter-by-tx eavt tx-id))))

(defn fetch-facts-avet [avet query tx-id]
  (let [[entity attribute value] query]
    (match [entity attribute value]
           ;; a v e
           [(false :<< has-var?) (false :<< has-var?) (false :<< has-var?)]
           (set/slice avet
                      {:attribute attribute :value value :entity entity :tx-id 0}
                      {:attribute attribute :value value :entity entity :tx-id tx-id})
           ;; a v ?
           [(true :<< has-var?) (false :<< has-var?) (false :<< has-var?)]
           (filter-by-tx (set/slice avet
                                    {:attribute attribute :value value}
                                    {:attribute attribute :value value})
                         tx-id)
           ;; a ? ?
           [(true :<< has-var?) (false :<< has-var?) (true :<< has-var?)]
           (filter-by-tx (set/slice avet
                                    {:attribute attribute}
                                    {:attribute attribute})
                         tx-id)
           ;; ? ? ?
           [_ _ _]
           (filter-by-tx avet tx-id))))

(defn fetch-facts-from-index [db query tx-id]
  (let [eavt @(:eavt db)
        avet @(:avet db)
        [entity attribute] query]
    (match [entity attribute]
           ;; (? a v), (? a ?)
           [(true :<< has-var?) (false :<< has-var?)]
           (fetch-facts-avet avet query tx-id)
           ;; (e a v), (e a ?), (e ? v), (e ? ?), (? ? v), (? ? ?)
           [_ _]
           (fetch-facts-eavt eavt query tx-id))))

(defrecord InMemoryStorageBackend [eavt avet id-counter]
  storage/IStorageBackend
  (transact-facts! [self facts]
    (let [mfacts (map vec->fact facts)]
      (doseq [idx [(:eavt self) (:avet self)]]
        (swap! idx #(into %1 mfacts))))
    self)
  (transact-rules! [self rules])
  (fetch-facts [self query tx-id frame]
    (let [instantiated (binding/instantiate frame query (fn [v f] v))]
      (stream/->source
       (map fact->vec
            (fetch-facts-from-index self instantiated tx-id)))))
  (fetch-rules [self query tx-id frame])
  (get-next-id [self] (swap! (:id-counter self) inc)))

(defn new []
  (->InMemoryStorageBackend
   (atom (set/sorted-set-by cmp-fact-eavt))
   (atom (set/sorted-set-by cmp-fact-avet))
   (atom 0)))
