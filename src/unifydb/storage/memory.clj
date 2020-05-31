(ns unifydb.storage.memory
  (:refer-clojure :exclude [var?])
  (:require [clojure.core.match :refer [match]]
            [me.tonsky.persistent-sorted-set :as set]
            [unifydb.binding :as binding :refer [var?]]
            [unifydb.facts :refer [fact-entity
                                   fact-attribute
                                   fact-value
                                   fact-tx-id
                                   fact-added?]]
            [unifydb.util :as util]
            [unifydb.storage :as storage]))

(defn var-or-blank? [exp]
  (or (var? exp) (= exp '_)))

(defn has-var-or-blank?
  "Returns true if the expression contains variables or blank signifiers"
  [exp]
  (letfn [(tree-walk [node]
            (cond
              (var-or-blank? node) true
              (util/not-nil-seq? node) (or (tree-walk (first node))
                                           (tree-walk (rest node)))
              :else false))]
    (tree-walk exp)))

(defn cmp-fact-vec
  "Like `compare`, but nil always has 0 (equal) priority instead of the least"
  [fact1 fact2]
  (cond
    (nil? fact1) 0
    (nil? fact2) 0
    (and (coll? fact1) (coll? fact2)) (if (= (count fact1) (count fact2))
                                        (if (empty? fact1)
                                          0
                                          (let [cmp (cmp-fact-vec (first fact1)
                                                                  (first fact2))]
                                            (if (zero? cmp)
                                              (cmp-fact-vec (rest fact1) (rest fact2))
                                              cmp)))
                                        (- (count fact1) (count fact2)))
    (not= (type fact1) (type fact2)) (- (hash fact1) (hash fact2))
    :else (compare fact1 fact2)))

(defn filter-by-tx
  "Filters out all `facts` with tx-id less than or equal to `tx-id`."
  [facts tx-id]
  (filter #(<= (fact-tx-id %1) tx-id) facts))

(defn fetch-facts-eavt [eavt query tx-id]
  (let [[entity attribute value] query]
    (match [entity attribute value]
      ;; e a v
      [(false :<< has-var-or-blank?) (false :<< has-var-or-blank?) (false :<< has-var-or-blank?)]
      (set/slice eavt
                 [entity attribute value 0 nil]
                 [entity attribute value tx-id nil])
      ;; e a ?
      [(false :<< has-var-or-blank?) (false :<< has-var-or-blank?) (true :<< has-var-or-blank?)]
      (filter-by-tx (set/slice eavt
                               [entity attribute nil nil nil]
                               [entity attribute nil nil nil])
                    tx-id)
      ;; e ? ?
      [(false :<< has-var-or-blank?) (true :<< has-var-or-blank?) (true :<< has-var-or-blank?)]
      (filter-by-tx (set/slice eavt
                               [entity nil nil nil nil]
                               [entity nil nil nil nil])
                    tx-id)
      ;; ? ? ?
      [_ _ _]
      (filter-by-tx eavt tx-id))))

(defn avet->eavt [avet-fact]
  (let [[a v e t a?] avet-fact]
    [e a v t a?]))

(defn fetch-facts-avet [avet query tx-id]
  (let [[entity attribute value] query
        avet-facts
        (match [entity attribute value]
          ;; a v e
          [(false :<< has-var-or-blank?) (false :<< has-var-or-blank?) (false :<< has-var-or-blank?)]
          (set/slice avet
                     [attribute value entity 0 nil]
                     [attribute value entity tx-id nil])
          ;; a v ?
          [(true :<< has-var-or-blank?) (false :<< has-var-or-blank?) (false :<< has-var-or-blank?)]
          (filter-by-tx (set/slice avet
                                   [attribute value nil nil nil]
                                   [attribute value nil nil nil])
                        tx-id)
          ;; a ? ?
          [(true :<< has-var-or-blank?) (false :<< has-var-or-blank?) (true :<< has-var-or-blank?)]
          (filter-by-tx (set/slice avet
                                   [attribute nil nil nil nil]
                                   [attribute nil nil nil nil])
                        tx-id)
          ;; ? ? ?
          [_ _ _]
          (filter-by-tx avet tx-id))]
    (map avet->eavt avet-facts)))

(defn fetch-facts-from-index [db query tx-id]
  (let [eavt (:eavt db)
        avet (:avet db)
        [entity attribute] query]
    (match [entity attribute]
      ;; (? a v), (? a ?)
      [(true :<< has-var-or-blank?) (false :<< has-var-or-blank?)]
      (fetch-facts-avet avet query tx-id)
      ;; (e a v), (e a ?), (e ? v), (e ? ?), (? ? v), (? ? ?)
      [_ _]
      (fetch-facts-eavt eavt query tx-id))))

(defrecord InMemoryStorageBackend [state]
  storage/IStorageBackend
  (transact-facts! [this facts]
    (let [facts-eavt facts
          facts-avet (map #(vector (fact-attribute %)
                                   (fact-value %)
                                   (fact-entity %)
                                   (fact-tx-id %)
                                   (fact-added? %))
                          facts)]
      (swap! (:state this)
             (fn [s]
               (update s :eavt
                       #(into % facts-eavt))))
      (swap! (:state this)
             (fn [s]
               (update s :avet
                       #(into % facts-avet)))))
    this)
  (fetch-facts [this query tx-id frame]
    (let [instantiated (binding/instantiate frame query (fn [v _f] v))]
      (fetch-facts-from-index @(:state this) instantiated tx-id)))
  (get-next-id [this]
    (:id-counter
     (swap! (:state this)
            #(update % :id-counter inc)))))

(defn new
  "Returns a new, empty in-memory store."
  []
  (->InMemoryStorageBackend
   (atom {:eavt (set/sorted-set-by cmp-fact-vec)
          :avet (set/sorted-set-by cmp-fact-vec)
          :id-counter 0})))
