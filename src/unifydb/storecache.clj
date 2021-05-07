(ns unifydb.storecache
  "A read-through cache wrapper around the KV store, providing the
  ability to queue up modifications and execute them all at once."
  (:refer-clojure :exclude [set!] :rename {contains? map-contains?})
  (:require [unifydb.kvstore :as store]))

;; Note: if we want to support horizontal transactor scaling at some
;; point, this is where we would add a locking mechanism. There are
;; two choices: a pessimistic locking scheme where the store cache
;; acquires a lock on every key it caches the moment it caches it, or
;; an optimistic locking scheme where the store cache remembers the
;; initial value of every key it caches, and during the commit call
;; compares the currently-persisted value of that key to the one it
;; recorded, only allowing the commit to occur if those values are the
;; same. Either of these options will require some careful thought,
;; particularly because the large size of b-tree nodes makes lock
;; contention extremely likely.

(defn store-cache
  "Returns a store cache object that can be used to queue up
  modifications to the store."
  [store]
  (atom {:store store
         :cache {}
         :modified #{}
         :deleted #{}}))

(defn get!
  "Retrieves a value from the store cache, loading the value from the
  store if it isn't already cached."
  [store-cache key]
  (let [state (swap! store-cache
                     (fn [cache]
                       (if (and (not ((:deleted cache) key))
                                (not (get (:cache cache) key)))
                         (assoc-in cache [:cache key] (store/get (:store cache) key))
                         cache)))]
    (and (not ((:deleted state) key))
         (get (:cache state) key))))

(defn contains?
  "True if the store cache contains the key. Does not cache the key
  value if not already cached."
  [store-cache key]
  (or (map-contains? (:cache @store-cache) key)
      (store/contains? (:store @store-cache) key)))

(defn set!
  "Sets `key` to `value` in the store cache but does not actually
  persist these changes to the store. Returns the store cache."
  [store-cache key value]
  (swap! store-cache
         (fn [cache]
           (-> cache
               (assoc-in [:cache key] value)
               (assoc :modified (conj (:modified cache) key))
               (assoc :deleted (disj (:deleted cache) key)))))
  store-cache)

(defn delete!
  "Enqueues a deletion for `key` in the store cache. Returns the store
  cache."
  [store-cache key]
  (swap! store-cache
         (fn [cache]
           (-> cache
               (assoc :cache (dissoc (:cache cache) key))
               (assoc :modified (disj (:modified cache) key))
               (assoc :deleted (conj (:deleted cache) key)))))
  store-cache)

(defn commit!
  "Persists enqueued modifications and deletions to the KV store."
  [store-cache]
  (let [state @store-cache
        modifications (map (fn [key]
                             [:assoc! key (get-in state [:cache key])])
                           (:modified state))
        deletions (map (fn [key] [:dissoc! key]) (:deleted state))]
    (store/write-batch! (:store state)
                        (concat modifications deletions))
    (swap! store-cache
           (fn [cache]
             (-> cache
                 (assoc :cache {})
                 (assoc :modified #{})
                 (assoc :deleted #{}))))
    store-cache))
