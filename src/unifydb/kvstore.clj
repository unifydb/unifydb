(ns unifydb.kvstore
  "Key-value store protocol. Implementations must support string keys
  and arbitrary EDN values. Operates under a batching mechanism
  whereby changes are first stage and must be committed via the
  commit! function to persist them. Note that changes to a particular
  kvstore instance are immediately visible via get, even before they
  are committed."
  (:refer-clojure :exclude [get assoc! dissoc!] :rename {contains? map-contains?})
  (:require [clojure.core.match :as match]
            [clojure.set :as set]
            [unifydb.kvstore.backend :as kvstore-backend]))

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

(defn get-batch
  [store ks]
  (let [state (swap! store
                     (fn [state]
                       (let [to-retrieve
                             (set/difference (set ks)
                                             (set (keys (:cache state)))
                                             (:deleted state))]
                         (if-not (empty? to-retrieve)
                           (assoc state :cache
                                  (apply assoc (:cache state)
                                         (interleave to-retrieve
                                                     (kvstore-backend/get-all
                                                      (:backend state)
                                                      to-retrieve))))
                           state))))]
    (map (fn [key]
           (when-not ((:deleted state) key)
             ((:cache state) key)))
         ks)))

(defn get
  "Retrieves the value associated with `key` in `store`."
  [store key]
  (first (get-batch store [key])))

(defn assoc!
  "Sets `key` to `value` in the store cache but does not actually
  persist these changes to the store. Returns the store."
  [store key value]
  (swap! store
         (fn [state]
           (-> state
               (assoc-in [:cache key] value)
               (assoc :modified (conj (:modified state) key))
               (assoc :deleted (disj (:deleted state) key)))))
  store)

(defn dissoc!
  "Enqueues a deletion for `key` in the store cache. Returns the store."
  [store key]
  (swap! store
         (fn [state]
           (-> state
               (assoc :cache (dissoc (:cache state) key))
               (assoc :modified (disj (:modified state) key))
               (assoc :deleted (conj (:deleted state) key)))))
  store)

(defn write-batch!
  "Performs multiple operations on `store` atomically, where each
  operation is either [:assoc! <key> <value>] or [:dissoc! <key>]"
  [store operations]
  (doseq [operation operations]
    (match/match operation
      [:assoc! key val] (assoc! store key val)
      [:dissoc! key] (dissoc! store key)
      [op & args] (throw (ex-info "Invalid batch write operation"
                                  {:operation op
                                   :args args}))))
  store)

(defn contains-batch?
  [store ks]
  (let [state @store
        uncached (set/difference (set ks)
                                 (set (keys (:cache state)))
                                 (:deleted state))
        cached (set/difference (set ks) uncached)]
    (and
     (kvstore-backend/contains-all? (:backend state) uncached)
     (every?
      (fn [key]
        (and (map-contains? (:cache state) key)
             (not ((:deleted state) key))))
      cached))))

(defn contains? [store key]
  (contains-batch? store [key]))

(defn commit!
  "Persists enqueued modifications and deletions to the KV store."
  [store]
  (let [state @store
        modifications (map (fn [key]
                             [:assoc! key (get-in state [:cache key])])
                           (:modified state))
        deletions (map (fn [key] [:dissoc! key]) (:deleted state))]
    (kvstore-backend/write-all! (:backend state)
                                (concat modifications deletions))
    (swap! store
           (fn [cache]
             (-> cache
                 (assoc :cache {})
                 (assoc :modified #{})
                 (assoc :deleted #{}))))
    store))

(defn new [backend]
  (atom {:backend backend
         :cache {}
         :modified #{}
         :deleted #{}}))
