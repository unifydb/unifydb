(ns unifydb.kvstore
  "Key-value store protocol. Implementations must support string keys
  and arbitrary EDN values. Operates under a batching mechanism
  whereby changes are first staged and must be committed via the
  (commit!) function to persist them. Note that changes to a
  particular kvstore instance are immediately visible via (get), even
  before they are committed."
  (:refer-clojure :exclude [assoc! dissoc!] :rename {contains? map-contains?
                                                     get map-get})
  (:require [clojure.core.match :as match]
            [clojure.set :as set]
            [manifold.deferred :as d]
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
  "Returns a deferred containing a seq of values in kvstore for each
  of the `ks`. Values not in the store will be represented by `nil`."
  [store ks]
  (let [state (swap! store
                     (fn [state]
                       (assoc state :cache
                              (d/let-flow [cache (:cache state)
                                           deleted (:deleted state)
                                           to-retrieve (set/difference (set ks)
                                                                       (set (keys cache))
                                                                       deleted)
                                           retrieved (when-not (empty? to-retrieve)
                                                       (kvstore-backend/get-all
                                                        (:backend state)
                                                        to-retrieve))]
                                (if-not (empty? retrieved)
                                  (merge cache retrieved)
                                  cache)))))]
    (d/let-flow [deleted (:deleted state)
                 cache (:cache state)]
      (map (fn [key]
             (when-not (deleted key)
               (cache key)))
           ks))))

(defn get
  "Retrieves the value associated with `key` in `store`."
  [store key]
  (d/chain (get-batch store [key]) first))

(defn assoc!
  "Sets `key` to `value` in the store cache but does not actually
  persist these changes to the store. Returns the store."
  [store key value]
  (swap! store
         (fn [state]
           (-> state
               (assoc :cache (d/chain (:cache state) #(assoc % key value)))
               (assoc :modified (d/chain (:modified state) #(conj % key)))
               (assoc :deleted (d/chain (:deleted state) #(disj % key))))))
  store)

(defn dissoc!
  "Enqueues a deletion for `key` in the store cache. Returns the store."
  [store key]
  (swap! store
         (fn [state]
           (-> state
               (assoc :cache (d/chain (:cache state) #(dissoc % key)))
               (assoc :modified (d/chain (:modified state) #(disj % key)))
               (assoc :deleted (d/chain (:deleted state) #(conj % key))))))
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
  (d/let-flow [state @store
               cache (:cache state)
               deleted (:deleted state)
               uncached (set/difference (set ks)
                                        (set (keys cache))
                                        deleted)
               cached (set/difference (set ks) uncached)
               contains-uncached? (kvstore-backend/contains-all?
                                   (:backend state)
                                   uncached)]
    (and
     contains-uncached?
     (every?
      (fn [key]
        (and (map-contains? cache key)
             (not (deleted key))))
      cached))))

(defn contains? [store key]
  (contains-batch? store [key]))

;; TODO add a way to discard enqueued modifications

(defn commit!
  "Persists enqueued modifications and deletions to the KV store."
  [store]
  (d/let-flow [state @store
               cache (:cache state)
               modified (:modified state)
               deleted (:deleted state)
               modifications (map (fn [key]
                                    [:assoc! key (map-get cache key)])
                                  modified)
               deletions (map (fn [key] [:dissoc! key]) deleted)
               _ (kvstore-backend/write-all! (:backend state)
                                             (concat modifications deletions))]
    (swap! store
           (fn [s]
             (-> s
                 (assoc :cache (d/success-deferred {}))
                 (assoc :modified (d/success-deferred #{}))
                 (assoc :deleted (d/success-deferred #{})))))
    store))

(defn new [backend]
  (atom {:backend backend
         :cache (d/success-deferred {})
         :modified (d/success-deferred #{})
         :deleted (d/success-deferred #{})}))
