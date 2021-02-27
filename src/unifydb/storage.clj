(ns unifydb.storage
  (:require [unifydb.kvstore :as kvstore]
            [unifydb.storage.btree :as btree]))

(defn insert-facts!
  [])

(defn get-next-id!
  "Returns the next available sequential ID. Not thread-safe, only
  call this in the transactor."
  [storage]
  (let [next-id (or (kvstore/get (:kvstore storage) "id-counter")
                    0)]
    (kvstore/assoc! (:kvstore storage) "id-counter" (inc next-id))
    next-id))

(defn new!
  "Returns a new storage backend, creating the indices in the
  `kvstore` if they don't exist."
  [kvstore]
  {:kvstore kvstore
   :indices {:eavt (btree/new! kvstore "eavt" 500)
             ;; TODO also have AEVT?
             :avet (btree/new! kvstore "avet" 500)
             :vaet (btree/new! kvstore "vaet" 500)}})
