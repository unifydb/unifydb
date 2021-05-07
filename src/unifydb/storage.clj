(ns unifydb.storage
  (:require [unifydb.facts :as facts]
            [unifydb.id :as id]
            [unifydb.kvstore :as kvstore]
            [unifydb.storecache :as storecache]
            [unifydb.storage.btree :as btree]))

(defn index
  "Returns the index names `index` from the `store`."
  [store index]
  (get (:indices store) index))

(defn store-facts!
  "Puts `facts` into the indexes of the `store`, returning `store`."
  [store facts]
  (let [eavt-kvs (map
                  (fn [[eid attr value txid added? :as fact]]
                    [[eid attr value txid added?] fact])
                  facts)
        avet-kvs (map
                  (fn [[eid attr value txid added? :as fact]]
                    [[attr value eid txid added?] fact])
                  facts)
        vaet-kvs (filter
                  (complement nil?)
                  (map
                   (fn [[eid attr value txid added? :as fact]]
                     ;; Only index backreferences to other entities in VAET
                     (when (id/id? value)
                       [[value attr eid txid added?] fact]))
                   facts))]
    (btree/insert! (index store :eavt) eavt-kvs)
    (btree/insert! (index store :avet) avet-kvs)
    (btree/insert! (index store :vaet) vaet-kvs)
    (storecache/commit! (:store-cache store)))
  store)

(defn get-matching-facts
  "Fetches facts from the `store` with matching `entity-id`,
  `attribute`, and/or `value`."
  [store {:keys [entity-id attribute value tx-id]}]
  (let [[search idx] (cond
                       entity-id [[entity-id attribute value] :eavt]
                       (and value (id/id? value)) [[value attribute entity-id] :vaet]
                       attribute [[attribute value entity-id] :avet]
                       :else [[entity-id attribute value] :eavt])]
    (->> (vec (take-while (complement nil?) search))
         (btree/search (index store idx))
         (map :value)
         ;; TODO I think there is a much more efficient way to do this
         ;; involving sorting the btree contents by tx-id so we don't need
         ;; to iterate over the entire result set here
         (filter #(#{0 -1} (compare (facts/fact-tx-id %) tx-id))))))

(defn get-next-id!
  "Returns the next available sequential ID. Not thread-safe, only
  call this in the transactor."
  [storage]
  (let [next-id (or (kvstore/get (:kvstore storage) "id-counter")
                    1)]
    (kvstore/assoc! (:kvstore storage) "id-counter" (inc next-id))
    (id/id next-id)))

(defn new!
  "Returns a new storage backend, creating the indices in the
  `kvstore` if they don't exist."
  [kvstore]
  (let [store-cache (storecache/store-cache kvstore)
        indices {:eavt (btree/new! store-cache "eavt" 500)
                 :avet (btree/new! store-cache "avet" 500)
                 :vaet (btree/new! store-cache "vaet" 500)}]
    (storecache/commit! store-cache)
    {:kvstore kvstore
     :store-cache store-cache
     :indices indices}))
