(ns unifydb.storage
  (:require [unifydb.facts :as facts]
            [unifydb.id :as id]
            [unifydb.kvstore :as kvstore]
            [unifydb.storage.btree :as btree]))

(defn index
  "Returns the index names `index` from the `store`."
  [store index]
  (get (:indices store) index))

(defn store-facts!
  "Puts `facts` into the indexes of the `store`, returning `store`."
  [store facts]
  (doseq [[eid attr value txid added? :as fact] facts]
    (btree/insert! (index store :eavt)
                   [eid attr value txid added?]
                   fact)
    (btree/insert! (index store :avet)
                   [attr value eid txid added?]
                   fact)
    ;; Only index backreferences to other entities in VAET
    (when (id/id? value)
      (btree/insert! (index store :vaet)
                     [value attr eid txid added?]
                     fact)))
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
  {:kvstore kvstore
   :indices {:eavt (btree/new! kvstore "eavt" 500)
             :avet (btree/new! kvstore "avet" 500)
             :vaet (btree/new! kvstore "vaet" 500)}})
