(ns unifydb.kvstore.memory
  (:require [unifydb.kvstore :as storage])
  (:refer-clojure :rename {contains? map-contains?}))

(defrecord InMemoryKeyValueStore [state]
  storage/IKeyValueStore
  (get-batch [store keys]
    (map @(:state store) keys))
  (write-batch! [store operations]
    (swap! (:state store)
           (fn [m]
             (let [assocs (->> operations
                               (filter #(= (first %) :assoc!))
                               (map (comp vec rest)))
                   dissocs (->> operations
                                (filter #(= (first %) :dissoc!))
                                (map (comp first rest)))]
               (as-> m v
                 (into v assocs)
                 (apply dissoc v dissocs))))))
  (contains-batch? [store keys]
    (every? #(map-contains? @(:state store) %) keys)))

(defn new []
  (->InMemoryKeyValueStore (atom {})))
