(ns unifydb.storage.memory
  (:require [unifydb.storage :as storage])
  (:refer-clojure :rename {contains? map-contains?}))

(defrecord InMemoryKeyValueStore [state]
  storage/IKeyValueStore
  (store-get [store key]
    (get @(:state store) key))
  (assoc! [store key val]
    (swap! (:state store) assoc key val))
  (contains? [store key]
    (map-contains? @(:state store) key)))

(defn new []
  (->InMemoryKeyValueStore (atom {})))
