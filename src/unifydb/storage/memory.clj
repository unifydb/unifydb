(ns unifydb.storage.memory
  (:require [unifydb.storage :as storage]))

(defrecord InMemoryKeyValueStore [state]
  storage/IKeyValueStore
  (store-get [store key]
    (get @(:state store) key))
  (store-assoc! [store key val]
    (swap! (:state store) assoc key val)))

(defn new []
  (->InMemoryKeyValueStore (atom {})))
