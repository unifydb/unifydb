(ns unifydb.kvstore.memory
  (:require [unifydb.kvstore.backend :as store-backend]
            [manifold.deferred :as d])
  (:refer-clojure :rename {contains? map-contains?}))

(defrecord InMemoryKeyValueStore [state]
  store-backend/IKeyValueStoreBackend
  (get-all [store keys]
    (d/success-deferred (select-keys @state keys)))
  (write-all! [store operations]
    (d/future
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
                   (apply dissoc v dissocs)))))
      true))
  (contains-all? [store keys]
    (d/success-deferred
     (every? #(map-contains? @(:state store) %) keys))))

(defn new []
  (->InMemoryKeyValueStore (atom {})))
