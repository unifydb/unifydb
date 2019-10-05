(ns unifydb.messagequeue.memory
  (:require [manifold.bus :as bus]
            [unifydb.messagequeue :as q])
  (:import [java.util UUID]))

(def bus (atom {}))

(defmethod q/publish-impl :memory [backend queue message]
  (bus/publish! (get @bus (:id backend)) queue message))

(defmethod q/subscribe-impl :memory [backend queue]
  (bus/subscribe (get @bus (:id backend)) queue))

(defn new []
  (let [id (str (UUID/randomUUID))]
    (swap! bus #(assoc % id (bus/event-bus)))
    {:type :memory
     :id id}))
