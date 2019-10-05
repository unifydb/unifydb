(ns unifydb.messagequeue.memory
  (:require [manifold.bus :as bus]
            [unifydb.messagequeue :as q]))

(def bus (atom (bus/event-bus)))

(defmethod q/publish-impl :memory [backend queue message]
  (bus/publish! @bus queue message))

(defmethod q/subscribe-impl :memory [backend queue]
  (bus/subscribe @bus queue))

(defn new []
  {:type :memory})
