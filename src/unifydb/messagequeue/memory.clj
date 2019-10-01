(ns unifydb.messagequeue.memory
  (:require [manifold.bus :as bus]
            [unifydb.messagequeue :as q]))

(defrecord InMemoryMessageQueueBackend [bus]
  q/IMessageQueueBackend
  (publish [self queue message] (bus/publish! bus queue message))
  (subscribe [self queue] (bus/subscribe bus queue)))

(defn new []
  (->InMemoryMessageQueueBackend (bus/event-bus)))
