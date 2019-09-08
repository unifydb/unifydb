(ns unifydb.messagequeue.memory
  (:require [unifydb.messagequeue :as q])
  (:import [clojure.lang PersistentQueue]))

(defn ensure-queue! [queues queue]
  (when-not (get @queues queue)
    (swap! queues #(assoc %1 queue (PersistentQueue/EMPTY)))))

(defn enqueue [queues queue-name message]
  (ensure-queue! queues queue-name)
  (swap! queues #(assoc %1 queue-name (conj (get %1 queue-name) message))))

(defn dequeue [queues queue-name]
  (ensure-queue! queues queue-name)
  (let [queue (get @queues queue-name)]
    (future
     (loop [message (peek queue)]
       (if (nil? message)
         (recur (peek queue))
         (do
           (swap! queues #(assoc %1 queue-name (pop (get %1 queue-name))))
           message))))))

(defrecord InMemoryMessageQueueBackend [queues]
  q/IMessageQueueBackend
  (publish [self queue message]
    (enqueue (:queues self) queue message))
  (consume [self queue]
    (dequeue (:queues self) queue)))

(defn new []
  (->InMemoryMessageQueueBackend (atom {})))
