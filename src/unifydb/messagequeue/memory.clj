(ns unifydb.messagequeue.memory
  (:require [unifydb.messagequeue :as q]))

(defn do-publish [state queue-name message]
  (let [callbacks (get (:queues @state) queue-name)]
    (pmap #((:callback %1) message) callbacks)))

(defn do-subscribe [state queue-name callback]
  (let [next-id (:next-id @state)
        queues (:queues @state)
        queue (get queues queue-name)]
    (swap! state
           #(-> %
                (assoc :next-id (inc next-id))
                (assoc :queues
                       (assoc queues queue-name
                              (conj queue {:id next-id :callback callback})))))
    {:id next-id :queue queue-name}))

(defn do-unsubscribe [state subscription]
  (let [queues (:queues @state)
        {:keys [queue id]} subscription]
    (swap! state
           (fn [s]
             (assoc s :queues
                    (assoc queues queue
                           (filter #(not= (:id %1) id) (get queues queue))))))))

(defrecord InMemoryMessageQueueBackend [state]
  q/IMessageQueueBackend
  (publish [self queue message]
    (do-publish (:state self) queue message))
  (subscribe [self queue callback]
    (do-subscribe (:state self) queue callback))
  (unsubscribe [self subscription]
    (do-unsubscribe (:state self) subscription)))

(defn new []
  (->InMemoryMessageQueueBackend (atom {:queues {} :next-id 0})))
