(ns unifydb.messagequeue.memory
  (:require [unifydb.messagequeue :as q]))

(defn do-publish [state queue-name message pub-promise]
  (deliver pub-promise nil)
  (let [callbacks (get (:queues state) queue-name)]
    (pmap #((:callback %1) message) callbacks))
  state)

(defn do-subscribe [state queue-name callback sub-promise]
  (let [next-id (:next-id state)
        queues (:queues state)
        queue (get queues queue-name)]
    (deliver sub-promise next-id)
    {:next-id (inc next-id)
     :queues (assoc queues queue-name (conj queue {:id next-id
                                                   :callback callback}))}))

(defn do-unsubscribe [state subscription unsub-promise]
  (let [queues (:queues state)
        {:keys [queue id]} subscription]
    (deliver unsub-promise nil)
    (assoc state :queues
           (assoc queues queue (filter #(not= (:id %1) id) (get queues queue))))))

(defrecord InMemoryMessageQueueBackend [agent]
  q/IMessageQueueBackend
  (publish [self queue message]
    (let [pub-p (promise)]
      (send (:agent self) do-publish queue message pub-p)
      @pub-p))
  (subscribe [self queue callback]
    (let [sub-promise (promise)]
      (send (:agent self) do-subscribe queue callback sub-promise)
      {:id @sub-promise
       :queue queue}))
  (unsubscribe [self subscription]
    (let [unsub-p (promise)]
      (send (:agent self) do-unsubscribe subscription unsub-p)
      @unsub-p)))

(defn new []
  (->InMemoryMessageQueueBackend (agent {:queues {}
                                         :next-id 0})))
