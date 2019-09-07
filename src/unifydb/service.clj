(ns unifydb.service
  (:require [unifydb.messagequeue :as queue]))

(defprotocol IService
  (start! [self])
  (stop! [self]))

(defn consume-loop! [queue-backend queue-name queue-fn started?]
  "Starts a loop that calls `queue-fn` on every message from `queue-name`
   while the `started?` function evaluates to true. This function blocks."
  (loop [message (queue/consume queue-backend queue-name)]
    (when (started?)
      (queue-fn @message)
      (recur (queue/consume queue-backend queue-name)))))

(defrecord QueueConsumerService [queue-backend queue-fns state]
  IService
  (start! [self]
    ;; For each mapping in queue-fns, start a loop consuming from that queue
    (swap! (:state self) #(assoc %1 :started true))
    (let [started? (fn [] (:started @(:state self)))]
     (doseq [[queue-name queue-fn] (:queue-fns self)]
       (future
         (consume-loop! (:queue-backend self) queue-name queue-fn started?)))))
  (stop! [self]
    (swap! (:state self) #(assoc %1 :started false))))

(defn make-service [queue-backend queue-fns]
  (->QueueConsumerService queue-backend queue-fns (atom {:started false})))
