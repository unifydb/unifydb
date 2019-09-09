(ns unifydb.service
  (:require [unifydb.messagequeue :as queue]
            [unifydb.util :as util]))

(defprotocol IService
  (start! [self])
  (stop! [self]))

(defrecord QueueConsumerService [queue-backend queue-fns state]
  IService
  (start! [self]
    (swap! (:state self) #(assoc %1 :started true))
    (doseq [[queue-name queue-fn] (:queue-fns self)]
      (let [subscription
            (queue/subscribe (:queue-backend self) queue-name queue-fn)]
        (swap! (:state self)
               #(assoc %1 :subscriptions (conj (:subscriptions %1) subscription))))))
  (stop! [self]
    (doseq [subscription (:subscriptions @(:state self))]
      (queue/unsubscribe (:queue-backend self) subscription))
    (swap! (:state self) #(assoc %1 :started false))))

(defn make-service [queue-backend queue-fns]
  (->QueueConsumerService queue-backend queue-fns (atom {:started false
                                                         :subscriptions []})))
