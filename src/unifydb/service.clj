(ns unifydb.service
  (:require [clojure.tools.logging :as log]
            [manifold.stream :as s]
            [unifydb.messagequeue :as queue]
            [unifydb.util :as util]))

(defprotocol IService
  (start! [self])
  (stop! [self]))

(defrecord QueueConsumerService [queue-backend queue-fns group-name state]
  IService
  (start! [self]
    (swap! (:state self) #(assoc %1 :started true))
    (doseq [[queue-name queue-fn] (:queue-fns self)]
      (let [subscription
            (queue/subscribe (:queue-backend self) queue-name group-name)]
        (swap! (:state self)
               #(assoc %1 :subscriptions (conj (:subscriptions %1) subscription)))
        (s/consume queue-fn subscription))))
  (stop! [self]
    (doseq [subscription (:subscriptions @(:state self))]
      (s/close! subscription))
    (swap! (:state self) #(assoc %1 :started false))))

(defn make-service [queue-backend queue-fns group-name]
  (->QueueConsumerService queue-backend queue-fns group-name (atom {:started false
                                                                    :subscriptions []})))
