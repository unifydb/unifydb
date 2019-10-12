(ns unifydb.messagequeue.memory
  (:require [manifold.bus :as bus]
            [manifold.stream :as s]
            [unifydb.messagequeue :as q])
  (:import [java.util UUID]))

;; :queues is a map from queue names to a map
;; that looks like the following:
;; {:subscription <event bus subscription stream>
;;  :groups {<group name> {:members [<subscriber streams>]
;;                         :next 0}}}
(defonce state (atom {:bus (bus/event-bus)
                      :queues {}}))

(defn reset-state! []
  (swap! state (fn [_]
                 {:bus (bus/event-bus)
                  :queues {}})))

(defmethod q/publish-impl :memory [backend queue message]
  (bus/publish! (:bus @state) queue message))

(defn choose-group-member [queue group]
  "Chooses a member of the group via round-robin."
  (let [g (-> @state :queues queue :groups group)
        n (-> (:next g)
              (mod (count (:members g))))]
    (swap! state #(assoc-in % [:queues queue :groups group :next] (inc n)))
    (nth (:members g) n)))

(defn send-out-message [queue group message]
  "Sends out the message to only one member of the group."
  (let [chosen-member (choose-group-member queue group)]
    (when chosen-member
     (s/put! chosen-member message))))

(defn subscribe-group [queue group]
  (when-not (-> @state :queues queue)
    (let [subscription (bus/subscribe (:bus @state) queue)]
     (swap! state #(assoc-in % [:queues queue]
                          {:subscription subscription
                           :groups {group {:members []
                                           :next 0}}}))
     (s/consume
      (fn [msg] (send-out-message queue group msg))
      subscription)))
  (let [queue-stream (-> @state :queues queue :subscription)
        subscriber-stream (s/stream)]
    (s/on-closed
     subscriber-stream
     (fn []
       (swap!
        state
        #(assoc-in % [:queues queue :groups group :members]
                (remove (partial = subscriber-stream)
                        (-> % :queues queue :groups group :members))))
       (when (empty (-> @state :queues queue :groups group :members))
         (swap! state #(assoc-in % [:queues queue] (dissoc (-> % :queues queue :groups) group))))
       (when (empty (-> @state :queues queue :groups))
         (s/close! queue-stream)
         (swap! state #(assoc % :queues (dissoc (:queues %) queue))))))
    (swap! state
           #(assoc-in % [:queues queue :groups group]
                      (assoc (-> % :queues queue :groups group) :members
                             (conj (-> % :queues queue :groups group :members) subscriber-stream))))
    subscriber-stream))

(defmethod q/subscribe-impl :memory
  ([backend queue]
   (let [id (UUID/randomUUID)]
     (subscribe-group queue (keyword (str id)))))
  ([backend queue group]
   (subscribe-group queue group)))
