(ns unifydb.messagequeue.memory
  (:require [manifold.bus :as bus]
            [manifold.stream :as s]
            [unifydb.structlog :as log]
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
  (let [g (get-in @state [:queues queue :groups group])
        n (-> (:next g)
              (mod (count (:members g))))]
    (swap! state #(assoc-in % [:queues queue :groups group :next] (inc n)))
    (nth (:members g) n)))

(defn send-out-message! [queue group message]
  "Sends out the message to only one member of the group."
  (let [chosen-member (choose-group-member queue group)]
    (when chosen-member
     (s/put! chosen-member message))))

(defn remove-subscriber! [queue group subscriber-stream queue-stream]
  (log/debug "Removing subscriber." :queue queue :group group :subscriber subscriber-stream)
  (swap!
   state
   #(assoc-in % [:queues queue :groups group :members]
              (remove (partial = subscriber-stream)
                      (get-in % [:queues queue :groups group :members]))))
  (when (empty (get-in @state [:queues queue :groups group :members] []))
    (log/debug "Removing group." :queue queue :group group)
    (swap! state #(assoc-in % [:queues queue]
                            (dissoc (get-in % [:queues queue :groups]) group))))
  (when (empty (get-in @state [:queues queue :groups] []))
    (log/debug "Closing queue subscription and removing queue." :queue queue)
    (s/close! queue-stream)
    (swap! state #(assoc % :queues (dissoc (:queues %) queue)))))

(defn add-subscriber! [queue group subscriber-stream]
  (swap!
   state
   #(assoc-in % [:queues queue :groups group]
              (assoc (get-in % [:queues queue :groups group]) :members
                     (conj (get-in % [:queues queue :groups group :members]) subscriber-stream)))))

(defn add-queue-subscription! [queue group]
  (let [subscription (bus/subscribe (:bus @state) queue)]
     (swap! state #(assoc-in % [:queues queue]
                          {:subscription subscription
                           :groups {group {:members []
                                           :next 0}}}))
     (s/consume
      (fn [msg] (send-out-message! queue group msg))
      subscription)))

(defn subscribe-group [queue group]
  (when-not (get-in @state [:queues queue])
    (add-queue-subscription! queue group))
  (let [queue-stream (get-in @state [:queues queue :subscription])
        subscriber-stream (s/stream)]
    (s/on-closed
     subscriber-stream
     #(remove-subscriber! queue group subscriber-stream queue-stream))
    (add-subscriber! queue group subscriber-stream)
    subscriber-stream))

(defmethod q/subscribe-impl :memory
  [backend queue group]
  (subscribe-group queue group))
