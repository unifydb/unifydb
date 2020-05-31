(ns unifydb.messagequeue.memory
  (:require [manifold.bus :as bus]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [unifydb.messagequeue :as q]))

(defn group-key
  "Returns the keyword that identifies
   the group designated by `queue` and `group-id`
   in (:groups @state)."
  [queue group-id]
  (keyword (str (name queue) "-" (name group-id))))

(defn make-group
  "Makes a new consumer group, which consists of
   an event bus subscription stream and a set of consumers."
  [state queue]
  (let [subscription (bus/subscribe (:bus state) queue)
        consumers (atom #{})
        consumer-stream (->> (repeatedly #(deref consumers))
                             (apply concat)
                             (filter #(not (s/closed? %)))
                             (filter #(contains? @consumers %))
                             (s/->source))]
    {:subscription subscription
     :consumers consumers
     :consumer-stream consumer-stream}))

(defn maybe-add-group
  "Adds the group designated by `queue` and `group-id`
   to `state` if it does not already exist."
  [state queue group-id]
  (if (get-in state [:groups (group-key queue group-id)])
    state
    (let [group (make-group state queue)]
      (s/consume-async
       (fn [msg]
         (d/chain (s/take! (:consumer-stream group))
                  (fn [consumer] (s/put! consumer msg))))
       (:subscription group))
      (assoc-in state
                [:groups (group-key queue group-id)]
                group))))

(defn remove-consumer!
  "Removes the `consumer` from the group designated
   by the `queue` and `group-id` in `state`."
  [state queue group-id consumer-stream]
  (let [consumers (get-in state
                          [:groups
                           (group-key queue group-id)
                           :consumers])]
    (when consumers
      (swap! consumers
             #(disj % consumer-stream)))
    state))

(defn maybe-close-group!
  "Closes the group subscription and cleans up the group
   if no consumers remain in that group."
  [state queue group-id]
  (let [consumers (get-in state
                          [:groups (group-key queue group-id) :consumers])
        subscription (get-in state
                             [:groups (group-key queue group-id) :subscription])]
    (if (and consumers (empty? (deref consumers)))
      (do
        (s/close! subscription)
        (update-in state [:groups] dissoc (group-key queue group-id)))
      state)))

(defn make-consumer
  "Constructs a new consumer stream."
  [state queue group-id]
  (let [consumer-stream (s/stream)]
    (s/on-closed
     consumer-stream
     (fn []
       (swap! state
              #(-> %
                   (remove-consumer! queue group-id consumer-stream)
                   (maybe-close-group! queue group-id)))))
    consumer-stream))

(defn add-consumer!
  "Adds the `consumer-stream` to the `state`."
  [state consumer-stream queue group-id]
  (swap! (get-in state [:groups (group-key queue group-id) :consumers])
         #(conj % consumer-stream))
  state)

(defn subscribe-in-group!
  "Returns a new subscription stream that receives
   messages from `queue` as a part of the group
   designated by `group-id`. Mutates `state`."
  [state queue group-id]
  (let [consumer-stream (make-consumer state queue group-id)]
    (swap! state #(-> %
                      (maybe-add-group queue group-id)
                      (add-consumer! consumer-stream queue group-id)))
    consumer-stream))

(defrecord InMemoryMessageQueueBackend [state]
  q/IMessageQueueBackend
  (publish [this queue message]
    (bus/publish! (:bus @(:state this)) queue message))
  (subscribe [this queue]
    (bus/subscribe (:bus @(:state this)) queue))
  (subscribe [this queue group-id]
    (subscribe-in-group! state queue group-id)))

(defn new
  "Returns a new in-memory message queue backend."
  []
  (->InMemoryMessageQueueBackend
   (atom {:bus (bus/event-bus)
          :groups {}})))
