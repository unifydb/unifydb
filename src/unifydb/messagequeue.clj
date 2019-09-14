(ns unifydb.messagequeue)

(defprotocol IMessageQueueBackend
  ;; TODO add the ability to have multiple backends keyed by queue, so
  ;;  e.g. all message on the :transact queue are in one backend and
  ;;  all message to the :query queue are in a different backend
  (publish [self queue message] "Publishes `message` onto the queue named `queue`.")
  (subscribe [self queue callback]
    "Registers a subscription that calls `callback` on every new message from `queue`.
     Returns a subscription object that can be passed to `unsubscribe`.")
  (unsubscribe [self subscription] "De-registers and cleans up the `subscription`."))

(defn qmap [queue-backend item-queue result-queue f coll]
  "Parallel map that distributes tasks across the `queue-backend`
   on the queue denoted by `queue-name`.

   `f` should be a symbol that resolves to a function, not a lambda.
   Returns immediately, but the return value can be dereferenced to get
   the full result set once the calculation is complete. Requires at least
   one subscriber registered with a callback generated via `qmap-process-fn`."
  (if (empty? coll)
    (deliver (promise) [])
    (let [state (atom {:id 0
                       :items coll
                       :results {}})
          result-promise (promise)
          next-id (fn []
                    (let [prev-id (:id @state)]
                      (swap! state #(assoc %1 :id (inc prev-id)))
                      prev-id))
          process-results (fn []
                            (unsubscribe queue-backend (:subscription @state))
                            (let [results (:results @state)
                                  ids (sort (keys results))]
                              (map #(get results %1) ids)))
          result-callback (fn [message]
                            (let [result (:result message)
                                  results (:results @state)]
                              (swap! state #(assoc %1 :results
                                                   (assoc results (:id message) result)))
                              (when (= (count (:results @state)) (count (:items @state)))
                                (deliver result-promise (process-results)))))
          result-subscription (subscribe queue-backend result-queue result-callback)
          state (swap! state #(assoc %1 :subscription result-subscription))]
      (doseq [item coll]
        (publish queue-backend
                 item-queue
                 {:id (next-id)
                  :fn f
                  :item item}))
      result-promise)))

(defn qmap-process-fn [queue-backend result-queue]
  "Returns a function that processes a qmap item
   and publishes the results to `result-queue`"
  (fn [message]
    (let [result (apply (:fn message) [(:item message)])]
      (publish queue-backend result-queue {:id (:id message)
                                           :result result}))))
