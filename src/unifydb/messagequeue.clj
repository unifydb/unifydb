(ns unifydb.messagequeue)

(defprotocol IMessageQueueBackend
  ;; TODO add the ability to have multiple backends keyed by queue, so
  ;;  e.g. all message on the :transact queue are in one backend and
  ;;  all message to the :query queue are in a different backend
  (publish [self queue message] "Publishes `message` onto the queue named `queue`.")
  (consume [self queue] "Returns a future of the next message in `queue`."))

(defn qmap [queue-backend item-queue result-queue f coll]
  "Parallel map that distributes tasks across the `queue-backend`
   on the queue denoted by `queue-name`.

   `f` should be a symbol that resolves to a function, not a lambda.
   Returns a future. Requires at least one consumer that listens to
   `result-queue` and calls a `qmap-process-fn` on each item."
  (let [state (atom {:id 0
                     :results {}})
        next-id (fn []
                  (let [prev-id (:id @state)]
                    (swap! state #(assoc %1 :id (inc prev-id)))
                    prev-id))
        process-results (fn []
                          (let [results (:results @state)
                                ids (sort (keys results))]
                            (map #(get results %1) ids)))]
    (doseq [item coll]
      (publish queue-backend
               item-queue
               {:id (next-id)
                :fn f
                :item item}))
    (future
      (loop [message (consume queue-backend result-queue)]
        (if (= (count (:results @state)) (count coll))
          (process-results)
          (do
            (let [result (:result @message)
                  results (:results @state)]
              (swap! state #(assoc %1 :results (assoc results (:id @message) result)))
              (recur (consume queue-backend result-queue)))))))))

(defn qmap-process-fn [queue-backend result-queue]
  "Returns a function that processes a qmap item
   and publishes the results to `result-queue`"
  (fn [message]
    (let [result (apply (:fn message) [(:item message)])]
      (publish queue-backend result-queue {:id (:id message)
                                           :result result}))))
