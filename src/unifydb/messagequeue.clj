(ns unifydb.messagequeue)

(defprotocol IMessageQueueBackend
  ;; TODO add the ability to have multiple backends keyed by queue, so
  ;;  e.g. all message on the :transact queue are in one backend and
  ;;  all message to the :query queue are in a different backend
  (publish [self queue message]
    "Publishes `message` onto the queue named `queue`.")
  (subscribe [self queue]
    "Returns a Manifold stream containing messages published onto `queue`."))
