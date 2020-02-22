(ns unifydb.messagequeue)

(defprotocol IMessageQueueBackend
  (publish [this queue message]
    "Publishes `message` onto the queue named `queue`.")
  (subscribe [this queue] [this queue group-id]
    "Returns a Manifold stream containing messages published onto `queue`."))
