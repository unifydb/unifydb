(ns unifydb.messagequeue)

(defmulti publish-impl (fn [backend queue message] (:type backend)))

(defmulti subscribe-impl (fn [backend queue] (:type backend)))

(defn publish [backend queue message]
  "Publishes `message` onto the queue named `queue`."
  (publish-impl backend queue message))

(defn subscribe [backend queue]
  "Returns a Manifold stream containing messages published onto `queue`."
  (subscribe-impl backend queue))
