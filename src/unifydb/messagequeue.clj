(ns unifydb.messagequeue)

(defmulti publish-impl (fn [backend queue message] (:type backend)))

(defmulti subscribe-impl (fn [backend & args] (:type backend)))

(defn publish [backend queue message]
  "Publishes `message` onto the queue named `queue`."
  (publish-impl backend queue message))

(defn subscribe
  "Returns a Manifold stream containing messages published onto `queue`."
  ([backend queue]
   (subscribe-impl backend queue))
  ([backend queue group]
   (subscribe-impl backend queue group)))
