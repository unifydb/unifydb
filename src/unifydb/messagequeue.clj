(ns unifydb.messagequeue
  (:require [unifydb.structlog :as log])
  (:import [java.util UUID]))

(defmulti publish-impl (fn [backend queue message] (:type backend)))

(defmulti subscribe-impl (fn [backend queue group] (:type backend)))

(defn publish [backend queue message]
  "Publishes `message` onto the queue named `queue`."
  (log/debug "Publishing message." :queue queue :message message)
  (publish-impl backend queue message))

(defn subscribe
  "Returns a Manifold stream containing messages published onto `queue`."
  ([backend queue]
   (let [id (-> (UUID/randomUUID) (str) (keyword))]
    (subscribe backend queue id)))
  ([backend queue group]
   (log/debug "Subscribing to queue." :queue queue :group group)
   (subscribe-impl backend queue group)))
