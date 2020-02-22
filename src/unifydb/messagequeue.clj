(ns unifydb.messagequeue
  (:require [taoensso.timbre :as log])
  (:import [java.util UUID]))

(defmulti publish-impl (fn [backend queue message] (:type backend)))

(defmulti subscribe-impl (fn [backend queue group] (:type backend)))

(defn publish
  "Publishes `message` onto the queue named `queue`."
  [backend queue message]
  (log/debug "Publishing message"
             :queue queue
             :message message
             :backend backend)
  (publish-impl backend queue message))

(defn subscribe
  "Returns a Manifold stream containing messages published onto `queue`."
  ([backend queue]
   (log/debug "Subscribing to queue"
              :queue queue
              :backend backend
              :group-id nil)
   (subscribe backend queue nil))
  ([backend queue group-id]
   (log/debug "Subscribing to queue in group"
              :queue queue
              :backend backend
              :group-id group-id)
   (subscribe-impl backend queue group-id)))
