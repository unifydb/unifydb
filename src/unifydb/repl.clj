(ns unifydb.repl
  (:require [clojure.repl :refer :all]
            [unifydb.messagequeue.memory :as memq]
            [unifydb.service :as service]
            [unifydb.server :as server]))

(defonce queue (atom (memq/new)))

(defonce server (atom (server/new @queue)))

(defn start-server! []
  (service/start! @server))

(defn stop-server! []
  (service/stop! @server))

(defn restart-server! []
  (service/stop! @server)
  (service/start! @server))
