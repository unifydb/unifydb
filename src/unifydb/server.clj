(ns unifydb.server
  (:require [ring.adapter.jetty :as jetty]
            [unifydb.service :as service]))

(defn app [request]
  {:status 200
   :headers {"Content-Type" "text/html"}
   :body "<h1>TODO implement me</h1>"})

(defn start-server! [server handler queue-backend]
  (when @server (.stop @server))
  ;; TODO add config system and read port from config with default
  (reset! server (jetty/run-jetty handler {:port 8181
                                           :join? false})))

(defn stop-server! [server]
  (when @server (.stop @server)))

(defrecord WebServerService [server handler queue-backend]
  service/IService
  (start! [self] (start-server! (:server self) (:handler self) (:queue-backend self)))
  (stop! [self] (stop-server! (:server self))))

(defn new [queue-backend]
  "Returns a new server component instance"
  (->WebServerService (atom nil) #'app queue-backend))
