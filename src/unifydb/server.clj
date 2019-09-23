(ns unifydb.server
  (:require [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.string :as string]
            [compojure.core :as compojure :refer [GET POST]]
            [compojure.route :as route]
            [ring.adapter.jetty :as jetty]
            [ring.util.request :as request]
            [unifydb.service :as service]))

(defn query [queue-backend]
  (fn [request respond raise]
   (respond {:body {:message "TODO implement me"}})))

(defn routes [queue-backend]
  (compojure/routes
   (POST "/query" request (query queue-backend))
   (route/not-found
    {:body {:message "These aren't the droids you're looking for."}})))

(defn wrap-content-type [handler]
  (fn [request respond raise]
    (let [content-type (get (:headers request) "content-type")
          body (condp = (string/lower-case content-type)
                 "application/json" (json/read-str (request/body-string request) :key-fn keyword)
                 "application/edn" (edn/read-string (request/body-string request))
                 :unsupported)]
      (if (= :unsupported body)
        (respond
         {:status 400
          :body {:message (format "Unsupported content type %s" content-type)}})
        (handler (assoc request :body body) respond raise)))))

(defn wrap-accept-type [handler]
  (fn [request respond raise]
    (let [accept-type (or (get (:readers request) "accept") "application/json")
          wrapper (condp = (string/lower-case accept-type)
                    "application/json" {:fn json/write-str :type "application/json"}
                    "application/edn" {:fn pr-str :type "application/edn"}
                    {:error (format "Unsupported accept type %s" accept-type)})]
      (if (:error wrapper)
        (respond {:status 400
                   :headers {"Content-Type" "application/json"}
                   :body (json/write-str (:error wrapper))})
        (handler request
                 (fn [response]
                   (respond
                    (-> response
                        (#(assoc % :body ((:fn wrapper) (:body %))))
                        (#(assoc % :headers (assoc (:headers %)
                                                   "Content-Type"
                                                   (:type wrapper)))))))
                 raise)))))

(defn app [queue-backend]
  (-> (routes queue-backend)
      (wrap-content-type)
      (wrap-accept-type)))

(defn start-server! [server handler-fn queue-backend]
  (when @server (.stop @server))
  ;; TODO add config system and read port from config with default
  (reset! server (jetty/run-jetty (handler-fn) {:port 8181
                                                :async? true
                                                :join? false})))

(defn stop-server! [server]
  (when @server (.stop @server)))

(defrecord WebServerService [server handler-fn queue-backend]
  service/IService
  (start! [self] (start-server! (:server self) (:handler-fn self) (:queue-backend self)))
  (stop! [self] (stop-server! (:server self))))

(defn new [queue-backend]
  "Returns a new server component instance"
  (->WebServerService (atom nil) #(app queue-backend) queue-backend))
