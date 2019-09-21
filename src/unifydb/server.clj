(ns unifydb.server
  (:require [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.string :as string]
            [compojure.core :refer [GET POST defroutes]]
            [compojure.route :as route]
            [ring.adapter.jetty :as jetty]
            [ring.util.request :as request]
            [unifydb.service :as service]))

(defroutes routes
  (POST "/query" request
    {:status 200
     :body {:message "TODO write me"}})
  (route/not-found
   {:body {:message "These aren't the droids you're looking for."}}))

(defn wrap-content-type [handler]
  (fn [request]
    (let [content-type (get (:headers request) "content-type")
          body (condp = (string/lower-case content-type)
                 "application/json" (json/read-str (request/body-string request) :key-fn keyword)
                 "application/edn" (edn/read-string (request/body-string request))
                 :unsupported)]
      (if (= :unsupported body)
        {:status 400
         :body {:message (format "Unsupported content type %s" content-type)}}
        (handler (assoc request :body body))))))

(defn wrap-accept-type [handler]
  (fn [request]
    (let [accept-type (get (:headers request) "accept")
          wrap-fn (fn [wrap-type-fn content-type]
                    (-> (handler request)
                        (#(assoc % :body (wrap-type-fn (:body %))))
                        (#(assoc % :headers
                                 (assoc (:headers %)
                                        "Content-Type"
                                        content-type)))))]
      (condp = (string/lower-case accept-type)
        "application/json" (wrap-fn json/write-str "application/json")
        "application/edn" (wrap-fn pr-str "application/edn")
        nil (wrap-fn json/write-str "application/json")
        {:status 400
         :headers {"Content-Type" "application/json"}
         :body (json/write-str {:message (format "Unsupported accept type %s" accept-type)})}))))

(def app (-> routes (wrap-content-type) (wrap-accept-type)))

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
