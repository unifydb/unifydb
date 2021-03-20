(ns unifydb.server
  (:require [aleph.http :as http]
            [clojure.string :as string]
            [compojure.core :as compojure :refer [GET POST]]
            [compojure.route :as route]
            [manifold.deferred :as d]
            [ring.middleware.keyword-params :as keyword-params]
            [ring.middleware.nested-params :as nested-params]
            [ring.middleware.params :as params]
            [ring.util.request :as request]
            [taoensso.timbre :as log]
            [unifydb.auth :as auth]
            [unifydb.config :as config]
            [unifydb.edn :as edn]
            [unifydb.service :as service]
            [unifydb.transact :as transact]
            [unifydb.util :as util])
  (:import [java.util UUID]))

(defn query [queue-backend]
  (fn [request]
    (let [query-data (:query (:body request))
          db {:tx-id (:tx-id (:body request))}
          query-results (util/query queue-backend db query-data)]
      (d/chain query-results
               :results
               #(assoc {} :body %)))))

(defn transact [queue-backend]
  (fn [request]
    (let [tx-data (:tx-data (:body request))
          tx-result (transact/transact queue-backend tx-data)]
      (d/chain tx-result
               :tx-report
               #(assoc {} :body %)))))

(defn secure-routes [queue-backend]
  (compojure/routes
   (POST "/query" _request (query queue-backend))
   (POST "/transact" _request (transact queue-backend))))

(defn routes [queue-backend cache]
  (compojure/routes
   (compojure/wrap-routes
    (secure-routes queue-backend)
    auth/wrap-jwt-auth)
   (GET "/authenticate" _request
     (auth/login-get-salt-handler queue-backend cache))
   (POST "/authenticate" _request
     (auth/login-handler queue-backend cache))
   (route/not-found
    {:body {:message "These aren't the droids you're looking for."}})))

(defn wrap-content-type [handler]
  (fn [request]
    (if-let [content-type (request/content-type request)]
      (let [body (condp = (string/lower-case content-type)
                   "application/edn" (edn/read-string
                                      (request/body-string request))
                   :unsupported)]
        (if (= :unsupported body)
          (d/success-deferred
           {:status 400
            :body {:message (str "Unsupported content type " content-type)}})
          (handler (assoc request :body body))))
      (handler request))))

(defn wrap-accept-type [handler]
  (fn [request]
    (let [accept-type (or (get (:headers request) "accept") "application/edn")
          wrapper (condp = (string/lower-case accept-type)
                    "application/edn" {:fn pr-str :type "application/edn"}
                    "*/*" {:fn pr-str :type "application/edn"}
                    {:error (format "Unsupported accept type %s" accept-type)})]
      (if (:error wrapper)
        (d/success-deferred {:status 400
                             :headers {"Content-Type" "application/edn"}
                             :body (pr-str (:error wrapper))})
        (d/chain (handler request)
                 #(update-in % [:body] (:fn wrapper))
                 #(assoc-in % [:headers "Content-Type"] (:type wrapper)))))))

(defn wrap-logging [handler]
  (fn [request]
    (let [id (str (UUID/randomUUID))]
      (log/info "Received request" :request request :request-id id)
      (let [res (handler request)]
        (d/chain
         res
         (fn [response]
           (log/info "Returning response" :response response :request-id id)
           response))))))

(defn app [state]
  (let [{:keys [queue-backend cache]} @state]
    (-> (routes queue-backend cache)
        (params/wrap-params)
        (keyword-params/wrap-keyword-params)
        (nested-params/wrap-nested-params)
        (wrap-logging)
        (wrap-content-type)
        (wrap-accept-type))))

(defn start-server! [state]
  (let [server (:server @state)
        handler (app state)]
    (when server (.close server))
    (swap! state #(assoc % :server
                         (http/start-server handler {:port (config/port)})))))

(defn stop-server! [state]
  (let [server (:server @state)]
    (when server (.close server))))

(defrecord WebServerService [state]
  service/IService
  (start! [self]
    (start-server! (:state self)))
  (stop! [self]
    (stop-server! (:state self))))

(defn new [queue-backend cache]
  (->WebServerService (atom {:queue-backend queue-backend
                             :cache cache})))
