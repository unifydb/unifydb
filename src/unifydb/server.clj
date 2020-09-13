(ns unifydb.server
  (:require [aleph.http :as http]
            [cemerick.friend :as friend]
            [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.string :as string]
            [compojure.core :as compojure :refer [POST]]
            [compojure.route :as route]
            [manifold.deferred :as d]
            [ring.middleware.keyword-params :as keyword-params]
            [ring.middleware.nested-params :as nested-params]
            [ring.middleware.params :as params]
            [ring.util.request :as request]
            [taoensso.timbre :as log]
            [unifydb.auth :as auth]
            [unifydb.config :as config]
            [unifydb.service :as service]
            [unifydb.transact :as transact]
            [unifydb.util :as util])
  (:import [java.util UUID]))

(defn edn->json
  "Transforms idiomatic EDN to a format that can be losslessly represented as JSON."
  [edn-data]
  (cond
    (keyword? edn-data) (str edn-data)
    (symbol? edn-data) (str "'" edn-data)
    (and (string? edn-data)
         (string/starts-with? edn-data ":")) (str "\\" edn-data)
    (and (string? edn-data)
         (string/starts-with? edn-data "'")) (str "\\" edn-data)
    (and (string? edn-data)
         (string/starts-with? edn-data "\\")) (str "\\" edn-data)
    (vector? edn-data) (vec (map edn->json edn-data))
    (map? edn-data) (into {} (map
                              #(vector (edn->json (first %))
                                       (edn->json (second %)))
                              edn-data))
    (list? edn-data) (into ["#list"] (map edn->json edn-data))
    (set? edn-data) (into ["#set"] (map edn->json edn-data))
    :else edn-data))

(defn json->edn
  "Transforms JSON representing EDN data into actual EDN data. The reverse of edn->json."
  [json-data]
  (cond
    (and (string? json-data)
         (string/starts-with? json-data ":")) (keyword (subs json-data 1))
    (and (string? json-data)
         (string/starts-with? json-data "'")) (symbol (subs json-data 1))
    (and (string? json-data)
         (string/starts-with? json-data "\\")) (subs json-data 1)
    (and (vector? json-data)
         (= (first json-data) "#list")) (into '() (map json->edn (rest json-data)))
    (and (vector? json-data)
         (= (first json-data) "#set")) (set (map json->edn (rest json-data)))
    (vector? json-data) (vec (map json->edn json-data))
    (map? json-data) (into {} (map
                               #(vector (json->edn (first %))
                                        (json->edn (second %)))
                               json-data))
    :else json-data))

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

(defn routes [queue-backend]
  (compojure/routes
   (POST "/query" _request (query queue-backend))
   (POST "/transact" _request (transact queue-backend))
   (route/not-found
    {:body {:message "These aren't the droids you're looking for."}})))

(defn wrap-content-type [handler]
  (fn [request]
    (let [content-type (request/content-type request)
          body (condp = (string/lower-case content-type)
                 "application/json" (-> (request/body-string request)
                                        (json/read-str)
                                        (json->edn))
                 "application/edn" (edn/read-string (request/body-string request))
                 :unsupported)]
      (if (= :unsupported body)
        (d/success-deferred
         {:status 400
          :body {:message (str "Unsupported content type " content-type)}})
        (handler (assoc request :body body))))))

(defn wrap-accept-type [handler]
  (fn [request]
    (let [accept-type (or (get (:headers request) "accept") "application/json")
          wrapper (condp = (string/lower-case accept-type)
                    "application/json" {:fn #(-> %
                                                 (edn->json)
                                                 (json/write-str))
                                        :type "application/json"}
                    "application/edn" {:fn pr-str :type "application/edn"}
                    {:error (format "Unsupported accept type %s" accept-type)})]
      (if (:error wrapper)
        (d/success-deferred {:status 400
                             :headers {"Content-Type" "application/json"}
                             :body (json/write-str (:error wrapper))})
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
  (let [{:keys [queue-backend]} @state]
    (-> (routes queue-backend)
        (friend/authenticate
         {:workflows [(auth/jwt-workflow
                       :credential-fn auth/jwt-credential-fn)
                      (auth/login-workflow
                       :credential-fn auth/login-credential-fn)]})
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

(defn new [queue-backend storage-backend]
  (->WebServerService (atom {:queue-backend queue-backend
                             :storage-backend storage-backend})))
