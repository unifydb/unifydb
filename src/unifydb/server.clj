(ns unifydb.server
  (:require [aleph.http :as http]
            [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.string :as string]
            [compojure.core :as compojure :refer [GET POST]]
            [compojure.route :as route]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [ring.util.request :as request]
            [unifydb.messagequeue :as queue]
            [unifydb.service :as service]
            [unifydb.structlog :as log])
  (:import [java.util UUID]))

(defn edn->json [edn-data]
  "Transforms idiomatic EDN to a format that can be losslessly represented as JSON."
  (cond
    (keyword? edn-data) (str edn-data)
    (symbol? edn-data) (str "'" edn-data)
    (and (string? edn-data)
         (string/starts-with? edn-data ":")) (str "\\" edn-data)
    (and (string? edn-data)
         (string/starts-with? edn-data "'")) (str "\\" edn-data)
    (and (string? edn-data)
         (string/starts-with? edn-data "\\")) (str "\\" edn-data)
    (vector? edn-data) (into [] (map edn->json edn-data))
    (map? edn-data) (into {} (map
                              #(vector (edn->json (first %))
                                       (edn->json (second %)))
                              edn-data))
    (list? edn-data) (into ["#list"] (map edn->json edn-data))
    (set? edn-data) (into ["#set"] (map edn->json edn-data))
    :else edn-data))

(defn json->edn [json-data]
  "Transforms JSON representing EDN data into actual EDN data. The reverse of edn->json."
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
         (= (first json-data) "#set")) (into #{} (map json->edn (rest json-data)))
    (vector? json-data) (into [] (map json->edn json-data))
    (map? json-data) (into {} (map
                               #(vector (json->edn (first %))
                                        (json->edn (second %)))
                               json-data))
    :else json-data))

(defn query [queue-backend storage-backend]
  (fn [request]
    (let [query-data (:query (:body request))
          id (str (UUID/randomUUID))
          query-msg {:db {:queue-backend queue-backend
                          :storage-backend storage-backend
                          :tx-id (:tx-id (:body request))}
                     :query query-data
                     :id id}
          query-results (queue/subscribe queue-backend :query/results)]
      (queue/publish queue-backend :query query-msg)
      (as-> query-results v
           (s/filter #(= (:id %) id) v)
           (s/take! v)
           (d/chain v
                    :results
                    #(assoc {} :body %))
           (d/chain v #(do (s/close! query-results) %))))))

(defn transact [queue-backend storage-backend]
  (fn [request]
    (let [tx-data (:tx-data (:body request))
          id (str (UUID/randomUUID))
          tx-msg {:conn {:queue-backend queue-backend
                         :storage-backend storage-backend}
                  :tx-data tx-data
                  :id id}
          tx-results (queue/subscribe queue-backend :transact/results)]
      (queue/publish queue-backend :transact tx-msg)
      (as-> tx-results v
        (s/filter #(= (:id %) id) v)
        (s/take! v)
        (d/chain v
                 :tx-report
                 #(assoc {} :body %))
        (d/chain v #(do (s/close! tx-results) %))))))

(defn routes [queue-backend storage-backend]
  (compojure/routes
   (POST "/query" request (query queue-backend storage-backend))
   (POST "/transact" request (transact queue-backend storage-backend))
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
        (-> (handler request)
            (d/chain #(assoc % :body ((:fn wrapper) (:body %)))
                     #(assoc % :headers
                             (assoc (:headers %) "Content-Type" (:type wrapper)))))))))

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
  (let [{:keys [queue-backend storage-backend]} @state]
   (-> (routes queue-backend storage-backend)
       (wrap-logging)
       (wrap-content-type)
       (wrap-accept-type))))

(defn start-server! [state]
  (let [server (:server @state)
        handler (app state)]
    (when server (.close server))
    ;; TODO add config system and read port from config with default
    (swap! state #(assoc % :server
                         (http/start-server handler {:port 8181})))))

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
