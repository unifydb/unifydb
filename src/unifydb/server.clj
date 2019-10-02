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
            [unifydb.service :as service])
  (:import [java.util UUID]))

(defn translate-json-query [json-query]
  "Translates queries from the JSON-compatible format to
   a proper EDN format with keywords, symbols, and variables."
  (let [val (if (keyword? json-query) (name json-query) json-query)]
   (cond
     (string/starts-with? val "_?_") (symbol (str "?" (subs val 3)))
     (string/starts-with? val "_:_") (keyword (subs val 3))
     (string/starts-with? val "_'_") (symbol (subs val 3))
     (map? val) (into {} (map
                          #(vector (translate-json-query (first %))
                                   (translate-json-query (second %)))
                          json-query))
     (vector? val) (into [] (map translate-json-query json-query))
     :else json-query)))

(defn translate-edn-result [edn-result]
  "Translates an EDN-formatted query-result into JSON quasi-edn."
  (cond
    (keyword? edn-result) (str "_:_" (name edn-result))
    (and (symbol? edn-result)
         (string/starts-with? (name edn-result) "?")) (str "_?_" (subs (name edn-result) 1))
    (symbol? edn-result) (str "_'_" (name edn-result))
    (map? edn-result) (into {} (map
                                #(vector (translate-edn-result (first %))
                                         (translate-edn-result (second %)))
                                edn-result))
    (vector? edn-result) (into [] (map translate-edn-result edn-result))
    :else edn-result))

(defn query [queue-backend storage-backend]
  (fn [request]
    (let [raw-query (:query (:body request))
          query-data (if (= (string/lower-case (request/content-type request))
                            "application/json")
                       (translate-json-query raw-query)
                       raw-query)
          id (UUID/randomUUID)
          query-msg {:db {:queue-backend queue-backend
                          :storage-backend storage-backend
                          :tx-id (:tx-id (:body request))}
                     :query query-data
                     :id id}
          ;; TODO if I don't close this, am I creating a new persistent subscription on every request?
          query-results (queue/subscribe queue-backend :query/results)]
      (queue/publish queue-backend :query query-msg)
      (as-> query-results v
           (s/filter #(= (:id %) id) v)
           (s/take! v)
           (d/chain v :results)))))

(defn routes [queue-backend storage-backend]
  (compojure/routes
   (POST "/query" request (query queue-backend storage-backend))
   (route/not-found
    {:body {:message "These aren't the droids you're looking for."}})))

(defn wrap-content-type [handler]
  (fn [request]
    (let [content-type (request/content-type request)
          body (condp = (string/lower-case content-type)
                 "application/json" (json/read-str (request/body-string request) :key-fn keyword)
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
                    "application/json" {:fn json/write-str :type "application/json"}
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

(defn app [state]
  (let [{:keys [queue-backend storage-backend]} @state]
   (-> (routes queue-backend storage-backend)
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
