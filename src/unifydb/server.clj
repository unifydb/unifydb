(ns unifydb.server
  (:require [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.string :as string]
            [compojure.core :as compojure :refer [GET POST]]
            [compojure.route :as route]
            [ring.adapter.jetty :as jetty]
            [ring.util.request :as request]
            [unifydb.messagequeue :as queue]
            [unifydb.service :as service])
  (:import [java.util UUID]))

(def query-results (atom {}))

(defn register-result-listener [query-id callback]
  (swap! query-results #(assoc % query-id callback)))

(defn receive-query-result [message]
  (let [{:keys [id results]} message]
    (when-let [callback (get @query-results id)]
      (callback message))
    (swap! query-results #(dissoc % id))))

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
  (fn [request respond raise]
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
                     :id id}]
      (register-result-listener
            id
            ;; TODO error handling - what happens if something is down or times out?
            ;; TODO add a timeout
            (fn [message]
              (respond (if (= (string/lower-case (get (:headers request) "accept"))
                              "application/json")
                         (translate-edn-result (:results message))
                         (:results message)))))
      (queue/publish queue-backend :query query-msg))))

(defn routes [queue-backend storage-backend]
  (compojure/routes
   (POST "/query" request (query queue-backend storage-backend))
   (route/not-found
    {:body {:message "These aren't the droids you're looking for."}})))

(defn wrap-content-type [handler]
  (fn [request respond raise]
    (let [content-type (request/content-type request)
          body (condp = (string/lower-case content-type)
                 "application/json" (json/read-str (request/body-string request) :key-fn keyword)
                 "application/edn" (edn/read-string (request/body-string request))
                 :unsupported)]
      (if (= :unsupported body)
        (respond
         {:status 400
          :body {:message (str "Unsupported content type " content-type)}})
        (handler (assoc request :body body) respond raise)))))

(defn wrap-accept-type [handler]
  (fn [request respond raise]
    (let [accept-type (or (get (:headers request) "accept") "application/json")
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

(defn app [queue-backend storage-backend]
  (-> (routes queue-backend storage-backend)
      (wrap-content-type)
      (wrap-accept-type)))

(defn start-server! [server handler-fn queue-backend]
  (when @server (.stop @server))
  ;; TODO add config system and read port from config with default
  (queue/subscribe queue-backend :query/results #(receive-query-result %))
  (reset! server (jetty/run-jetty (handler-fn) {:port 8181
                                                :async? true
                                                :join? false})))

(defn stop-server! [server queue-backend]
  (queue/unsubscribe queue-backend :query/results)
  (when @server (.stop @server)))

(defrecord WebServerService [server handler-fn queue-backend]
  service/IService
  (start! [self] (start-server! (:server self)
                                (:handler-fn self)
                                (:queue-backend self)))
  (stop! [self] (stop-server! (:server self) queue-backend)))

(defn new [queue-backend storage-backend]
  "Returns a new server component instance"
  (->WebServerService (atom nil)
                      #(app queue-backend storage-backend)
                      queue-backend))
