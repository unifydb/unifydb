(ns unifydb.repl
  "A bunch of utilities to make the REPL development experience easier"
  (:require [cognitect.test-runner :as test-runner]
            [clojure.edn :as edn]
            [clojure.repl :refer :all]
            [clojure.pprint :refer [pprint]]
            [clojure.test :refer [run-tests]]
            [taoensso.timbre :as log]
            [unifydb.messagequeue :as queue :refer [publish subscribe]]
            [unifydb.messagequeue.memory :as memq]
            [unifydb.query :as query]
            [unifydb.service :as service]
            [unifydb.server :as server]
            [unifydb.storage.memory :as memstore]
            [unifydb.structlog :as structlog]
            [unifydb.transact :as transact :refer [transact]]
            [unifydb.util :refer [query]]))

(structlog/init!)
(structlog/set-log-formatter! #'structlog/human-format)

(defn run-all-tests []
  (test-runner/test {}))

(defonce queue {:type :memory})

(defonce storage {:type :memory})

(defonce server (atom (server/new queue storage)))

(defonce query-service (atom (query/new queue storage)))

(defonce transact-service (atom (transact/new queue storage)))

(defn start-server! []
  (service/start! @server))

(defn stop-server! []
  (service/stop! @server))

(defn restart-server! []
  (service/stop! @server)
  (service/start! @server))

(defn start-query-service! []
  (service/start! @query-service))

(defn stop-query-service! []
  (service/stop! @query-service))

(defn restart-query-service! []
  (service/stop! @query-service)
  (service/start! @query-service))

(defn start-transact-service! []
  (service/start! @transact-service))

(defn stop-transact-service! []
  (service/stop! @transact-service))

(defn restart-transact-service! []
  (service/stop! @transact-service)
  (service/start! @transact-service))

(defn start-system! []
  (start-query-service!)
  (start-transact-service!)
  (start-server!))

(defn stop-system! []
  (stop-query-service!)
  (stop-transact-service!)
  (stop-server!)
  (memq/reset-state!))

(defn restart-system! []
  (memq/reset-state!)
  (restart-query-service!)
  (restart-transact-service!)
  (restart-server!))
