(ns unifydb.repl
  "A bunch of utilities to make the REPL development experience easier"
  {:clj-kondo/config
   '{:linters
     {:unused-namespace
      {:exclude [clojure.edn
                 clojure.pprint
                 clojure.test
                 unifydb.messagequeue
                 unifydb.transact
                 unifydb.util]}
      :unused-referred-var
      {:exclude {clojure.pprint [pprint]
                 clojure.test [run-tests]
                 unifydb.messagequeue [publish subscribe]
                 unifydb.transact [transact]
                 unifydb.util [query]}}
      :refer-all
      {:exclude [clojure.repl]}}}}
  (:require [clojure.edn :as edn]
            [clojure.pprint :refer [pprint]]
            [clojure.repl :refer :all]
            [clojure.test :refer [run-tests]]
            [cognitect.test-runner :as test-runner]
            [unifydb.cache.memory :as memcache]
            [unifydb.config :as config]
            [unifydb.id]
            [unifydb.kvstore :as kvstore]
            [unifydb.kvstore.memory :as memstore]
            [unifydb.messagequeue :as queue :refer [publish subscribe]]
            [unifydb.messagequeue.memory :as memq]
            [unifydb.query :as query]
            [unifydb.server :as server]
            [unifydb.service :as service]
            [unifydb.storage :as store]
            [unifydb.structlog :as structlog]
            [unifydb.transact :as transact :refer [transact]]
            [unifydb.util :refer [query]]))

(structlog/init!)
(structlog/set-log-formatter! #'structlog/human-format)

(config/load-env!)

(defn run-all-tests []
  (test-runner/test {}))

(defn make-state []
  (let [queue (memq/new)
        memstore (memstore/new)
        kvstore (kvstore/new memstore)
        store (store/new! kvstore)
        cache (memcache/new)]
    {:queue queue
     :store store
     :cache cache
     :server (server/new queue cache)
     :query (query/new queue store)
     :transact (transact/new queue store)}))

(defonce state (atom (make-state)))

(defn start-server! []
  (service/start! (:server @state)))

(defn stop-server! []
  (service/stop! (:server @state)))

(defn restart-server! []
  (service/stop! (:server @state))
  (service/start! (:server @state)))

(defn start-query-service! []
  (service/start! (:query @state)))

(defn stop-query-service! []
  (service/stop! (:query @state)))

(defn restart-query-service! []
  (service/stop! (:query @state))
  (service/start! (:query @state)))

(defn start-transact-service! []
  (service/start! (:transact @state)))

(defn stop-transact-service! []
  (service/stop! (:transact @state)))

(defn restart-transact-service! []
  (service/stop! (:transact @state))
  (service/start! (:transact @state)))

(defn start-system! []
  (start-query-service!)
  (start-transact-service!)
  (start-server!))

(defn stop-system! []
  (stop-query-service!)
  (stop-transact-service!)
  (stop-server!))

(defn restart-system! []
  (stop-system!)
  (reset! state (make-state))
  (start-system!))
