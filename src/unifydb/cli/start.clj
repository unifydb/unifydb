(ns unifydb.cli.start
  (:require [clojure.string :as string]
            [clojure.tools.cli :as cli]
            [taoensso.timbre :as log]
            [unifydb.cache.memory :as memcache]
            [unifydb.config :as config]
            [unifydb.kvstore.memory :as memstore]
            [unifydb.messagequeue.memory :as memq]
            [unifydb.query :as query]
            [unifydb.server :as server]
            [unifydb.service :as service]
            [unifydb.storage :as store]
            [unifydb.transact :as transact]))

(defn usage [opts-summary]
  (->> ["usage: unifydb start [OPTION]... SERVICE..."
        ""
        "Start one or more of the core UnifyDB services."
        ""
        "OPTIONS"
        opts-summary
        ""
        "SERVICES"
        "  all       Start all the services"
        "  server    Start the web server"
        "  query     Start the query service"
        "  transact  Start the transact service"]
       (string/join \newline)))

(def options
  [["-h" "--help" "Display this message and exit"]])

(defn make-queue-backend
  "Constructs a new queue backend."
  []
  (condp = (config/queue-backend)
    :memory (memq/new)))

(defn make-storage-backend
  "Constructs a new storage backend."
  []
  (let [kvstore (condp = (config/storage-backend)
                  :memory (memstore/new))]
    (store/new! kvstore)))

(defn make-cache-backend
  []
  (condp = (config/cache-backend)
    :memory (memcache/new)))

(defn start-services! [services]
  (let [queue-backend (make-queue-backend)
        storage-backend (make-storage-backend)
        cache (make-cache-backend)
        service-impls (map #(condp = %
                              "server" (server/new queue-backend
                                                   cache)
                              "query" (query/new queue-backend storage-backend)
                              "transact" (transact/new queue-backend
                                                       storage-backend))
                           services)]
    (log/info "Starting services" :services services)
    (doseq [service service-impls]
      (service/start! service))
    (.addShutdownHook
     (Runtime/getRuntime)
     (Thread.
      (fn []
        (log/info "Shutting down services " :services services)
        (doseq [service service-impls]
          (service/stop! service)))))
    ;; Main loop
    (while true)))

(defn start
  "Start one or more of the core UnifyDB services."
  ;; TODO validate config with spec
  [args]
  (let [opts (cli/parse-opts args options :in-order true)
        services (if (some #{"all"} (:arguments opts))
                   ["server" "query" "transact"]
                   (filter #(some #{%} (:arguments opts))
                           ["server" "query" "transact"]))]
    (cond
      (:help (:options opts)) {:exit-message (usage (:summary opts)) :ok? true}
      (seq services) (start-services! services)
      :else {:exit-message (usage (:summary opts))})))
