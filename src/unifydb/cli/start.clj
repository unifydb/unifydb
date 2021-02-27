(ns unifydb.cli.start
  (:require [clojure.string :as string]
            [clojure.tools.cli :as cli]
            [taoensso.timbre :as log]
            [unifydb.cache.memory :as memcache]
            [unifydb.messagequeue.memory :as memq]
            [unifydb.query :as query]
            [unifydb.server :as server]
            [unifydb.service :as service]
            [unifydb.kvstore.memory :as memstore]
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
  "Constructs a new queue backend from the `config` map."
  [config]
  (condp = (get-in config [:queue-backend :type])
    :memory (memq/new)))

(defn make-storage-backend
  "Constructs a new storage backend from the `config` map."
  [config]
  (condp = (get-in config [:storage-backend :type])
    :memory (memstore/new)))

(defn make-cache-backend
  [config]
  (condp = (get-in config [:cache-backend :type])
    :memory (memcache/new)))

(defn start-services! [config services]
  (let [queue-backend (make-queue-backend config)
        storage-backend (make-storage-backend config)
        cache (make-cache-backend config)
        service-impls (map #(condp = %
                              "server" (server/new queue-backend
                                                   storage-backend
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
  [config & args]
  (let [opts (cli/parse-opts args options :in-order true)
        services (if (some #{"all"} (:arguments opts))
                   ["server" "query" "transact"]
                   (filter #(some #{%} (:arguments opts))
                           ["server" "query" "transact"]))]
    (cond
      (:help (:options opts)) {:exit-message (usage (:summary opts)) :ok? true}
      (seq services) (start-services! config services)
      :else {:exit-message (usage (:summary opts))})))
