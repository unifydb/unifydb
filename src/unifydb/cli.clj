(ns unifydb.cli
  (:require [clojure.edn :as edn]
            [clojure.string :as string]
            [clojure.tools.cli :as cli]
            [unifydb.query :as query]
            [unifydb.server :as server]
            [unifydb.service :as service]
            [unifydb.transact :as transact]
            ;; The backend implementations are imported to register their multimethods.
            ;; It would be nice if there was a way to set things up so this isn't necessary...
            [unifydb.messagequeue.memory]
            [unifydb.storage.memory]
            [unifydb.structlog :as log])
  (:import [java.io FileNotFoundException]))

(def default-config
  {:port 8181
   :queue-backend {:type :memory}
   :storage-backend {:type :memory}})

(defn unifydb-usage [opts-summary]
  (->> ["usage: unifydb [OPTION]... SUBCOMMAND"
        ""
        "The UnifyDB command-line interface."
        ""
        "OPTIONS"
        opts-summary
        ""
        "SUBCOMMANDS"
        "  start    Start one or more of the core UnifyDB services"
        "  help     Display program usage documentation"
        ""
        "Run \"unifydb help <SUBCOMMAND>\" for usage information for each subcommand."]
       (string/join \newline)))

(defn start-usage [opts-summary]
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

(def unifydb-opts
  [["-c" "--config FILE" "Configuration file path"
    :default-fn (fn [opts]
                  (try
                    (edn/read-string (slurp "/etc/unifydb/config.edn"))
                    (catch FileNotFoundException ex default-config)))
    :default-desc "/etc/unifydb/config.edn"
    :parse-fn (fn [path]
                (try
                  (edn/read-string (slurp path))
                  (catch FileNotFoundException ex default-config)))]
   ["-h" "--help" "Display this message and exit"]])

(def start-opts
  [["-h" "--help" "Display this message and exit"]])

(def help-opts [])

(defn start-server [config])

(defn start-query [config])

(defn start-transact [config])

(defn start-services! [config services]
  (let [queue-backend (:queue-backend config)
        storage-backend (:storage-backend config)
        service-impls (map #(condp = %
                              "server" (server/new queue-backend storage-backend)
                              "query" (query/new queue-backend)
                              "transact" (transact/new queue-backend))
                           services)]
    (log/info "Starting services" :services services)
    (doseq [service service-impls]
      (service/start! service))
    (-> (Runtime/getRuntime)
        (.addShutdownHook
         (Thread.
          (fn []
            (log/info "Shutting down services " :services services)
            (doseq [service service-impls]
              (service/stop! service))))))
    ;; Main loop
    (while true)))
      

(defn start [config & args]
  "Start one or more of the core UnifyDB services."
  (let [opts (cli/parse-opts args start-opts :in-order true)
        services (if (some #{"all"} (:arguments opts))
                   ["server" "query" "transact"]
                   (filter #(some #{%} (:arguments opts))
                           ["server" "query" "transact"]))]
    (cond
      (:help (:options opts)) {:exit-message (start-usage (:summary opts)) :ok? true}
      (not (empty? services)) (start-services! config services)
      :else {:exit-message (start-usage (:summary opts))})))

(defn help [config & args]
  "Display program usage documentation."
  (let [opts (cli/parse-opts args help-opts :in-order true)
        subcmd (first (:arguments opts))]
    (cond
      (= subcmd "start") {:exit-message (start-usage (:summary (cli/parse-opts [] start-opts))) :ok? true}
      (nil? subcmd) {:exit-message (unifydb-usage (:summary (cli/parse-opts [] unifydb-opts))) :ok? true}
      :else {:exit-message (unifydb-usage (cli/summarize unifydb-opts))})))

(defn unifydb [& args]
  "The UnifyDB command-line interface."
  (let [opts (cli/parse-opts args unifydb-opts :in-order true)
        config (:config (:options opts))
        subcmd (first (:arguments opts))
        subcmd-args (rest (:arguments opts))]
    (cond
      (:help (:options opts)) {:exit-message (unifydb-usage (:summary opts)) :ok? true}
      (= "start" subcmd) (apply start config subcmd-args)
      (= "help" subcmd) (apply help config subcmd-args)
      :else {:exit-message (unifydb-usage (:summary opts))})))
                                          

(defn -main [& args]
  (let [{:keys [exit-message ok?]} (apply unifydb args)]
    (when exit-message (println exit-message))
    (System/exit (if ok? 0 1))))
  
