(ns unifydb.cli.unifydb
  (:require [clojure.string :as string]
            [clojure.tools.cli :as cli]
            [unifydb.cli.start :as start]
            [unifydb.config :as config]))

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

(def unifydb-opts
  [["-c" "--config FILE" "Configuration file path"
    :default "/etc/unifydb/config.edn"]
   ["-h" "--help" "Display this message and exit"]])

(def help-opts [])

(defn help
  "Display program usage documentation."
  [_config & args]
  (let [opts (cli/parse-opts args help-opts :in-order true)
        subcmd (first (:arguments opts))]
    (cond
      (= subcmd "start") {:exit-message (start/usage
                                         (:summary
                                          (cli/parse-opts [] start/options)))
                          :ok? true}
      (nil? subcmd) {:exit-message (unifydb-usage
                                    (:summary (cli/parse-opts [] unifydb-opts)))
                     :ok? true}
      :else {:exit-message (unifydb-usage (cli/summarize unifydb-opts))})))

(defn unifydb
  "The UnifyDB command-line interface."
  [& args]
  (let [opts (cli/parse-opts args unifydb-opts :in-order true)
        config (:config (:options opts))
        subcmd (first (:arguments opts))
        subcmd-args (rest (:arguments opts))]
    (config/load-env! :config-file config)
    (cond
      (:help (:options opts)) {:exit-message (unifydb-usage (:summary opts))
                               :ok? true}
      (= "start" subcmd) (apply start/start subcmd-args)
      (= "help" subcmd) (apply help config subcmd-args)
      :else {:exit-message (unifydb-usage (:summary opts))})))
