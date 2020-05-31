(ns unifydb.cli.user
  (:require [clojure.string :as string]
            [clojure.tools.cli :as cli]))

(defn usage [opts-summary]
  (->> ["usage: unifydb user [OPTION]... SUBCOMMAND"
        ""
        "Manage users."
        ""
        "OPTIONS"
        opts-summary
        ""
        "SUBCOMMANDS"
        "  create     Create a new user"
        "  help       Display this documentation"
        ""]
       (string/join \newline)))

(def options [["-h" "--help" "Display this message and exit"]])

;; TODO should create read a password from stdin if one isn't passed in?

(defn create-usage [opts-summary]
  (->> ["usage: unifydb user create [OPTION]... [USERNAME] [PASSWORD]"
        ""
        "Create a new user with USERNAME and PASSWORD."
        "Prompt for values if USERNAME or PASSWORD are not supplied."
        ""
        "OPTIONS"
        opts-summary]
       (string/join \newline)))

(def create-options [["-h" "--help" "Display this message and exit"]])

(defn create
  "Creates a new user."
  [_config & args]
  (let [opts (cli/parse-opts args create-options :in-order true)
        ;; TODO read user input if no username or password
        username (first args)
        password (second args)]
    (cond
      (:help (:options opts)) {:exit-message (create-usage (:summary opts))
                               :ok? true}
      ;; TODO
      )))

(defn user
  "Subcommands for user management."
  [config & args]
  (let [opts (cli/parse-opts args options :in-order true)
        subcmd (first (:arguments opts))
        subcmd-args (rest (:arguments opts))]
    ;; TODO implement me
    (cond
      (or (:help (:options opts))
          (= "help" subcmd)) {:exit-message (usage (:summary opts))
                              :ok? true}
      (= "create" subcmd) (apply create config subcmd-args)
      :else {:exit-message (usage (:summary opts))})))
