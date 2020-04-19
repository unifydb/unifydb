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
        "  help       Display subcommand usage documentation"
        ""]
       (string/join \newline)))

(def options [["-h" "--help" "Display this message and exit"]])

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
      (= "create" subcmd) ()
      :else {:exit-message (usage (:summary opts))})))
