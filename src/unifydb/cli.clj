(ns unifydb.cli
  (:require [clojure.edn :as edn]
            [clojure.string :as string]
            [clojure.tools.cli :as cli])
  (:import [java.io FileNotFoundException]))

(def cmd-name "unifydb")

(def default-config
  {:port 8181})

(defn get-config [default-config config key]
  (or (key config) (key default-config)))

(defmulti cli-command (fn [cmd parent-cmds parent-opts & args] cmd))

(defmulti cli-help (fn [cmd parent-cmds opts] cmd))

(defmulti cli-description (fn [cmd] cmd))

(defmacro defcommand [cmd [opts parent-opts parent-cmds] description
                      [& arg-options] [& child-args] & body]
  "Define a new CLI command. Any child-args must be previously defined."
  (let [arg-options (conj arg-options ["-h" "--help" "Print this message and exit"])]
   `(do
      (defmethod cli-description ~(str cmd) [cmd#]
        ~description)
      (defmethod cli-help ~(str cmd) [cmd# parent-cmds# opts#]
        (println
         (str "usage: "
              (string/join " " parent-cmds#)
              (if parent-cmds# " " "")
              cmd# " [OPTION]... ARG...\n"
              (cli-description ~(str cmd)) "\n"
              "\n"
              "OPTIONS\n"
              (:summary opts#) "\n"
              ~@(when child-args
                  (apply list
                         "\n"
                         "ARGUMENTS\n"
                         (map (fn [[argname arg-desc]]
                                (str "  " argname ": "
                                     (or arg-desc (cli-description argname)) "\n"))
                              child-args))))))
      (defmethod cli-command ~(str cmd) [cmd# parent-cmds# parent-opts# & args#]
        (let [~opts (cli/parse-opts args# ~(vec arg-options) :in-order true)
              ~parent-opts parent-opts#
              ~parent-cmds parent-cmds#]
          (if (:help (:options ~opts))
            (cli-help ~(str cmd) (vec (map str parent-cmds#)) ~opts)
            (do ~@body)))))))

(defcommand server [opts parent-opts parent-cmds]
  "Start the web server."
  [] [])

(defcommand query [opts parent-opts parent-cmds]
  "Start the query service."
  [] [])

(defcommand transact [opts parent-opts parent-cmds]
  "Start the transact service."
  [] [])

(defcommand all [opts parent-opts parent-cmds]
  "Start all UnifyDB services."
  [] [])
                      
(defcommand start [opts parent-opts parent-cmds]
  "Start one or more UnifyDB components."
  []
  [["all"]
   ["server"]
   ["query"]
   ["transact"]]
  (cond
    (some #{"all"} (:arguments opts))
    (cli-command "all" (conj parent-cmds "start") opts)
    (first (:arguments opts))
    (apply cli-command
           (first (:arguments opts))
           (conj parent-cmds "start")
           opts
           (rest (:arguments opts)))
    :else (cli-help "start" parent-cmds opts)))

(defcommand help [opts parent-opts parent-cmds]
  "Display help message."
  [] []
  (cli-help "unifydb" [] parent-opts))

(defcommand unifydb [opts parent-opts parent-cmds]
  "The UnifyDB command-line interface."
  [["-c" "--config FILE" "Configuration file path"
    :default-fn (fn [opts]
                  (try
                    (edn/read-string (slurp "/etc/unifydb/config.edn"))
                    (catch FileNotFoundException ex default-config)))
    :default-desc "/etc/unifydb/config.edn"
    :parse-fn (fn [path]
                (try
                  (edn/read-string (slurp path))
                  (catch FileNotFoundException ex default-config)))]]
  [["start"] ["help"]]
  (cond
    (first (:arguments opts))
    (apply cli-command (first (:arguments opts)) [cmd-name] opts (rest (:arguments opts)))
    :else (cli-command "help" [] opts)))

(defn -main [& args]
  (apply cli-command "unifydb" nil {} args))
