(ns unifydb.structlog
  (:require [clojure.data.json :as json]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [clj-logging-config.log4j :as logconfig]))

;; TODO add support for logging throwables
;; TODO capture exceptions from manifold stream errors and put them into a structured format

;; TODO read in config from unifyDB config.edn file
;; TODO instead of hard-coding the logger here support multiple loggers
(logconfig/set-loggers! :root {:pattern "%m%n"})

;; TODO a much more elegant formatter system would work like Ring,
;; where the log object is passed through a stack of middlewares
;; that manipulate the object and finally logs the end result.
;; Another helpful feature would be being able to "bind" certain
;; values to a logger so that e.g. every log gets a request id on it
;; automatically.

;; This could look similar to the with-logging-context macro from
;; https://github.com/malcolmsparks/clj-logging-config#log4j-ndc-and-mdc

(def edn-format pr-str)

(def json-format json/write-str)

(defn human-format [msg]
  (format "%s [%s] - %s %s"
          (-> msg (:level) (name) (string/upper-case))
          (:ns msg)
          (:message msg)
          (-> msg
              (:data)
              (#(reduce
                 (fn [acc [k v]] (str acc (name k) "=" (pr-str v) " "))
                 ""
                 %))
              (string/trim))))

(def state (atom {:formatter edn-format}))

(defn set-log-formatter! [formatter]
  (swap! state #(assoc % :formatter formatter)))

(defn set-log-level! [level]
  (logconfig/set-loggers! :root {:level level}))

(defn log* [level data]
  (log/log level ((:formatter @state) data)))

(defmacro log [level msg & data]
  (when-not (= (mod (count data) 2) 0)
    (throw (IllegalArgumentException. "Expected an even number of data forms")))
  `(let [ns# ~*ns*
         data-map# (apply hash-map (list ~@data))
         log# {:ns (symbol (str ns#))
               :level ~level
               :timestamp (.getTime (java.util.Date.))
               :message ~msg
               :data data-map#}]
     (log* ~level log#)))

(defmacro trace [msg & data]
  `(log :trace ~msg ~@data))

(defmacro debug [msg & data]
  `(log :debug ~msg ~@data))

(defmacro info [msg & data]
  `(log :info ~msg ~@data))

(defmacro warn [msg & data]
  `(log :warn ~msg ~@data))

(defmacro error [msg & data]
  `(log :error ~msg ~@data))

(defmacro fatal [msg & data]
  `(log :fatal ~msg ~@data))
