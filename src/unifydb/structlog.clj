(ns unifydb.structlog
  (:require [clojure.data.json :as json]
            [clojure.string :as string]
            [clojure.pprint :as pprint]
            [taoensso.timbre :as timbre]))

;; TODO read in config from unifyDB config.edn file

(defn args->data-map
  "Transforms raw vargs from a Timbre log call into a structured map."
  [vargs]
  (apply hash-map vargs))

(defn log-map->structlog [log-map]
  {:ns (:?ns-str log-map)
   :level (:level log-map)
   :timestamp (inst-ms (:instant log-map))
   :message (first (:vargs log-map))
   :data (args->data-map (rest (:vargs log-map)))})

(defn with-err [data-map log-map]
  (if (:?err log-map)
    (assoc data-map :error (Throwable->map (:?err log-map)))
    data-map))

(defn edn-format [log-map]
  (let [structlog (with-err (log-map->structlog log-map) log-map)]
    (pr-str structlog)))

;; TODO this will blow up if it encounters a data object that isn't json-serializable
(defn json-format [log-map]
  (let [structlog (with-err (log-map->structlog log-map) log-map)]
    (json/write-str structlog)))

(defn human-format [log-map]
  (let [fmtd
        (format "%s [%s] - %s %s"
                (->> log-map
                     (:level)
                     (name)
                     (string/upper-case)
                     (timbre/color-str
                      (condp = (:level log-map)
                        :debug :blue
                        :warn :yellow
                        :error :red
                        :fatal :red
                        :green)))
                (:?ns-str log-map)
                (first (:vargs log-map))
                (->> log-map
                     (:vargs)
                     (rest)
                     (partition 2)
                     (#(reduce
                        (fn [acc [k v]] (str acc
                                             (timbre/color-str :blue (name k))
                                             "="
                                             (timbre/color-str :purple (pr-str v))
                                             " "))
                        ""
                        %))
                     (string/trim)))]
    (if (:?err log-map)
      (format "%s\n%s"
              fmtd
              (timbre/stacktrace (:?err log-map)))
      fmtd)))

(defn set-log-formatter! [formatter]
  (timbre/merge-config!
   {:output-fn formatter}))

(defn transform-spy [log-map]
  (if (= (second (:vargs log-map)) "=>")
    (assoc log-map :vargs
           `["Spied value"
             ~(first (:vargs log-map))
             ~@(subvec (:vargs log-map) 2)])
    log-map))

(defn init! []
  (timbre/merge-config!
   {:level :info
    :output-fn #'edn-format
    :middleware [#'transform-spy]})
  (timbre/handle-uncaught-jvm-exceptions!))
