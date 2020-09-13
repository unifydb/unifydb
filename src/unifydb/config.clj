(ns unifydb.config
  (:require [config.core :as c]))

(def default-config-path "/etc/unifydb/config.edn")

(defn load-env []
  (c/load-env (c/read-env-file (or (:unifydb-config c/env)
                                   default-config-path))))

(defonce env (load-env))

(defn reload-env []
  (alter-var-root #'env (fn [_] (load-env))))

(defn get-required [env key]
  (if-let [val (get env key)]
    val
    (throw (ex-info (format "Missing required config key %s" key)
                    {:key key}))))

(defn secret []
  (get-required env :secret))

(defn port []
  (get env :port 8181))
