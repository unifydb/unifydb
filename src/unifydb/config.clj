(ns unifydb.config
  (:require [config.core :as c]))

(defonce env-state (atom {:env nil}))

(defn load-env!
  [& {:keys [config-file overrides]
      :or {config-file (:unifydb-config c/env)
           overrides {}}}]
  (swap! env-state assoc :env
         (c/load-env (c/read-env-file config-file)
                     overrides)))

(defn get-config
  [key & {:keys [default required]}]
  (when (nil? (:env @env-state))
    (throw (ex-info "Env not initialized" {})))
  (let [val (get (:env @env-state) key default)]
    (if (and required (nil? val))
      (throw (ex-info (format "Missing required config key %s" key)
                      {:key key}))
      val)))

(defmacro with-config
  "Overwrite the config in `body` with the values in the `override` map."
  [overrides & body]
  `(let [old-config# (:env @env-state)]
     (swap! env-state #(assoc % :env
                              (merge (:env %)
                                     ~overrides)))
     ~@body
     (swap! env-state #(assoc % :env old-config#))))

(defn secret []
  (get-config :secret :required true))

(defn port []
  (get-config :port :default 8181))

(defn token-ttl-seconds []
  (get-config :token-ttl-seconds :default 3600))

(defn queue-backend []
  (get-config :queue-backend :default :memory))

(defn storage-backend []
  (get-config :storage-backend :default :memory))

(defn cache-backend []
  (get-config :cache-backend :default :memory))

(defn jdbc-url []
  ;; Only required for JDBC storage backend, which is the only
  ;; circumstances under which this will be called
  (get-config :jdbc-url :required true))
