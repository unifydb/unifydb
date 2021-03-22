(ns unifydb.config
  (:require [config.core :as c]))

;; TODO support running as non-root
(def default-config-path "/etc/unifydb/config.edn")

(defonce env-state (atom {:env nil}))

(defn load-env!
  ([config-file]
   (swap! env-state assoc :env
          (c/load-env (c/read-env-file (or config-file
                                           default-config-path)))))
  ([]
   (load-env! (:unifydb-config c/env))))

(defn env-get
  [key & {:keys [default required]}]
  (when (nil? (:env @env-state))
    (throw (ex-info "Env not initialized" {})))
  (let [val (get (:env @env-state) key default)]
    (if (and required (nil? val))
      (throw (ex-info (format "Missing required config key %s" key)
                      {:key key}))
      val)))

(defn secret []
  (env-get :secret :required true))

(defn port []
  (env-get :port :default 8181))

(defn token-ttl-seconds []
  (env-get :token-ttl-seconds :default 3600))

(defn queue-backend []
  (env-get :queue-backend :default :memory))

(defn storage-backend []
  (env-get :storage-backend :default :memory))

(defn cache-backend []
  (env-get :cache-backend :default :memory))
