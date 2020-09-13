(ns unifydb.auth
  (:require [buddy.sign.jwt :as jwt]
            [cemerick.friend.workflows :as workflows]
            [taoensso.timbre :as log]
            [unifydb.config :as config])
  (:import [clojure.lang ExceptionInfo]))

;; Two workflows:
;; The first one checks for a JWT and verifies a timestamp-generated
;; nonce to prevent replay attacks
;; If the first fails, the second one initiates the login sequence
;; To log in:
;; - server sends the user salt and a nonce
;; - the nonce needs to be stored somewhere accessible to all running
;;   server instances
;; - client SHA-512 hashes the password + salt + nonce
;; - server verifies with its copy of the hashed password and the nonce
;; - on success, server sends back a JWT with a (configurable) limited TTL

;; TODO these need to handle deferreds

(defn get-jwt [request]
  (when-let [auth-header (:authorization (:headers request))]
    (second (re-matches #"^Bearer (.*)" auth-header))))

(defn jwt-workflow [& {:keys [credential-fn]}]
  (fn [request]
    (let [credential-fn (or credential-fn
                            (get-in request [:friend/auth-config
                                             :credential-fn]))
          jwt (get-jwt request)]
      (when (and credential-fn jwt)
        (credential-fn jwt)))))

(defn jwt-credential-fn
  "Given the JWT, validates it, pulls out the user identity and roles
  and returns them in an auth-map."
  [jwt]
  (try
    (jwt/unsign jwt (config/secret))
    (catch ExceptionInfo e
      (log/warn "Error unsigning JWT" :error (ex-data e)))))

(defn login-workflow [& {:keys [credential-fn]}]
  (fn [request]))

(defn login-credential-fn
  "Given the username and hashed password+nonce, verifies the user and
  returns an auth-map with the user identity and any user roles."
  [])
