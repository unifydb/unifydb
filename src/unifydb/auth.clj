(ns unifydb.auth
  (:require [buddy.core.codecs :as codecs]
            [buddy.core.codecs.base64 :as base64]
            [buddy.core.nonce :as nonce]
            [buddy.sign.jwt :as jwt]
            [manifold.deferred :as d]
            [taoensso.timbre :as log]
            [unifydb.config :as config]
            [unifydb.datetime :as datetime]
            [unifydb.user :as user]
            [unifydb.cache :as cache])
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

(defn get-jwt [request]
  (when-let [auth-header (get (:headers request) "authorization")]
    (second (re-matches #"^Bearer (.*)" auth-header))))

(defn validate-jwt
  "Given the JWT, validates it, pulls out the user identity and roles
  and returns them in an auth-map. Returns nil if JWT is missing or
  invalid."
  [jwt]
  (when jwt
    (try
      (let [jwt-data (jwt/unsign jwt (config/secret))]
        (when (< (datetime/between (datetime/chrono-unit :seconds)
                                   (datetime/utc-now)
                                   (datetime/from-iso (:created jwt-data)))
                 (config/token-ttl-seconds))
          jwt-data))
      (catch ExceptionInfo e
        (log/warn "Error unsigning JWT" :error (ex-data e))))))

(defn wrap-jwt-auth
  "Ring middleware to authenticate requests via a JWT.
  If authentication is successful, adds the JWT data to the request
  under an :auth key."
  [handler]
  (fn [request]
    (if-let [jwt (validate-jwt (get-jwt request))]
      (handler (assoc request :auth jwt))
      ;; TODO add WWW-Authenticate header?
      {:status 401
       :body "Access denied"})))

(defn make-jwt
  ([username roles]
   (make-jwt username
             roles
             (datetime/iso-format (datetime/utc-now))))
  ([username roles created]
   (jwt/sign {:username username
              :roles roles
              :created created}
             (config/secret))))

(defn login-get-salt-handler [queue-backend cache]
  (fn [request]
    (log/info "in login get handler")
    (if-let [username (get (:params request) "username")]
      (d/let-flow [nonce (-> (nonce/random-nonce 64)
                             (base64/encode)
                             (codecs/bytes->str))
                   nonce-key (-> (nonce/random-bytes 16)
                                 (base64/encode)
                                 (codecs/bytes->str))
                   user (user/get-user! queue-backend
                                        {:tx-id :latest}
                                        username)]
        (if user
          (do
            (cache/cache-set! cache nonce-key nonce 60)
            {:status 200
             :body {:username username
                    :salt (:unifydb/salt user)
                    :nonce-key nonce-key
                    :nonce nonce}})
          {:status 400
           :body "Invalid 'username' parameter"}))
      {:status 400
       :body "Invalid 'username' parameter"})))

(defn login-handler [queue-backend cache]
  (fn [request]
    (let [username (:username (:body request))
          hashed-password (:password (:body request))
          nonce-key (:nonce-key (:body request))
          client-nonce (:nonce (:body request))]
      (if (and username hashed-password nonce-key client-nonce)
        (d/let-flow [user (user/get-user! queue-backend
                                          {:tx-id :latest}
                                          username)
                     nonce (cache/cache-get cache nonce-key)]
          (if (and (= client-nonce nonce)
                   (= hashed-password (:unifydb/password user)))
            {:status 200
             :body {:username username
                    ;; TODO get user roles
                    :token (make-jwt username [:unifydb/user])}}
            {:status 400
             :body "Invalid username, password, or nonce"}))
        {:status 400
         :body "Invalid username, password, or nonce"}))))
