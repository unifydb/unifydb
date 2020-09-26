(ns unifydb.auth
  (:require [buddy.sign.jwt :as jwt]
            [cemerick.friend :as friend]
            [cemerick.friend.workflows :as workflows]
            [clojure.core.match :refer [match]]
            [manifold.deferred :as d]
            [ring.util.request :as req]
            [taoensso.timbre :as log]
            [unifydb.config :as config]
            [unifydb.datetime :as datetime]
            [unifydb.user :as user])
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
  (when-let [auth-header (get (:headers request) "authorization")]
    (second (re-matches #"^Bearer (.*)" auth-header))))

(defn jwt-workflow [& {:keys [credential-fn]}]
  (fn [request]
    (let [credential-fn (or credential-fn
                            (get-in request [::friend/auth-config
                                             :credential-fn]))
          jwt (get-jwt request)]
      (when (and credential-fn jwt)
        (credential-fn jwt)))))

(defn jwt-credential-fn
  "Given the JWT, validates it, pulls out the user identity and roles
  and returns them in an auth-map."
  [jwt]
  (try
    (let [auth-map (jwt/unsign jwt (config/secret))]
      (when (< (datetime/between (datetime/chrono-unit :seconds)
                                 (datetime/utc-now)
                                 (datetime/from-iso (:created auth-map)))
               (config/token-ttl-seconds))
        (workflows/make-auth auth-map
                             {::friend/workflow ::jwt
                              ::friend/redirect-on-auth? false
                              ::friend/ensure-session true})))
    (catch ExceptionInfo e
      (log/warn "Error unsigning JWT" :error (ex-data e)))))

;; TODO add a nonce to prevent replay attacks
(defn login-workflow [queue-backend & {:keys [login-uri]
                                       :or {login-uri "/authenticate"}}]
  (fn [request]
    (let [path (req/path-info request)
          method (:request-method request)]
      (match [method path]
        [:get login-uri]
        (when-let [username (get (:params request)
                                 "username")]
          @(d/let-flow [user (user/get-user! queue-backend
                                             {:tx-id :latest}
                                             username)]
             {:status 200
              :body {:username username
                     :salt (:unifydb/salt user)}}))
        ;; TODO once caching layer is in place, cache user from first step
        [:post login-uri]
        (let [username (:username (:body request))
              hashed-password (:password (:body request))]
          (when (and username hashed-password)
            @(d/let-flow [user (user/get-user! queue-backend
                                               {:tx-id :latest}
                                               username)]
               (when (= hashed-password (:unifydb/password user))
                 {:status 200
                  :body {:username username
                         :token (jwt/sign {:username username
                                           ;; TODO get roles
                                           :roles [:unifydb/user]
                                           :created (datetime/iso-format
                                                     (datetime/utc-now))}
                                          (config/secret))}}))))
        :else nil))))

(defn not-authorized [_request]
  {:status 403 :body "Access denied"})

(defn not-authenticated [_request]
  ;; TODO add WWW-Authenticate header
  {:status 401 :body "Access denied"})
