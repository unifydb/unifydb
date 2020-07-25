(ns unifydb.http-auth
  (:require [clojure.string :as str]
            [clojure.walk :as walk]
            [crypto.random :as rand]
            [manifold.deferred :as d]
            [unifydb.scram :as scram]
            [unifydb.util :as util]))

(defn auth-fields->map
  "Parses HTTP Authentication header fields into a map."
  [header]
  (as-> (str/split header #" ") v
    (second v)
    (or v "")
    (str/split v #",")
    (filter #(str/includes? % "=") v)
    (map #(str/split % #"=") v)
    (into {} v)
    (walk/keywordize-keys v)))

(defn map->auth-fields
  "Writes a map with HTTP Authentication header fields into a string."
  [field-map]
  (->> field-map
       walk/stringify-keys
       (into [])
       (map (partial str/join "="))
       (str/join ",")))

(defn auth-step-2
  [auth-fields]
  (let [scram-fields (-> (scram/decode (:c2s auth-fields))
                         (slurp)
                         (auth-fields->map))
        client-proof (-> (:p scram-fields)
                         (scram/decode)
                         (slurp))]))

(defn c2s-fields
  "Given the raw auth header fields, returns the client-to-server
  fields as a map."
  [raw-fields]
  (-> (scram/decode (:c2s raw-fields))
      (slurp)
      (auth-fields->map)))

(defn list->map
  "Transforms a list `l` into a map with `keys`.
  Returns an empty map if `l` is `nil`"
  [keys l]
  (->> l
       (interleave keys)
       (partition 2)
       (map vec)
       (into {})))

(defn get-i-and-salt!
  "Returns a deferred map of {:i i :salt salt} for the given username.
  If no such i and salt exist, returns an empty map."
  [queue-backend username]
  (d/chain (util/query queue-backend {:tx-id :latest}
                       ;; TODO the query system really ought to
                       ;; support parameterized queries instead of
                       ;; having to interpolate username here
                       `{:find [?i ?s]
                         :where [[?uid :unifydb/username ~username]
                                 [?uid :unifydb/i ?i]
                                 [?uid :unifydb/salt ?s]]})
           first
           (partial list->map [:i :salt])))

(defn format-salt-and-i
  "Returns the `salt`, `i`, and `server-nonce` in the right format for
  HTTP transfer."
  [server-nonce salt i]
  (format "r=%s,s=%s,i=%s" server-nonce salt i))

(defn auth-exchange!
  "The handler function for the auth endpoint."
  [queue-backend]
  (fn [request]
    (if-let [auth-header (get-in request [:headers "authorization"])]
      (let [auth-fields (auth-fields->map auth-header)]
        (if (= (:s2s auth-fields) "step2")
          (auth-step-2 auth-fields)
          (let [c2s (c2s-fields auth-fields)
                username (:u c2s)
                nonce (:r c2s)
                server-nonce (str nonce (rand/hex 12))]
            (if-not (and username nonce)
              (d/success-deferred
               {:status 401
                :body "Missing required SCRAM fields \"n\" and \"r\""})
              (d/chain (get-i-and-salt! queue-backend username)
                       (juxt :i :salt)
                       (fn [[i salt]]
                         (if-not (and i salt)
                           (d/success-deferred
                            {:status 401
                             :body "Invalid credentials"})
                           (format-salt-and-i server-nonce salt i)))
                       scram/encode
                       (fn [encoded-s2c]
                         {:s2s "step2"
                          :s2c encoded-s2c})
                       (fn [response-fields]
                         (if (:c2c auth-fields)
                           (assoc response-fields :c2c (:c2c auth-fields))
                           response-fields))
                       (fn [response-fields]
                         {:status 401
                          :headers {"WWW-Authenticate"
                                    (format "SASL %s"
                                            (map->auth-fields response-fields))}}))))))
      (d/success-deferred
       {:status 401
        :body "Authorization header required"}))))
