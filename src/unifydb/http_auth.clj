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

(defn auth-step-1
  "Step 1 of SCRAM exchange: given the username and nonce from the
  client, return the salt and the iteration count for that user."
  [queue-backend auth-fields]
  (let [scram-fields (-> (scram/decode (:c2s auth-fields))
                         (slurp)
                         (auth-fields->map))
        username (:n scram-fields)
        nonce (:r scram-fields)]
    (if-not (and username nonce)
      (d/success-deferred
       {:status 401
        :body "Missing required SCRAM fields \"n\" and \"r\""})
      (d/chain (util/query queue-backend {:tx-id :latest}
                           ;; TODO the query system really ought to
                           ;; support parameterized queries instead of
                           ;; having to interpolate username here
                           `{:find [?i ?s]
                             :where [[?uid :unifydb/username ~username]
                                     [?uid :unifydb/i ?i]
                                     [?uid :unifydb/salt ?s]]})
               (fn [results]
                 (if (empty? results)
                   {:status 401
                    :body "Invalid credentials"}
                   (let [[[i salt]] results
                         server-nonce (str nonce (rand/hex 12))
                         response-str (format "r=%s,s=%s,i=%s"
                                              server-nonce
                                              salt
                                              i)
                         response-fields (assoc auth-fields
                                                :s2s "step2"
                                                :s2c (scram/encode response-str))]
                     {:status 401
                      :headers {"WWW-Authenticate"
                                (format "SASL %s"
                                        (map->auth-fields response-fields))}})))))))

(defn auth-step-2
  [auth-fields])

(defn auth-exchange
  "The handler function for the auth endpoint"
  [queue-backend]
  (fn [request]
    (if-let [auth-header (get-in request [:headers "authorization"])]
      (let [auth-fields (auth-fields->map auth-header)]
        (if (= (:s2s auth-fields) "step2")
          (auth-step-2 auth-fields)
          (auth-step-1 queue-backend auth-fields)))
      (d/success-deferred
       {:status 401
        :body "Authorization header required"}))))
