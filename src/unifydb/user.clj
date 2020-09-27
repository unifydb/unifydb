(ns unifydb.user
  (:require [buddy.core.bytes :as bytes]
            [buddy.core.codecs :as codecs]
            [buddy.core.codecs.base64 :as base64]
            [buddy.core.hash :as hash]
            [buddy.core.nonce :as nonce]
            [manifold.deferred :as d]
            [unifydb.util :as util]))

(defn make-user
  "Makes a new user record.
  Does not persist to the database."
  [username password]
  (let [salt (nonce/random-bytes 64)
        hashed-pw (hash/sha512 (bytes/concat (codecs/str->bytes password)
                                             salt))]
    {:unifydb/username username
     :unifydb/password (codecs/bytes->str (base64/encode hashed-pw))
     :unifydb/salt (codecs/bytes->str (base64/encode salt))}))

(defn get-user!
  "Gets the user record denoted by `username`, returning a deferred"
  [queue-backend db username]
  (d/chain (util/query queue-backend
                       db
                       '{:find [?password ?salt]
                         :where [[?e :unifydb/username ?username]
                                 [?e :unifydb/password ?password]
                                 [?e :unifydb/salt ?salt]]}
                       {:username username})
           :results
           first
           (fn [[password salt]]
             (when (and password salt)
               {:unifydb/username username
                :unifydb/password password
                :unifydb/salt salt}))))
