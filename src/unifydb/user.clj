(ns unifydb.user
  (:require [unifydb.scram :as s]
            [unifydb.transact :as t]
            [unifydb.cli.unifydb :as unifydb])
  (:import [java.security SecureRandom]))

(defn make-user
  "Makes a new user record.
  Does not persist to the database.
  For deterministic output, pass a pre-seeded Random instance."
  ([username password random]
   (let [salt (s/salt random)
         i 4096
         salted-password (s/pbk-df2-hmac-sha256
                          (.getBytes (s/normalize password))
                          salt
                          i)
         server-key (s/hmac salted-password (.getBytes "Server Key"))
         client-key (s/hmac salted-password (.getBytes "Client Key"))
         stored-key (s/hash-sha256 client-key)]
     {:unifydb/username username
      :unifydb/salt (s/encode salt)
      :unifydb/i i
      :unifydb/server-key (s/encode server-key)
      :unifydb/stored-key (s/encode stored-key)}))
  ([username password] (make-user username password (SecureRandom.))))

(defn new-user!
  "Persists a new user record to the database.
  Returns a Manifold deferred with the tx-report.
  For deterministic output, pass a pre-seeded Random instance."
  ([queue-backend username password random]
   (let [{:unifydb/keys [username salt i server-key stored-key]}
         (make-user username password random)
         ;; TODO when the transact engine supports using maps directly,
         ;;      refactor this to just use the user map
         ;; TODO when unique schema is supported, make :unifydb/username unique
         tx-data [[:unifydb/add "unifydb/user" :unifydb/username username]
                  [:unifydb/add "unifydb/user" :unifydb/salt salt]
                  [:unifydb/add "unifydb/user" :unifydb/i i]
                  [:unifydb/add "unifydb/user" :unifydb/server-key server-key]
                  [:unifydb/add "unifydb/user" :unifydb/stored-key stored-key]]]
     (t/transact queue-backend tx-data)))
  ([queue-backend username password]
   (new-user! queue-backend username password (SecureRandom.))))
