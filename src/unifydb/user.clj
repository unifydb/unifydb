(ns unifydb.user
  (:require [unifydb.scram :as scram]))

(defn make-user
  "Makes a new user record. Does not
  persist to the database."
  [username password]
  (let [salt (scram/salt)
        i 4096
        salted-password (scram/pbk-df2-hmac-sha256
                         (.getBytes (scram/normalize password))
                         salt
                         i)
        server-key (scram/hmac salted-password (.getBytes "Server Key"))
        client-key (scram/hmac salted-password (.getBytes "Client Key"))
        stored-key (scram/hash-sha256 client-key)]
    {:unifydb/username username
     :unifydb/salt (scram/encode salt)
     :unifydb/i i
     :unifydb/server-key server-key
     :unifydb/stored-key stored-key}))
