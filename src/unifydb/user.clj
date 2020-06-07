(ns unifydb.user
  (:require [unifydb.scram :as s])
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
