(ns unifydb.user
  (:import [java.security SecureRandom]))

(defn make-user
  "Makes a new user record.
  Does not persist to the database.
  For deterministic output, pass a pre-seeded Random instance."
  ([username password random]
   :TODO)
  ([username password] (make-user username password (SecureRandom.))))
