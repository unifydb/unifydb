(ns unifydb.kvstore.jdbc
  "An implementation of the KV store backend based on JDBC."
  (:require [next.jdbc :as jdbc]
            [taoensso.nippy :as nippy]
            [unifydb.kvstore :as kvstore])
  (:refer-clojure :exclude [contains?]))

(defrecord JDBCKeyValueStore [connection]
  ;; Closing the store closes the underlying connection
  java.io.Closeable
  (close [self] (.close (:connection self)))

  ;; Actual KV store interface methods
  kvstore/IKeyValueStore
  (store-get
    [self key]
    (when-let [row (jdbc/execute-one! (:connection self)
                                      ["SELECT value FROM unifydb_kvs WHERE key LIKE ?"
                                       key])]
      (nippy/thaw (:unifydb_kvs/value row))))
  (assoc! [self key val]
    (jdbc/execute! (:connection self) ["BEGIN TRANSACTION"])
    (if (kvstore/contains? self key)
      (jdbc/execute! (:connection self)
                     ["UPDATE unifydb_kvs SET value = ? WHERE key LIKE ?" (nippy/freeze val) key])
      (jdbc/execute! (:connection self)
                     ["INSERT INTO unifydb_kvs VALUES (?, ?)" key (nippy/freeze val)]))
    (jdbc/execute! (:connection self) ["COMMIT"])
    self)
  (dissoc!
    [self key]
    (jdbc/execute! (:connection self)
                   ["DELETE FROM unifydb_kvs WHERE key LIKE ?"
                    key])
    self)
  (contains?
    [self key]
    (> (:count (jdbc/execute-one! (:connection self)
                                  ["SELECT COUNT(*) AS count FROM unifydb_kvs WHERE key LIKE ?"
                                   key]))
       0)))

(defn new!
  "Instantiate a new JDBCKeyValueStore using the `connection-uri`,
  creating the unifydb_kvs table if it doesn't exist."
  [connection-uri]
  (let [conn (jdbc/get-connection connection-uri)]
    (jdbc/execute! conn ["
CREATE TABLE IF NOT EXISTS unifydb_kvs (key VARCHAR(255) PRIMARY KEY, value BLOB NOT NULL)
"])
    (->JDBCKeyValueStore conn)))
