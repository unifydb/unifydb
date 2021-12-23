(ns unifydb.kvstore.jdbc
  "An implementation of the KV store backend based on JDBC."
  (:require [clojure.string :as str]
            [manifold.deferred :as d]
            [next.jdbc :as jdbc]
            [taoensso.nippy :as nippy]
            [unifydb.kvstore.backend :as kvstore-backend])
  (:refer-clojure :exclude [contains?]))

(defn param-list
  "Generates a string consisting of an opening paren, a
  comma-separated sequence of '?'s of length `length`, and finally a
  closing paren."
  [length]
  (format "(%s)" (str/join ", " (repeat length "?"))))

(defn parameterized-with-coll
  "Returns a jdbc statement vector of `query` followed by all items in
  `coll`. `query` will be passed to (format) with a param-list of
  length (count coll) as its first argument."
  [query coll]
  (vec (apply list
              (format query (param-list (count coll)))
              coll)))

(defn jdbc-write-batch! [store operations]
  (let [assoc-kvs (->> operations
                       (filter #(= (first %) :assoc!))
                       (map rest))
        dissoc-keys (->> operations
                         (filter #(= (first %) :dissoc!))
                         (map (comp str first rest)))]
    (d/future
      (jdbc/with-transaction [tx (:connection store)]
        ;; TODO this would be more efficient if we batched it as 1 SQL
        ;; call for the updates and 1 for the inserts
        (doseq [[key val] assoc-kvs]
          ;; Dereferencing here doesn't make this synchronous since we
          ;; are in a future block
          (if @(kvstore-backend/contains-all? store [key])
            (jdbc/execute! tx
                           ["UPDATE unifydb_kvs SET value = ? WHERE key LIKE ?"
                            (nippy/freeze val)
                            (str key)])
            (jdbc/execute! tx
                           ["INSERT INTO unifydb_kvs VALUES (?, ?)"
                            (str key)
                            (nippy/freeze val)])))
        (when-not (empty? dissoc-keys)
          (jdbc/execute! tx
                         (parameterized-with-coll
                          "DELETE FROM unifydb_kvs WHERE key IN %s"
                          dissoc-keys))))
      ;; If the transaction fails, the future will have already failed here
      true)))

(defn jdbc-contains-batch? [store keys]
  (d/chain (d/future (jdbc/execute-one!
                      (:connection store)
                      (parameterized-with-coll
                       "SELECT COUNT(*) AS count FROM unifydb_kvs WHERE key IN %s"
                       (map str keys))))
           :count
           #(>= % (count keys))))

(defrecord JDBCKeyValueStore [connection]
  ;; Closing the store closes the underlying connection
  java.io.Closeable
  (close [self] (.close (:connection self)))

  ;; Actual KV store interface methods
  kvstore-backend/IKeyValueStoreBackend
  (get-all
    [self keys]
    (d/chain (d/future
               (jdbc/execute! (:connection self)
                              (parameterized-with-coll
                               "SELECT value FROM unifydb_kvs WHERE key IN %s"
                               (map str keys))))
             (partial map (comp nippy/thaw :unifydb_kvs/value))))

  (write-all! [self operations] (jdbc-write-batch! self operations))

  (contains-all? [self keys] (jdbc-contains-batch? self keys)))

(defn new!
  "Instantiate a new JDBCKeyValueStore using the `connection-uri`,
  creating the unifydb_kvs table if it doesn't exist."
  [connection-uri]
  (let [conn (jdbc/get-connection connection-uri)
        blob-type (if (re-find #"postgresql" connection-uri) "BYTEA" "BLOB")]
    (jdbc/execute! conn [(format "
CREATE TABLE IF NOT EXISTS unifydb_kvs (
    key VARCHAR(255) PRIMARY KEY, value %s NOT NULL
)
" blob-type)])
    (->JDBCKeyValueStore conn)))

(defn close!
  "Closes `kvstore`'s underlying database connection."
  [kvstore]
  (.close kvstore))
