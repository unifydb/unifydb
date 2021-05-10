(ns unifydb.kvstore.jdbc-test
  (:require [clojure.test :as t]
            [unifydb.kvstore.backend :as kvstore-backend]
            [unifydb.kvstore.jdbc :as jdbcstore]))

(t/deftest jdbc-kvstore-test
  (with-open [kvstore (jdbcstore/new! "jdbc:sqlite::memory:")]
    (t/is (false? (kvstore-backend/contains-all? kvstore ["foo"])))

    (kvstore-backend/write-all! kvstore [[:assoc! "foo" {:foo "bar"}]])
    (t/is (true? (kvstore-backend/contains-all? kvstore ["foo"])))
    (t/is (= [{:foo "bar"}] (kvstore-backend/get-all kvstore ["foo"])))

    (kvstore-backend/write-all! kvstore [[:assoc! "foo" {:foo "baz"}]])
    (t/is (= [{:foo "baz"}] (kvstore-backend/get-all kvstore ["foo"])))

    (kvstore-backend/write-all! kvstore [[:dissoc! "foo"]])
    (t/is (false? (kvstore-backend/contains-all? kvstore ["foo"])))
    (t/is (= [] (kvstore-backend/get-all kvstore ["foo"])))))
