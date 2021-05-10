(ns unifydb.kvstore.jdbc-test
  (:require [clojure.test :as t]
            [unifydb.kvstore :as kvstore]
            [unifydb.kvstore.jdbc :as jdbcstore]))

(t/deftest jdbc-kvstore-test
  (with-open [backend (jdbcstore/new! "jdbc:sqlite::memory:")]
    (let [kvstore (kvstore/new backend)]
      (t/is (false? (kvstore/contains? kvstore "foo")))

      (kvstore/assoc! kvstore "foo" {:foo "bar"})
      (t/is (true? (kvstore/contains? kvstore "foo")))
      (t/is (= {:foo "bar"} (kvstore/get kvstore "foo")))

      (kvstore/assoc! kvstore "foo" {:foo "baz"})
      (t/is (= {:foo "baz"} (kvstore/get kvstore "foo")))

      (kvstore/dissoc! kvstore "foo")
      (t/is (false? (kvstore/contains? kvstore "foo")))
      (t/is (nil? (kvstore/get kvstore "foo"))))))
