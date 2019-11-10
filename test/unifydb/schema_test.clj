(ns unifydb.schema-test
  (:require [clojure.test :refer [deftest testing is]]
            [unifydb.messagequeue.memory :as memq]
            [unifydb.query :as query]
            [unifydb.schema :as schema]
            [unifydb.storage.memory :as memstore]
            [unifydb.service :as service]
            [unifydb.transact :as transact]))

(defmacro def-schema-test [name & body]
  `(deftest ~name
     (let [q# {:type :memory}
           store# {:type :memory}
           transact-service# (transact/new q# store#)
           query-service# (query/new q# store#)]
       (try
         (service/start! transact-service#)
         (service/start! query-service#)
         ~@body
         (finally
           (service/stop! transact-service#)
           (service/stop! query-service#)
           (memq/reset-state!)
           (memstore/empty-store!))))))

(def-schema-test test-get-schema
  (testing "get-schemas"
   (let [tx-data [[:unifydb/add "foo" :unifydb/schema :foo]
                  [:unifydb/add "foo" :unifydb/cardinality :cardinality/many]
                  [:unifydb/add "bar" :unifydb/schema :bar]
                  [:unifydb/add "bar" :unifydb/unique :unique/upsert]]]
     @(transact/transact {:type :memory} tx-data)
     (is (= @(schema/get-schemas {:type :memory} [:foo :bar :baz] 3)
            {:foo {:unifydb/cardinality :cardinality/many
                   :unifydb/schema :foo}
             :bar {:unifydb/unique :unique/upsert
                   :unifydb/schema :bar}})))))
