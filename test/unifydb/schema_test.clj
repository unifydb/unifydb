(ns unifydb.schema-test
  (:require [clojure.test :refer [deftest testing is]]
            [unifydb.kvstore :as kvstore]
            [unifydb.kvstore.memory :as memstore]
            [unifydb.messagequeue.memory :as memq]
            [unifydb.query :as query]
            [unifydb.schema :as schema]
            [unifydb.service :as service]
            [unifydb.storage :as store]
            [unifydb.transact :as transact]))

(deftest test-get-schema
  (testing "get-schemas"
    (let [queue-backend (memq/new)
          storage-backend (store/new! (kvstore/new (memstore/new)))
          transact-service (transact/new queue-backend storage-backend)
          query-service (query/new queue-backend storage-backend)
          tx-data [[:unifydb/add "foo" :unifydb/schema :foo]
                   [:unifydb/add "foo" :unifydb/cardinality :cardinality/many]
                   [:unifydb/add "bar" :unifydb/schema :bar]
                   [:unifydb/add "bar" :unifydb/unique :unique/upsert]]]
      (try
        (service/start! transact-service)
        (service/start! query-service)
        @(transact/transact queue-backend tx-data)
        (is (= @(schema/get-schemas queue-backend #unifydb/id 3 [:foo :bar :baz])
               {:foo {:unifydb/cardinality :cardinality/many
                      :unifydb/schema :foo}
                :bar {:unifydb/unique :unique/upsert
                      :unifydb/schema :bar}}))
        (finally
          (service/stop! transact-service)
          (service/stop! query-service))))))
