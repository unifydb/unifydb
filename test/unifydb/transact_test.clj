(ns unifydb.transact-test
  (:require [clojure.test :refer [deftest testing is]]
            [unifydb.facts :refer [fact-entity
                                   fact-value
                                   fact-attribute]]
            [unifydb.kvstore :as kvstore]
            [unifydb.kvstore.memory :as memstore]
            [unifydb.messagequeue.memory :as memq]
            [unifydb.service :as service]
            [unifydb.storage :as storage]
            [unifydb.transact :as t]))

(deftest transact-test
  (let [queue-backend (memq/new)
        transact-service (t/new queue-backend
                                (storage/new!
                                 (kvstore/new
                                  (memstore/new))))]
    (try
      (service/start! transact-service)
      (let [tx-data [[:unifydb/add "ben" :name "Ben Bitdiddle"]
                     [:unifydb/add "ben" :salary 60000]
                     [:unifydb/add "alyssa" :name "Alyssa P. Hacker"]
                     [:unifydb/add "alyssa" :salary 40000]
                     [:unifydb/add "alyssa" :supervisor "ben"]]
            tx-report (:tx-report @(t/transact queue-backend tx-data))
            tempids (:tempids tx-report)
            facts (:tx-data tx-report)
            db-after (:db-after tx-report)]
        (testing "Resolving temporary IDs"
          (is (= ["ben" "alyssa" "unifydb.tx"] (keys tempids)))
          (is (= (get tempids "ben") (fact-entity (first facts))))
          (is (= (get tempids "alyssa") (fact-entity (nth facts 2))))
          (is (= (fact-entity (first facts)) (fact-entity (second facts))))
          (is (= (fact-entity (nth facts 2)) (fact-entity (nth facts 3))))
          (is (= (fact-entity (nth facts 2)) (fact-entity (nth facts 4))))
          (is (= (fact-entity (first facts)) (fact-value (nth facts 4)))))
        (testing "Adding transaction metadata"
          (is (= (fact-entity (last facts)) (get tempids "unifydb.tx")))
          (is (= (fact-attribute (last facts)) :unifydb/txInstant))
          (is (= java.lang.Long (type (fact-value (last facts)))))
          (is (>= (System/currentTimeMillis) (fact-value (last facts)))))
        (testing "Returning a new DB"
          (is (= (get tempids "unifydb.tx") (:tx-id db-after)))))
      (finally
        (service/stop! transact-service)))))

(deftest transact-map-form-test
  (let [queue-backend (memq/new)
        transact-service (t/new queue-backend
                                (storage/new!
                                 (kvstore/new
                                  (memstore/new))))]
    (try
      (service/start! transact-service)
      (let [tx-data [{:unifydb/id "ben"
                      :name "Ben Bitdiddle"
                      :salary 60000}
                     {:unifydb/id "alyssa"
                      :name "Alyssa P. Hacker"
                      :salary 40000
                      :supervisor "ben"}]
            tx-report (:tx-report @(t/transact queue-backend tx-data))
            tempids (:tempids tx-report)
            facts (:tx-data tx-report)
            db-after (:db-after tx-report)]
        (testing "Resolving temporary IDs"
          (is (= ["ben" "alyssa" "unifydb.tx"] (keys tempids)))
          (is (= (get tempids "ben") (fact-entity (first facts))))
          (is (= (get tempids "alyssa") (fact-entity (nth facts 2))))
          (is (= (fact-entity (first facts)) (fact-entity (second facts))))
          (is (= (fact-entity (nth facts 2)) (fact-entity (nth facts 3))))
          (is (= (fact-entity (nth facts 2)) (fact-entity (nth facts 4))))
          (is (= (fact-entity (first facts)) (fact-value (nth facts 4)))))
        (testing "Adding transaction metadata"
          (is (= (fact-entity (last facts)) (get tempids "unifydb.tx")))
          (is (= (fact-attribute (last facts)) :unifydb/txInstant))
          (is (= java.lang.Long (type (fact-value (last facts)))))
          (is (>= (System/currentTimeMillis) (fact-value (last facts)))))
        (testing "Returning a new DB"
          (is (= (get tempids "unifydb.tx") (:tx-id db-after)))))
      (finally
        (service/stop! transact-service)))))

(deftest expand-map-forms-test
  (let [test-cases [{:name "Basic example"
                     :map-form {:unifydb/id "foo"
                                :foo "bar"
                                :baz "qux"}
                     :add-forms [[:unifydb/add "foo" :foo "bar"]
                                 [:unifydb/add "foo" :baz "qux"]]}
                    {:name "Nested map"
                     :map-form {:unifydb/id "foo"
                                :bar {:unifydb/id "bar"
                                      :baz "qux"}}
                     :add-forms [[:unifydb/add "foo" :bar "bar"]
                                 [:unifydb/add "bar" :baz "qux"]]}
                    {:name "List value"
                     :map-form {:unifydb/id "order1"
                                :customer-id 1234
                                :line-items [{:unifydb/id "li1" :cost 100}
                                             {:unifydb/id "li2" :cost 200}]}
                     :add-forms [[:unifydb/add "order1" :customer-id 1234]
                                 [:unifydb/add "order1" :line-items "li1"]
                                 [:unifydb/add "li1" :cost 100]
                                 [:unifydb/add "order1" :line-items "li2"]
                                 [:unifydb/add "li2" :cost 200]]}
                    {:name "Non-map lists"
                     :map-form {:unifydb/id "foo"
                                :bar [1 2 3]}
                     :add-forms [[:unifydb/add "foo" :bar [1 2 3]]]}]]
    (doseq [{:keys [name map-form add-forms]} test-cases]
      (testing name
        (is (= (t/map-form->add-forms map-form) add-forms))))))

(deftest transact-user-test
  (let [queue-backend (memq/new)
        store (storage/new! (kvstore/new (memstore/new)))
        transact-service (t/new queue-backend store)]
    (try
      (service/start! transact-service)
      (testing "Transact user - happy path"
        (let [tx-data [[:unifydb/add "my-user" :unifydb/username "user"]
                       [:unifydb/add "my-user" :unifydb/password "pencil"]]
              tx-report (:tx-report @(t/transact queue-backend tx-data))
              facts (:tx-data tx-report)]
          (is (= (count facts) 3))
          (is (not= "pencil" (fact-value
                              (first
                               (filter (comp #{:unifydb/password}
                                             fact-attribute)
                                       facts)))))
          (is (= "user" (fact-value
                         (first
                          (filter
                           (comp #{:unifydb/username}
                                 fact-attribute)
                           facts)))))
          (is (= (fact-value (first facts)) "user"))))
      (finally
        (service/stop! transact-service)))))
