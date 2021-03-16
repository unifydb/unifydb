(ns unifydb.transact-test
  (:require [clojure.test :refer [deftest testing is]]
            [unifydb.facts :refer [fact-entity
                                   fact-value
                                   fact-attribute]]
            [unifydb.kvstore.memory :as memstore]
            [unifydb.messagequeue.memory :as memq]
            [unifydb.service :as service]
            [unifydb.storage :as storage]
            [unifydb.transact :as t]))

(deftest transact-test
  (let [queue-backend (memq/new)
        transact-service (t/new queue-backend (storage/new! (memstore/new)))]
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

(deftest transact-user-test
  (let [queue-backend (memq/new)
        store (storage/new! (memstore/new))
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
