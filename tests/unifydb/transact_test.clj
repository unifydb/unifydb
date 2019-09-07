(ns unifydb.transact-test
  (:require [clojure.test :refer [deftest testing is]]
            [unifydb.facts :refer [fact-entity fact-tx-id fact-value fact-attribute]]
            [unifydb.storage.memory :as memdb]
            [unifydb.transact :as t]))

(deftest transact-test
  (let [conn {:storage-backend (memdb/new)}
        tx-data [[:unifydb/add "ben" :name "Ben Bitdiddle"]
                 [:unifydb/add "ben" :salary 60000]
                 [:unifydb/add "alyssa" :name "Alyssa P. Hacker"]
                 [:unifydb/add "alyssa" :salary 40000]
                 [:unifydb/add "alyssa" :supervisor "ben"]]
        tx-report @(t/transact conn tx-data)
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
      (is (= (get tempids "unifydb.tx") (:tx-id db-after))))))
