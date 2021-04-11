(ns unifydb.query-test
  (:require [clojure.test :refer [deftest is testing]]
            [unifydb.messagequeue.memory :as memqueue]
            [unifydb.query :as query]
            [unifydb.service :as service]
            [unifydb.storage :as store]
            [unifydb.kvstore.memory :as memstore]
            [unifydb.util :as util]))

(deftest simple-matching
  (let [facts [[#unifydb/id 1 :name "Ben Bitdiddle" #unifydb/id 0 true]
               [#unifydb/id 1 :job [:computer :wizard] #unifydb/id 0 true]
               [#unifydb/id 1 :salary 60000 #unifydb/id 1 true]
               [#unifydb/id 2 :name "Alyssa P. Hacker" #unifydb/id 1 true]
               [#unifydb/id 2 :job [:computer :programmer] #unifydb/id 2 true]
               [#unifydb/id 2 :salary 40000 #unifydb/id 2 true]
               [#unifydb/id 2 :supervisor #unifydb/id 1 #unifydb/id 2 true]
               [#unifydb/id 1 :address [:slumerville [:ridge :road] 10] #unifydb/id 2 true]
               [#unifydb/id 2 :address [:cambridge [:mass :ave] 78] #unifydb/id 2 true]
               [#unifydb/id 2 :address [:cambridge [:mass :ave] 78] #unifydb/id 3 false]
               [#unifydb/id 3 :address [:slumerville [:davis :square] 42] #unifydb/id 4 true]]
        storage-backend (store/store-facts! (store/new! (memstore/new)) facts)
        queue-backend (memqueue/new)
        query-service (query/new queue-backend storage-backend)
        db-latest {:tx-id #unifydb/id 4}
        db-tx-2 {:tx-id #unifydb/id 2}]
    (try
      (service/start! query-service)
      (doseq [{:keys [query db expected]}
              [{:query '{:find [?e]
                         :where [[?e :name "Ben Bitdiddle"]]}
                :db db-latest
                :expected '[[#unifydb/id 1]]}
               {:query '{:find [?e ?what]
                         :where [[?e :job [:computer ?what]]]}
                :db db-latest
                :expected '[[#unifydb/id 2 :programmer]
                            [#unifydb/id 1 :wizard]]}
               {:query '{:find [?town ?road-and-number]
                         :where [[#unifydb/id 1 :address [?town & ?road-and-number]]]}
                :db db-latest
                :expected '[[:slumerville [[:ridge :road] 10]]]}
               {:query '{:find [?town ?road-and-number]
                         :where [[#unifydb/id 2 :address [?town & ?road-and-number]]]}
                :db db-tx-2
                :expected '[[:cambridge [[:mass :ave] 78]]]}
               {:query '{:find [?town ?road-and-number]
                         :where [[#unifydb/id 2 :address [?town & ?road-and-number]]]}
                :db db-latest
                :expected '[]}
               {:query '{:find [?e]
                         :where [[?e :job [:computer _]]]}
                :db db-latest
                :expected '[[#unifydb/id 2] [#unifydb/id 1]]}
               {:query '{:find [?address]
                         :where [[_ :address [:slumerville & ?address]]]}
                :db db-latest
                :expected '[[[[:davis :square] 42]] [[[:ridge :road] 10]]]}]]
        (testing (format "query: %s\ndb: %s" query db)
          (is (= expected
                 (:results @(util/query queue-backend db query))))))
      (finally
        (service/stop! query-service)))))

(deftest compound-queries
  (let [facts [[#unifydb/id 1 :name "Ben Bitdiddle" #unifydb/id 0 true]
               [#unifydb/id 1 :job [:computer :wizard] #unifydb/id 0 true]
               [#unifydb/id 1 :salary 60000 #unifydb/id 1 true]
               [#unifydb/id 2 :name "Alyssa P. Hacker" #unifydb/id 1 true]
               [#unifydb/id 2 :job [:computer :programmer] #unifydb/id 2 true]
               [#unifydb/id 2 :salary 40000 #unifydb/id 2 true]
               [#unifydb/id 2 :supervisor #unifydb/id 1 #unifydb/id 2 true]
               [#unifydb/id 1 :address [:slumerville [:ridge :road] 10] #unifydb/id 2 true]
               [#unifydb/id 2 :address [:cambridge [:mass :ave] 78] #unifydb/id 2 true]
               [#unifydb/id 2 :address [:cambridge [:mass :ave] 78] #unifydb/id 3 false]
               [#unifydb/id 3 :address [:slumerville [:davis :square] 42] #unifydb/id 4 true]]
        storage-backend (store/store-facts! (store/new! (memstore/new)) facts)
        queue-backend (memqueue/new)
        query-service (query/new queue-backend storage-backend)
        db {:tx-id #unifydb/id 4}]
    (try
      (service/start! query-service)
      (doseq [{:keys [query db expected]}
              [{:query '{:find [?e ?what]
                         :where [[:and
                                  [?e :job [:computer ?what]]
                                  [?e :salary 60000]]]}
                :db db
                :expected '[[#unifydb/id 1 :wizard]]}
               {:query '{:find [?e ?what]
                         :where [[?e :job [:computer ?what]]
                                 [?e :salary 60000]]}
                :db db
                :expected '[[#unifydb/id 1 :wizard]]}
               {:query '{:find [?e]
                         :where [[:or
                                  [?e :job [:computer :wizard]]
                                  [?e :job [:computer :programmer]]]]}
                :db db
                :expected '[[#unifydb/id 1] [#unifydb/id 2]]}
               {:query '{:find [?e ?what]
                         :where [[:and
                                  [?e :job [:computer ?what]]
                                  [:not [?e :salary 60000]]]]}
                :db db
                :expected '[[#unifydb/id 2 :programmer]]}]]
        (testing (str query)
          (is (= (:results @(util/query queue-backend db query))
                 expected))))
      (finally
        (service/stop! query-service)))))

(deftest rules
  (let [facts [[#unifydb/id 1 :name "Ben Bitdiddle" #unifydb/id 0 true]
               [#unifydb/id 1 :job [:computer :wizard] #unifydb/id 0 true]
               [#unifydb/id 1 :salary 60000 #unifydb/id 1 true]
               [#unifydb/id 2 :name "Alyssa P. Hacker" #unifydb/id 1 true]
               [#unifydb/id 2 :job [:computer :programmer] #unifydb/id 2 true]
               [#unifydb/id 2 :salary 40000 #unifydb/id 2 true]
               [#unifydb/id 2 :supervisor #unifydb/id 1 #unifydb/id 2 true]
               [#unifydb/id 1 :address [:slumerville [:ridge :road] 10] #unifydb/id 2 true]
               [#unifydb/id 2 :address [:cambridge [:mass :ave] 78] #unifydb/id 2 true]
               [#unifydb/id 2 :address [:cambridge [:mass :ave] 78] #unifydb/id 3 false]
               [#unifydb/id 3 :address [:slumerville [:davis :square] 42] #unifydb/id 4 true]]
        storage-backend (store/store-facts! (store/new! (memstore/new)) facts)
        queue-backend (memqueue/new)
        query-service (query/new queue-backend storage-backend)
        db {:tx-id #unifydb/id 4}]
    (try
      (service/start! query-service)
      (doseq [{:keys [query db expected]}
              [{:query '{:find [?who]
                         :where [(:lives-near ?who #unifydb/id 1)]
                         :rules [[(:lives-near ?person1 ?person2)
                                  [?person1 :address [?town & _]]
                                  [?person2 :address [?town & _]]
                                  [:not (:same ?person1 ?person2)]]
                                 [(:same ?x ?x)]]}
                :db db
                :expected '[[#unifydb/id 3]]}]]
        (testing (format "query: %s\ndb: %s" query db)
          (is (= expected
                 (:results @(util/query queue-backend db query))))))
      (finally
        (service/stop! query-service)))))

(deftest cardinality
  (let [facts [[#unifydb/id 1 :unifydb/schema :favorite-colors #unifydb/id 0 true]
               [#unifydb/id 1 :unifydb/cardinality :cardinality/many #unifydb/id 0 true]
               [#unifydb/id 2 :name "Bob" #unifydb/id 0 true]
               [#unifydb/id 2 :favorite-colors "red" #unifydb/id 0 true]
               [#unifydb/id 2 :favorite-colors "green" #unifydb/id 0 true]
               [#unifydb/id 2 :favorite-colors "blue" #unifydb/id 0 true]
               [#unifydb/id 2 :favorite-colors "blue" #unifydb/id 1 false]
               [#unifydb/id 3 :name "Emily" #unifydb/id 2 true]
               [#unifydb/id 3 :favorite-colors "yellow" #unifydb/id 2 true]
               [#unifydb/id 4 :name "Joe" #unifydb/id 3 true]
               [#unifydb/id 4 :lucky-number 7 #unifydb/id 3 true]
               [#unifydb/id 4 :lucky-number 9 #unifydb/id 4 true]
               [#unifydb/id 4 :lucky-number 9 #unifydb/id 5 false]]
        storage-backend (store/store-facts! (store/new! (memstore/new)) facts)
        queue-backend (memqueue/new)
        query-service (query/new queue-backend storage-backend)]
    (try
      (service/start! query-service)
      (testing "Cardinality many"
        (is (= [[#unifydb/id 2 "red"]
                [#unifydb/id 2 "green"]
                [#unifydb/id 3 "yellow"]]
               (:results @(util/query queue-backend
                                      {:tx-id #unifydb/id 5}
                                      '{:find [?ent ?color]
                                        :where [[?ent :favorite-colors ?color]]})))))
      (finally
        (service/stop! query-service)))))

(deftest operators
  (let [facts [[#unifydb/id 1 :name "Ben Bitdiddle" #unifydb/id 0 true]
               [#unifydb/id 1 :job [:computer :wizard] #unifydb/id 0 true]
               [#unifydb/id 1 :salary 60000 #unifydb/id 1 true]
               [#unifydb/id 2 :name "Alyssa P. Hacker" #unifydb/id 1 true]
               [#unifydb/id 2 :job [:computer :programmer] #unifydb/id 2 true]
               [#unifydb/id 2 :salary 40000 #unifydb/id 2 true]
               [#unifydb/id 2 :supervisor #unifydb/id 1 #unifydb/id 2 true]
               [#unifydb/id 1 :address [:slumerville [:ridge :road] 10] #unifydb/id 2 true]
               [#unifydb/id 2 :address [:cambridge [:mass :ave] 78] #unifydb/id 2 true]
               [#unifydb/id 2 :address [:cambridge [:mass :ave] 78] #unifydb/id 3 false]
               [#unifydb/id 3 :address [:slumerville [:davis :square] 42] #unifydb/id 4 true]]
        storage-backend (store/store-facts! (store/new! (memstore/new)) facts)
        queue-backend (memqueue/new)
        query-service (query/new queue-backend storage-backend)]
    (try
      (service/start! query-service)
      (testing "Core functions"
        (is (= [[#unifydb/id 2]]
               (:results
                @(util/query queue-backend
                             {:tx-id #unifydb/id 4}
                             '{:find [?e]
                               :where [[?e :salary ?s]
                                       [?ben :name "Ben Bitdiddle"]
                                       [?ben :salary ?bs]
                                       [(< ?s ?bs)]]}))))
        (is (= [[#unifydb/id 1]]
               (:results
                @(util/query queue-backend
                             {:tx-id #unifydb/id 4}
                             '{:find [?e]
                               :where [[?e :salary ?s]
                                       [(< 50000 ?s 70000)]]}))))
        (is (= [[#unifydb/id 2] [#unifydb/id 1]]
               (:results
                @(util/query queue-backend
                             {:tx-id #unifydb/id 4}
                             '{:find [?e]
                               :where [[?e :job ?job]
                                       [(some #{:computer} ?job)]]}))))
        (is (= [[#unifydb/id 2]]
               (:results
                @(util/query queue-backend
                             {:tx-id #unifydb/id 4}
                             '{:find [?e]
                               :where [[?e :job ?job]
                                       [(some #{:computer} ?job)]
                                       [?e :name ?name]
                                       [(!= "Ben Bitdiddle" ?name)]]}))))
        (is (= {:code :unbound-variable
                :variable "joob"
                :message "Unbound variable joob"}
               (:error
                @(util/query queue-backend
                             {:tx-id #unifydb/id 4}
                             '{:find [?e]
                               :where [[?e :job ?job]
                                       [(some #{:computer} ?joob)]
                                       [?e :name ?name]
                                       [(!= "Ben Bitdiddle" ?name)]]})))))
      (is (= {:code :unknown-predicate
              :predicate "foo"
              :message "Unknown predicate foo"}
             (:error
              @(util/query queue-backend
                           {:tx-id #unifydb/id 4}
                           '{:find [?e]
                             :where [[?e :salary ?s]
                                     [(foo 50000 ?s 70000)]]}))))
      (finally
        (service/stop! query-service)))))

(deftest parameterization
  (let [facts [[#unifydb/id 1 :name "Ben Bitdiddle" #unifydb/id 0 true]
               [#unifydb/id 1 :job [:computer :wizard] #unifydb/id 0 true]
               [#unifydb/id 1 :salary 60000 #unifydb/id 1 true]
               [#unifydb/id 2 :name "Alyssa P. Hacker" #unifydb/id 1 true]
               [#unifydb/id 2 :job [:computer :programmer] #unifydb/id 2 true]
               [#unifydb/id 2 :salary 40000 #unifydb/id 2 true]
               [#unifydb/id 2 :supervisor #unifydb/id 1 #unifydb/id 2 true]
               [#unifydb/id 1 :address [:slumerville [:ridge :road] 10] #unifydb/id 2 true]
               [#unifydb/id 2 :address [:cambridge [:mass :ave] 78] #unifydb/id 2 true]
               [#unifydb/id 2 :address [:cambridge [:mass :ave] 78] #unifydb/id 3 false]
               [#unifydb/id 3 :address [:slumerville [:davis :square] 42] #unifydb/id 4 true]]
        storage-backend (store/store-facts! (store/new! (memstore/new)) facts)
        queue-backend (memqueue/new)
        query-service (query/new queue-backend storage-backend)
        db-latest {:tx-id :latest}]
    (try
      (service/start! query-service)
      (doseq [{:keys [query db expected]}
              [{:query '{:find [?e]
                         :where [[?e :name ?name]]
                         :bind {name "Ben Bitdiddle"}}
                :db db-latest
                :expected '[[#unifydb/id 1]]}]]
        (testing (str query)
          (is (= expected
                 (:results @(util/query queue-backend db query))))))
      (finally
        (service/stop! query-service)))))

(deftest historical-queries
  (let [facts [[#unifydb/id 0 :doc "First transaction" #unifydb/id 0 true]
               [#unifydb/id 1 :address "78 Mass Ave, Cambridge MA" #unifydb/id 0 true]
               [#unifydb/id 2 :doc "Second transaction" #unifydb/id 2 true]
               [#unifydb/id 1 :address "78 Mass Ave, Cambridge MA" #unifydb/id 2 false]
               [#unifydb/id 1 :address "10 Ridge Road, Slumerville MA" #unifydb/id 2 true]]
        storage-backend (store/store-facts! (store/new! (memstore/new)) facts)
        queue-backend (memqueue/new)
        query-service (query/new queue-backend storage-backend)]
    (try
      (service/start! query-service)
      (doseq [{:keys [query db expected]}
              [{:query '{:find [?tx-id ?address ?added ?doc]
                         :where [[_ :address ?address ?tx-id ?added]
                                 [?tx-id :doc ?doc]]}
                :db {:tx-id :latest
                     :historical true}
                :expected [[#unifydb/id 2 "78 Mass Ave, Cambridge MA" false "Second transaction"]
                           [#unifydb/id 0 "78 Mass Ave, Cambridge MA" true "First transaction"]
                           [#unifydb/id 2 "10 Ridge Road, Slumerville MA" true "Second transaction"]]}
               {:query '{:find [?tx-id ?address ?added ?doc]
                         :where [[_ :address ?address ?tx-id ?added]
                                 [?tx-id :doc ?doc]]}
                :db {:tx-id #unifydb/id 0
                     :historical true}
                :expected [[#unifydb/id 0 "78 Mass Ave, Cambridge MA" true "First transaction"]]}]]
        (testing (str query)
          (is (= expected (:results @(util/query queue-backend db query))))))
      (finally
        (service/stop! query-service)))))

(deftest aggregation
  (let [facts [[#unifydb/id 1 :employee/name "Ben Bitdiddle" #unifydb/id 0 true]
               [#unifydb/id 1 :employee/age 45 #unifydb/id 0 true]
               [#unifydb/id 2 :employee/name "Alyssa P. Hacker" #unifydb/id 0 true]
               [#unifydb/id 2 :employee/age 32 #unifydb/id 0 true]
               [#unifydb/id 3 :employee/name "Oliver Warbucks" #unifydb/id 0 true]
               [#unifydb/id 3 :employee/age 56 #unifydb/id 0 true]
               [#unifydb/id 4 :employee/name "Lem E. Tweakit" #unifydb/id 0 true]
               [#unifydb/id 4 :employee/age 32 #unifydb/id 0 true]]
        storage-backend (store/store-facts! (store/new! (memstore/new)) facts)
        queue-backend (memqueue/new)
        query-service (query/new queue-backend storage-backend)]
    (try
      (service/start! query-service)
      (doseq [{:keys [query db expected expected-error]}
              [{:query '{:find [(foo ?age)]
                         :where [[_ :employee/age ?age]]}
                :db {:tx-id :latest}
                :expected-error {:message "Unknown aggregation expression foo"
                                 :code :unknown-aggregation
                                 :aggregation "foo"}}]]
        (testing (str query)
          (if expected-error
            (is (= expected-error (:error @(util/query queue-backend db query))))
            (is (= expected (:results @(util/query queue-backend db query)))))))
      (finally
        (service/stop! query-service)))))
