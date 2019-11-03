(ns unifydb.server-test
  (:require [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.test :refer [deftest testing is]]
            [manifold.deferred :as d]
            [unifydb.messagequeue :as queue]
            [unifydb.messagequeue.memory :as memq]
            [unifydb.query :as query]
            [unifydb.server :as server]
            [unifydb.service :as service]
            [unifydb.storage.memory :as memstore]
            [unifydb.transact :as transact]))

(defmacro defservertest [name [req-fn store-name queue-name] txs & body]
  `(deftest ~name
    (let [~queue-name {:type :memory}
          ~store-name {:type :memory}
          query# (query/new ~queue-name)
          transact# (transact/new ~queue-name ~store-name)
          server# (server/new ~queue-name ~store-name)
          ~req-fn (fn [request#]
                    (let [app# (server/app (:state server#))
                          response# (app# request#)]
                      @response#))]
      (try
        (service/start! query#)
        (service/start! transact#)
        (service/start! server#)
        (doseq [tx# ~txs]
          (queue/publish ~queue-name :transact {:tx-data tx#}))
        (Thread/sleep 5)  ;; give the transaction time to process
        ~@body
        (finally
          (memstore/empty-store!)
          (memq/reset-state!)
          (service/stop! server#)
          (service/stop! transact#)
          (service/stop! query#))))))
(defservertest query-endpoint [make-request store queue-backend]
  '[[[:unifydb/add "ben" :name "Ben Bitdiddle"]
     [:unifydb/add "ben" :job ["computer" "wizard"]]
     [:unifydb/add "ben" :salary 60000]
     [:unifydb/add "alyssa" :name "Alyssa P. Hacker"]
     [:unifydb/add "alyssa" :job ["computer" "programmer"]]
     [:unifydb/add "alyssa" :salary 40000]
     [:unifydb/add "alyssa" :supervisor "ben"]]]
  (testing "/query (EDN)"
    (let [response (make-request
                    {:request-method :post
                     :uri "/query"
                     :headers {"content-type" "application/edn"
                               "accept" "application/edn"}
                     :body (prn-str
                            {:tx-id 3
                             :query '{:find [?name]
                                           :where [[?e :job ["computer" _]]
                                                   [?e :name ?name]]}})})]
      (is (= response '{:status 200
                        :headers {"Content-Type" "application/edn"}
                        :body "([\"Alyssa P. Hacker\"] [\"Ben Bitdiddle\"])"}))))
  (testing "/query (JSON)"
    (let [response (make-request {:request-method :post
                                  :uri "/query"
                                  :headers {"content-type" "application/json"
                                            "accept" "application/json"}
                                  :body (json/write-str
                                         {":tx-id" 3
                                          ":query" {":find" ["'?name"]
                                                    ":where" [["'?e" ":job" ["computer" "'_"]]
                                                              ["'?e" ":name" "'?name"]]}})})]
      (is (= response '{:status 200
                        :headers {"Content-Type" "application/json"}
                        :body "[[\"Alyssa P. Hacker\"],[\"Ben Bitdiddle\"]]"})))))

(defservertest transact-endpoint [make-request store queue-backend]
  []
  (testing "/transact (EDN)"
    (let [response (make-request
                    {:request-method :post
                     :uri "/transact"
                     :headers {"content-type" "application/edn"
                               "accept" "application/edn"}
                     :body (prn-str
                            {:tx-data [[:unifydb/add "ben" :name "Ben Bitdiddle"]
                                       [:unifydb/add "alyssa" :name "Alyssa P. Hacker"]
                                       [:unifydb/add "alyssa" :supervisor "ben"]]})})
          tx-instant (as-> (:body response) v
                       (edn/read-string v)
                       (:tx-data v)
                       (filter #(= :unifydb/txInstant (second %)) v)
                       (first v)
                       (nth v 2))]
      (is (= response {:status 200
                       :headers {"Content-Type" "application/edn"}
                       :body (pr-str {:db-after {:tx-id 3}
                                       :tx-data [[1 :name "Ben Bitdiddle" 3 true]
                                                 [2 :name "Alyssa P. Hacker" 3 true]
                                                 [2 :supervisor 1 3 true]
                                                 [3 :unifydb/txInstant tx-instant 3 true]]
                                       :tempids {"ben" 1
                                                 "alyssa" 2
                                                 "unifydb.tx" 3}})}))))
  (testing "/transact (JSON)"
    (let [response (make-request
                    {:request-method :post
                     :uri "/transact"
                     :headers {"content-type" "application/json"
                               "accept" "application/json"}
                     :body (json/write-str
                            {":tx-data" [[":unifydb/add" "ben" ":name" "Ben Bitdiddle"]
                                         [":unifydb/add" "alyssa" ":name" "Alyssa P. Hacker"]
                                         [":unifydb/add" "alyssa" ":supervisor" "ben"]]})})
          tx-instant (as-> (:body response) v
                       (json/read-str v)
                       (get v ":tx-data")
                       (filter #(= ":unifydb/txInstant" (second %)) v)
                       (first v)
                       (nth v 2))]
      (is (= response {:status 200
                       :headers {"Content-Type" "application/json"}
                       :body (json/write-str
                              {":db-after" {":tx-id" 6}
                               ":tx-data" [[4 ":name" "Ben Bitdiddle" 6 true]
                                           [5 ":name" "Alyssa P. Hacker" 6 true]
                                           [5 ":supervisor" 4 6 true]
                                           [6 ":unifydb/txInstant" tx-instant 6 true]]
                               ":tempids" {"ben" 4
                                           "alyssa" 5
                                           "unifydb.tx" 6}})})))))
