(ns unifydb.server-test
  (:require [clojure.data.json :as json]
            [clojure.test :refer [deftest testing is]]
            [manifold.deferred :as d]
            [unifydb.messagequeue :as queue]
            [unifydb.messagequeue.memory :as memq]
            [unifydb.query :as query]
            [unifydb.server :as server]
            [unifydb.service :as service]
            [unifydb.storage.memory :as memstore]
            [unifydb.transact :as transact]))

(defmacro defservertest [name [req-fn] txs & body]
  `(deftest ~name
    (let [queue# (memq/new)
          store# (memstore/new)
          query# (query/new queue#)
          transact# (transact/new queue#)
          server# (server/new queue# store#)
          ~req-fn (fn [request#]
                    (let [app# (server/app (:state server#))
                          response# (app# request#)]
                      @response#))]
      (try
        (service/start! query#)
        (service/start! transact#)
        (service/start! server#)
        (doseq [tx# ~txs]
          (queue/publish queue# :transact {:conn {:storage-backend store#
                                                  :queue-backend queue#}
                                           :tx-data tx#}))
        (Thread/sleep 5)  ;; give the transaction time to process
        ~@body
        (finally
          (service/stop! server#)
          (service/stop! transact#)
          (service/stop! query#))))))

(defservertest query-endpoint [make-request]
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
