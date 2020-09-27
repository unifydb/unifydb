(ns unifydb.server-test
  {:clj-kondo/config
   '{:linters
     {:unresolved-symbol
      {:exclude [(unifydb.server-test/with-server)]}}}}
  (:require [buddy.core.bytes :as bytes]
            [buddy.core.codecs :as codecs]
            [buddy.core.codecs.base64 :as base64]
            [buddy.core.hash :as hash]
            [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.test :refer [deftest testing is]]
            [unifydb.config :as config]
            [unifydb.messagequeue :as queue]
            [unifydb.messagequeue.memory :as memq]
            [unifydb.query :as query]
            [unifydb.server :as server]
            [unifydb.service :as service]
            [unifydb.storage.memory :as memstore]
            [unifydb.transact :as transact]
            [unifydb.auth :as auth]))

(defmacro with-server [[req-fn store-name queue-name token-header-name] txs & body]
  `(with-redefs [config/env (merge config/env {:secret "secret"})]
     (let [~queue-name (memq/new)
           ~store-name (memstore/new)
           query# (query/new ~queue-name ~store-name)
           transact# (transact/new ~queue-name ~store-name)
           server# (server/new ~queue-name ~store-name)
           ~req-fn (fn [request#]
                     (let [app# (server/app (:state server#))
                           response# (app# request#)]
                       @response#))
           token# (auth/make-jwt "ben" [:unifydb/user])
           ~token-header-name (format "Bearer %s" token#)]
       (try
         (service/start! query#)
         (service/start! transact#)
         (service/start! server#)
         (doseq [tx# ~txs]
           (queue/publish ~queue-name :transact {:tx-data tx#}))
         (Thread/sleep 5)  ;; give the transaction time to process
         ~@body
         (finally
           (service/stop! server#)
           (service/stop! transact#)
           (service/stop! query#))))))

(deftest query-endpoint
  (with-server [make-request store queue-backend auth-header]
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
                                 "accept" "application/edn"
                                 "authorization" auth-header}
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
                                              "accept" "application/json"
                                              "authorization" auth-header}
                                    :body (json/write-str
                                           {":tx-id" 3
                                            ":query" {":find" ["'?name"]
                                                      ":where" [["'?e" ":job" ["computer" "'_"]]
                                                                ["'?e" ":name" "'?name"]]}})})]
        (is (= response '{:status 200
                          :headers {"Content-Type" "application/json"}
                          :body "[[\"Alyssa P. Hacker\"],[\"Ben Bitdiddle\"]]"}))))))

(deftest transact-endpoint
  (with-server [make-request store queue-backend auth-header]
    []
    (testing "/transact (EDN)"
      (let [response (make-request
                      {:request-method :post
                       :uri "/transact"
                       :headers {"content-type" "application/edn"
                                 "accept" "application/edn"
                                 "authorization" auth-header}
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
        (is (= (:status response) 200))
        (is (= (:headers response) {"Content-Type" "application/edn"}))
        (is (= (edn/read-string (:body response))
               {:db-after {:tx-id 3}
                :tx-data [[1 :name "Ben Bitdiddle" 3 true]
                          [2 :name "Alyssa P. Hacker" 3 true]
                          [2 :supervisor 1 3 true]
                          [3 :unifydb/txInstant tx-instant 3 true]]
                :tempids {"ben" 1
                          "alyssa" 2
                          "unifydb.tx" 3}}))))
    (testing "/transact (JSON)"
      (let [response (make-request
                      {:request-method :post
                       :uri "/transact"
                       :headers {"content-type" "application/json"
                                 "accept" "application/json"
                                 "authorization" auth-header}
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
        (is (= (:status response) 200))
        (is (= (:headers response) {"Content-Type" "application/json"}))
        (is (= (json/read-str (:body response))
               {":db-after" {":tx-id" 6}
                ":tx-data" [[4 ":name" "Ben Bitdiddle" 6 true]
                            [5 ":name" "Alyssa P. Hacker" 6 true]
                            [5 ":supervisor" 4 6 true]
                            [6 ":unifydb/txInstant" tx-instant 6 true]]
                ":tempids" {"ben" 4
                            "alyssa" 5
                            "unifydb.tx" 6}}))))))

(deftest test-auth
  (with-server [make-request store queue-backend _auth-header]
    '[[[:unifydb/add "user" :unifydb/username "ben"]
       [:unifydb/add "user" :unifydb/password "top secret"]
       [:unifydb/add "thing" :id "foo-thing"]
       [:unifydb/add "thing" :foo "bar"]]]
    (testing "missing authentication"
      (let [response (make-request {:request-method :post
                                    :uri "/query"
                                    :headers {"content-type" "application/edn"
                                              "accept" "application/edn"}
                                    :body (prn-str '{:tx-id :latest
                                                     :query {:find [?foo]
                                                             :where [[?thing :id "foo-thing"]
                                                                     [?thing :foo ?foo]]}})})]
        (is (= 401 (:status response)))))
    (testing "authenticate GET request"
      (let [response (make-request {:request-method :get
                                    :uri "/authenticate"
                                    :query-string "username=ben"
                                    :headers {"accept" "application/edn"}})
            body (edn/read-string (:body response))]
        (is (= 200 (:status response)))
        (is (= "ben" (:username body)))
        (is (not (nil? (:salt body))))
        (is (string? (:salt body)))))
    (testing "authenticate POST request"
      (let [get-response (make-request {:request-method :get
                                        :uri "/authenticate"
                                        :query-string "username=ben"
                                        :headers {"accept" "application/edn"}})
            {:keys [username salt]} (edn/read-string (:body get-response))
            hashed-password (codecs/bytes->str
                             (base64/encode
                              (hash/sha512 (bytes/concat (codecs/str->bytes "top secret")
                                                         (base64/decode salt)))))
            response (make-request {:request-method :post
                                    :uri "/authenticate"
                                    :headers {"content-type" "application/edn"
                                              "accept" "application/edn"}
                                    :body (prn-str {:username username
                                                    :password hashed-password})})
            response-body (edn/read-string (:body response))]
        (is (= 200 (:status response)))
        (is (= "ben" (:username response-body)))
        (is (not (nil? (:token response-body))))
        (is (string? (:token response-body)))))
    (testing "token-authenticated request"
      (let [get-response (make-request {:request-method :get
                                        :uri "/authenticate"
                                        :query-string "username=ben"
                                        :headers {"accept" "application/edn"}})
            {:keys [username salt]} (edn/read-string (:body get-response))
            hashed-password (codecs/bytes->str
                             (base64/encode
                              (hash/sha512 (bytes/concat (codecs/str->bytes "top secret")
                                                         (base64/decode salt)))))
            post-response (make-request {:request-method :post
                                         :uri "/authenticate"
                                         :headers {"content-type" "application/edn"
                                                   "accept" "application/edn"}
                                         :body (prn-str {:username username
                                                         :password hashed-password})})
            {:keys [token]} (edn/read-string (:body post-response))
            response (make-request {:request-method :post
                                    :uri "/query"
                                    :headers {"content-type" "application/edn"
                                              "accept" "application/edn"
                                              "authorization" (format "Bearer %s" token)}
                                    :body (prn-str '{:tx-id :latest
                                                     :query {:find [?foo]
                                                             :where [[?thing :id "foo-thing"]
                                                                     [?thing :foo ?foo]]}})})]
        (is (= 200 (:status response)))
        (is (= [["bar"]] (edn/read-string (:body response))))))
    (testing "not found"
      (let [response (make-request {:request-method :get
                                    :uri "/does-not-exist"})]
        (is (= 404 (:status response)))))))
