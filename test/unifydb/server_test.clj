(ns unifydb.server-test
  {:clj-kondo/config
   '{:linters
     {:unresolved-symbol
      {:exclude [(unifydb.server-test/with-server)]}}}}
  (:require [buddy.core.bytes :as bytes]
            [buddy.core.codecs :as codecs]
            [buddy.core.codecs.base64 :as base64]
            [buddy.core.hash :as hash]
            [clojure.test :refer [deftest testing is]]
            [unifydb.auth :as auth]
            [unifydb.cache.memory :as memcache]
            [unifydb.config :as config]
            [unifydb.edn :as edn]
            [unifydb.kvstore.memory :as memstore]
            [unifydb.messagequeue :as queue]
            [unifydb.messagequeue.memory :as memq]
            [unifydb.query :as query]
            [unifydb.server :as server]
            [unifydb.service :as service]
            [unifydb.storage :as store]
            [unifydb.transact :as transact]))

(defmacro with-server [[req-fn store-name queue-name token-header-name] txs & body]
  `(config/with-config {:secret "secret"}
     (let [~queue-name (memq/new)
           ~store-name (memstore/new)
           query# (query/new ~queue-name (store/new! ~store-name))
           transact# (transact/new ~queue-name (store/new! ~store-name))
           cache# (memcache/new)
           server# (server/new ~queue-name cache#)
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
                              {:tx-id #unifydb/id 3
                               :query '{:find [?name]
                                        :where [[?e :job ["computer" _]]
                                                [?e :name ?name]]}})})]
        (is (= response '{:status 200
                          :headers {"Content-Type" "application/edn"}
                          :body "([\"Alyssa P. Hacker\"] [\"Ben Bitdiddle\"])"}))))))

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
        (is (= {:db-after {:tx-id #unifydb/id 3}
                :tx-data [[#unifydb/id 1 :name "Ben Bitdiddle" #unifydb/id 3 true]
                          [#unifydb/id 2 :name "Alyssa P. Hacker" #unifydb/id 3 true]
                          [#unifydb/id 2 :supervisor #unifydb/id 1 #unifydb/id 3 true]
                          [#unifydb/id 3 :unifydb/txInstant tx-instant #unifydb/id 3 true]]
                :tempids {"ben" #unifydb/id 1
                          "alyssa" #unifydb/id 2
                          "unifydb.tx" #unifydb/id 3}}
               (edn/read-string (:body response))))))))

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
        (is (string? (:salt body)))
        (is (not (nil? (:nonce-key body))))
        (is (string? (:nonce-key body)))
        (is (not (nil? (:nonce body))))
        (is (string? (:nonce body)))))
    (testing "authenticate POST request"
      (let [get-response (make-request {:request-method :get
                                        :uri "/authenticate"
                                        :query-string "username=ben"
                                        :headers {"accept" "application/edn"}})
            {:keys [username
                    salt
                    nonce
                    nonce-key]} (edn/read-string (:body get-response))
            hashed-password (codecs/bytes->str
                             (base64/encode
                              (hash/sha512 (bytes/concat (codecs/str->bytes "top secret")
                                                         (base64/decode salt)))))
            response (make-request {:request-method :post
                                    :uri "/authenticate"
                                    :headers {"content-type" "application/edn"
                                              "accept" "application/edn"}
                                    :body (prn-str {:username username
                                                    :password hashed-password
                                                    :nonce nonce
                                                    :nonce-key nonce-key})})
            response-body (edn/read-string (:body response))]
        (is (= 200 (:status response)))
        (is (= "ben" (:username response-body)))
        (is (not (nil? (:token response-body))))
        (is (string? (:token response-body)))))
    (testing "invalid nonce"
      (let [get-response (make-request {:request-method :get
                                        :uri "/authenticate"
                                        :query-string "username=ben"
                                        :headers {"accept" "application/edn"}})
            {:keys [username
                    salt
                    nonce-key]} (edn/read-string (:body get-response))
            hashed-password (codecs/bytes->str
                             (base64/encode
                              (hash/sha512 (bytes/concat (codecs/str->bytes "top secret")
                                                         (base64/decode salt)))))
            response (make-request {:request-method :post
                                    :uri "/authenticate"
                                    :headers {"content-type" "application/edn"
                                              "accept" "application/edn"}
                                    :body (prn-str {:username username
                                                    :password hashed-password
                                                    :nonce "wrong"
                                                    :nonce-key nonce-key})})]
        (is (= 400 (:status response)))))
    (testing "token-authenticated request"
      (let [get-response (make-request {:request-method :get
                                        :uri "/authenticate"
                                        :query-string "username=ben"
                                        :headers {"accept" "application/edn"}})
            {:keys [username
                    salt
                    nonce
                    nonce-key]} (edn/read-string (:body get-response))
            hashed-password (codecs/bytes->str
                             (base64/encode
                              (hash/sha512 (bytes/concat (codecs/str->bytes "top secret")
                                                         (base64/decode salt)))))
            post-response (make-request {:request-method :post
                                         :uri "/authenticate"
                                         :headers {"content-type" "application/edn"
                                                   "accept" "application/edn"}
                                         :body (prn-str {:username username
                                                         :password hashed-password
                                                         :nonce-key nonce-key
                                                         :nonce nonce})})
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
