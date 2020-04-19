(ns unifydb.user-test
  (:require [unifydb.user :as u]
            [unifydb.transact :as transact]
            [unifydb.messagequeue.memory :as memq]
            [unifydb.service :as service]
            [unifydb.storage.memory :as memstore]
            [clojure.set :refer [subset?]]
            [clojure.test :refer [deftest is testing]])
  (:import [java.util Random]))

(deftest test-new-user!
  (let [queue (memq/new)
        transact-service (transact/new queue (memstore/new))
        seeded-rand (Random. 12345)]
    (try
      (service/start! transact-service)
      (testing "Valid username and password"
        (let [{{:keys [tx-data tempids]} :tx-report}
              @(u/new-user! queue "user" "pencil" seeded-rand)
              user-id (tempids "unifydb/user")
              tx-id (tempids "unifydb.tx")]
          (is (subset?
               #{[user-id :unifydb/username "user" tx-id true]
                 [user-id :unifydb/salt "1iCfXDGzYYMiqdjueAfI6g==" tx-id true]
                 [user-id :unifydb/i 4096 tx-id true]
                 [user-id
                  :unifydb/server-key
                  "lJAkDYrYIBgXilqFrA7i9D72FlwWTJXZhYTD0O7JulU="
                  tx-id
                  true]
                 [user-id
                  :unifydb/stored-key
                  "SVBCFA/4M6QOwY+UWn5mQ00bsWkZnJrT9CrLzYgrXuA="
                  tx-id
                  true]}
               (set tx-data)))))
      (testing "Invalid password (fails normalization)"
        (is (thrown-with-msg? Exception #"Prohibited codepoints"
                              @(u/new-user! queue "user" "\u0007"))))
      (finally
        (service/stop! transact-service)))))
