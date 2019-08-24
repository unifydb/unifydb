(ns unifydb.query-test
  (:require [clojure.test :refer [deftest is]]
            [unifydb.query :as query]
            [unifydb.storage :as store]
            [unifydb.storage.memory :as memstore]
            [unifydb.streaming.threadpool :as pool]))

(deftest query-facts
  (let [facts [[1 :name "Ben Bitdiddle" 0 true]
               [1 :job [:computer :wizard] 0 true]
               [1 :salary 60000 1 true]
               [2 :name "Alyssa P. Hacker" 1 true]
               [2 :job [:computer :programmer] 2 true]
               [2 :salary 40000 2 true]
               [2 :supervisor 1 2 true]
               [1 :address [:slumerville [:ridge :road] 10] 2 true]
               [2 :address [:cambridge [:mass :ave] 78] 2 true]
               [2 :address [:cambridge [:mass :ave] 78] 3 false]]
        storage-backend (-> (memstore/new) (store/transact-facts! facts))
        streaming-backend (pool/new)
        db-latest {:storage-backend storage-backend
                   :streaming-backend streaming-backend
                   :tx-id 3}
        db-tx-2 {:storage-backend storage-backend
                 :streaming-backend streaming-backend
                 :tx-id 2}
        db-tx-1 {:storage-backend storage-backend
                 :streaming-backend streaming-backend
                 :tx-id 1}]
    (doseq [{:keys [query db expected]}
            [{:query '[[? e] :name "Ben Bitdiddle"]
              :db db-latest
              :expected '[{e 1}]}
             {:query '[[? e] :job [:computer [? what]]]
              :db db-latest
              :expected '[{e 2
                           what :programmer}
                          {e 1
                           what :wizard}]}
             {:query '[:and
                       [[? e] :job [:computer [? what]]]
                       [[? e] :salary 60000]]
              :db db-latest
              :expected '[{e 1
                           what :wizard}]}
             {:query '[:or
                       [[? e] :job [:computer :wizard]]
                       [[? e] :job [:computer :programmer]]]
              :db db-latest
              :expected '[{e 1}
                          {e 2}]}
             {:query '[:and
                       [[? e] :job [:computer [? what]]]
                       [:not [[? e] :salary 60000]]]
              :db db-latest
              :expected '[{e 2
                           what :programmer}]}
             {:query '[1 :address [[? town] & [? road-and-number]]]
              :db db-latest
              :expected '[{town :slumerville
                           road-and-number [[:ridge :road] 10]}]}
             {:query '[2 :address [[? town] & [? road-and-number]]]
              :db db-tx-2
              :expected '[{town :cambridge
                           road-and-number [[:mass :ave] 78]}]}
             {:query '[2 :address [[? town] & [? road-and-number]]]
              :db db-latest
              :expected '[]}]]
      (is (= (query/query db query)
             expected)))))
