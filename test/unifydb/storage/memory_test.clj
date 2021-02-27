(ns unifydb.storage.memory-test
  (:require [clojure.test :refer [deftest is testing]]
            [unifydb.kvstore.memory :as mem]
            [unifydb.storage :as store]))

(deftest test-transact-facts
  (let [facts '[[1 :color "red" 0 true]
                [1 :name "Widget A" 0 true]
                [2 :name "Machine Z" 1 true]
                [1 :in-machine 2 2 true]]
        db (store/transact-facts! (mem/new) facts)]
    (is (= (vec (:eavt @(:state db)))
           [[1 :color "red" 0 true]
            [1 :in-machine 2 2 true]
            [1 :name "Widget A" 0 true]
            [2 :name "Machine Z" 1 true]]))
    (is (= (vec (:avet @(:state db)))
           [[:color "red" 1 0 true]
            [:in-machine 2 1 2 true]
            [:name "Machine Z" 2 1 true]
            [:name "Widget A" 1 0 true]]))))

(deftest test-fetch-facts
  (let [facts [[1 :name "Ben Bitdiddle" 0 true]
               [1 :job [:computer :wizard] 0 true]
               [1 :salary 60000 1 true]
               [2 :name "Alyssa P. Hacker" 1 true]
               [2 :job [:computer :programmer] 2 true]
               [2 :salary 40000 2 true]
               [2 :supervisor 1 2 true]
               [2 :address [:cambridge [:mass :ave] 78] 2 true]
               [2 :address [:cambridge [:mass :ave] 78] 3 false]]
        db (store/transact-facts! (mem/new) facts)]
    (doseq [{:keys [query tx-id frame expected]}
            [{:query '[[? e] :name "Ben Bitdiddle"]
              :tx-id 3
              :frame {}
              :expected [[1 :name "Ben Bitdiddle" 0 true]]}
             {:query '[[? e] :name [? v]]
              :tx-id 3
              :frame {}
              :expected [[2 :name "Alyssa P. Hacker" 1 true]
                         [1 :name "Ben Bitdiddle" 0 true]]}
             {:query '[[? e] :name [? v]]
              :tx-id 3
              :frame {'e 2}
              :expected [[2 :name "Alyssa P. Hacker" 1 true]]}
             {:query '[1 :name [? v]]
              :tx-id 3
              :frame {}
              :expected [[1 :name "Ben Bitdiddle" 0 true]]}
             {:query '[1 [? a] [? v]]
              :tx-id 3
              :frame {}
              :expected [[1 :job [:computer :wizard] 0 true]
                         [1 :name "Ben Bitdiddle" 0 true]
                         [1 :salary 60000 1 true]]}
             {:query '[1 [? a] [? v]]
              :tx-id 0
              :frame {}
              :expected [[1 :job [:computer :wizard] 0 true]
                         [1 :name "Ben Bitdiddle" 0 true]]}
             {:query '[[? e] :job [:computer [? what]]]
              :tx-id 3
              :frame {}
              :expected [[2 :job [:computer :programmer] 2 true]
                         [1 :job [:computer :wizard] 0 true]]}
             {:query '[2 :address [? v]]
              :tx-id 3
              :frame {}
              :expected [[2 :address [:cambridge [:mass :ave] 78] 2 true]
                         [2 :address [:cambridge [:mass :ave] 78] 3 false]]}
             {:query '[2 :address [? v]]
              :tx-id :latest
              :frame {}
              :expected [[2 :address [:cambridge [:mass :ave] 78] 2 true]
                         [2 :address [:cambridge [:mass :ave] 78] 3 false]]}
             {:query '[[? e] [? a] [? v]]
              :tx-id :latest
              :frame {}
              :expected [[1 :job [:computer :wizard] 0 true]          
                         [1 :name "Ben Bitdiddle" 0 true]
                         [1 :salary 60000 1 true]
                         [2 :address [:cambridge [:mass :ave] 78] 2 true]
                         [2 :address [:cambridge [:mass :ave] 78] 3 false]
                         [2 :job [:computer :programmer] 2 true]
                         [2 :name "Alyssa P. Hacker" 1 true]
                         [2 :salary 40000 2 true]
                         [2 :supervisor 1 2 true]]}]]
      (testing {:query query :tx-id tx-id :frame frame}
        (is (= expected
               (store/fetch-facts db query tx-id frame)))))))
