(ns unifydb.storage.memory-test
  (:require [clojure.test :refer [deftest is]]
            [manifold.stream :as stream]
            [me.tonsky.persistent-sorted-set :as set]
            [unifydb.storage.memory :as mem]
            [unifydb.storage :as store]))

(deftest test-transact-facts
  (let [facts '[[1 :color "red" 0 true]
                [1 :name "Widget A" 0 true]
                [2 :name "Machine Z" 1 true]
                [1 :in-machine 2 2 true]]
        db (-> (mem/new) (store/transact-facts! facts))]
    (is (= (vec @(:eavt db))
           [(mem/vec->fact [1 :color "red" 0 true])
            (mem/vec->fact [1 :in-machine 2 2 true])
            (mem/vec->fact [1 :name "Widget A" 0 true])
            (mem/vec->fact [2 :name "Machine Z" 1 true])]))
    (is (= (vec @(:aevt db))
           [(mem/vec->fact [1 :color "red" 0 true])
            (mem/vec->fact [1 :in-machine 2 2 true])
            (mem/vec->fact [1 :name "Widget A" 0 true])
            (mem/vec->fact [2 :name "Machine Z" 1 true])]))
    (is (= (vec @(:avet db))
           [(mem/vec->fact [1 :color "red" 0 true])
            (mem/vec->fact [1 :in-machine 2 2 true])
            (mem/vec->fact [2 :name "Machine Z" 1 true])
            (mem/vec->fact [1 :name "Widget A" 0 true])]))))    
    

(deftest test-fetch-facts
  (let [facts '[[1 :color "red" 0 true]
                [1 :name "Widget A" 0 true]
                [2 :name "Machine Z" 1 true]
                [1 :in-machine 2 2 true]]
        db (-> (mem/new) (store/transact-facts! facts))]
    (doseq [{:keys [query tx-id frame expected]}
            [{:query '[[? e] [? a] [? v]]
              :tx-id 2
              :frame {}
              :expected '[[1 :color "red" 0 true]
                          [1 :in-machine 2 2 true]
                          [1 :name "Widget A" 0 true]
                          [2 :name "Machine Z" 1 true]]}
             {:query '[[? e] [? a] [? v]]
              :tx-id 1
              :frame {}
              :expected '[[1 :color "red" 0 true]
                          [1 :in-machine 2 2 true]
                          [1 :name "Widget A" 0 true]]}
             {:query '[1 [? a] [? v]]
              :tx-id 2
              :frame {}
              :expected '[[1 :color "red" 0 true]
                          [1 :in-machine 2 2 true]
                          [1 :name "Widget A" 0 true]]}
             {:query '[1 :in-machine [? v]]
              :tx-id 2
              :frame {}
              :expected '[[1 :in-machine 2 2 true]]}
             {:query '[[? e] :name [? v]]
              :tx-id 2
              :frame {}
              :expected '[[1 :name "Widget A" 0 true]
                          [2 :name "Machine Z" 1 true]]}
             {:query '[[? e] :name "Machine Z"]
              :tx-id 2
              :frame {}
              :expected '[[2 :name "Machine Z" 1 true]]}
             {:query '[[? e] :name [? v]]
              :tx-id 2
              :frame '{v "Machine Z"}
              :expected '[[2 :name "Machine Z" 1 true]]}]]
      (is (= (stream/stream->seq
              (store/fetch-facts db query tx-id frame)))
          expected))))
