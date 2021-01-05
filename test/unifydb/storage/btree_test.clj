(ns unifydb.storage.btree-test
  (:require [clojure.test :as t]
            [unifydb.storage.btree :as btree]
            [unifydb.storage.memory :as memstore]))

(t/deftest test-traverse
  (t/testing "Traversing"
    (let [store (memstore/->InMemoryKeyValueStore
                 (atom {"root" ["1"
                                ["a" "d"]
                                "2"
                                ["b" "c"]
                                "3"
                                ["c" "c"]
                                "4"]
                        "1" [["a" "a"] ["a" "b"] ["a" "c"]]
                        "2" [["a" "f"] ["b" "a"] ["b" "b"]]
                        "3" [["b" "d"] ["c" "a"] ["c" "b"]]
                        "4" [["c" "d"]
                             ["d" "a"]
                             ["d" "b"]
                             ["d" "c"]
                             ["d" "d"]]}))
          btree (btree/new! store "root" 7)]
      (t/is (= [["b" "a"]
                ["b" "b"]
                ["b" "c"]
                ["b" "d"]]
               (btree/traverse btree ["b"]))))))
