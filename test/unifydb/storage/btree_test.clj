(ns unifydb.storage.btree-test
  (:require [clojure.test :as t]
            [unifydb.storage :as store]
            [unifydb.storage.btree :as btree]
            [unifydb.storage.memory :as memstore]))

(t/deftest test-find-leaf-for
  (let [store (memstore/->InMemoryKeyValueStore
               (atom {"root" ["0"
                              ["a" "b" "b"]
                              "1"
                              ["a" "b" "c"]
                              "2"
                              ["a" "b" "d"]
                              "3"
                              ["a" "e"]
                              "4"]
                      "0" [["a" "a" "a" "a"]]
                      "1" [["a" "b" "b" "a"] ["a" "b" "b" "b"]]
                      "2" [["a" "b" "c" "a"]]
                      "3" [["a" "b" "d" "a"]]
                      "4" [["a" "e" "a" "b"] ["a" "e" "b" "a"]]}))]
    (doseq [{:keys [value expected-node expected-path]}
            [{:value ["a" "b" "b" "a"]
              :expected-node [["a" "b" "b" "a"] ["a" "b" "b" "b"]]
              :expected-path ["root" "1"]}
             {:value ["a" "a" "a" "a"]
              :expected-node [["a" "a" "a" "a"]]
              :expected-path ["root" "0"]}
             {:value ["a" "a" "b" "a"]
              :expected-node [["a" "a" "a" "a"]]
              :expected-path ["root" "0"]}
             {:value ["a" "f" "a" "b"]
              :expected-node [["a" "e" "a" "b"] ["a" "e" "b" "a"]]
              :expected-path ["root" "4"]}
             {:value ["a" "b" "c" "d"]
              :expected-node [["a" "b" "c" "a"]]
              :expected-path ["root" "2"]}
             {:value ["a" "b"]
              :expected-node [["a" "a" "a" "a"]]
              :expected-path ["root" "0"]}]]
      (t/testing (str "Finding leaf for " value)
        (t/is (= [expected-node expected-path]
                 (btree/find-leaf-for store
                                      (store/get store "root")
                                      value
                                      ["root"])))))))

(t/deftest test-insert!
  (doseq [{:keys [insertions expected-state order]
           :or {order 3}}
          [{:insertions [["a" "b" "c"]]
            :expected-state {"root" [["a" "b" "c"]]}}
           {:insertions [["a" "b" "c"]
                         ["a" "b" "a"]
                         ["a" "b" "d"]
                         ["a" "b" "e"]
                         ["a" "c" "a"]
                         ["a" "c" "b"]]
            :expected-state {"root" ["5" ["a" "b" "d"] "6"],
                             "1" [["a" "b" "a"]],
                             "2" [["a" "b" "c"]],
                             "3" [["a" "b" "d"]],
                             "5" ["1" ["a" "b" "c"] "2"],
                             "6" ["3" ["a" "b" "e"] "4" ["a" "c"] "7"],
                             "4" [["a" "b" "e"]],
                             "7" [["a" "c" "a"] ["a" "c" "b"]]}}
           {:insertions [["a" "b" "c"]
                         ["a" "b" "a"]
                         ["a" "b" "d"]
                         ["a" "b" "e"]
                         ["a" "c" "a"]
                         ["a" "c" "b"]
                         ["a" "c" "c"]
                         ["a" "a" "b"]
                         ["a" "d" "a"]
                         ["b" "a" "a"]
                         ["c" "a" "b"]
                         ["a" "d" "b"]
                         ["c" "b" "a"]
                         ["d" "a" "c"]
                         ["b" "d" "a"]]
            :expected-state {"9" ["7" ["a" "c" "b"] "8"],
                             "3" [["a" "b" "d"]],
                             "4" [["a" "b" "e"]],
                             "8" [["a" "c" "b"]],
                             "14" ["9" ["a" "c" "c"] "12" ["b"] "17"],
                             "root" ["13" ["a" "c"] "14"],
                             "17" ["15" ["c"] "16" ["c" "b"] "18"],
                             "15" [["b" "a" "a"] ["b" "d" "a"]],
                             "7" [["a" "c" "a"]],
                             "5" ["1" ["a" "b" "c"] "2"],
                             "18" [["c" "b" "a"] ["d" "a" "c"]],
                             "12" ["10" ["a" "d"] "11"],
                             "13" ["5" ["a" "b" "d"] "6"],
                             "6" ["3" ["a" "b" "e"] "4"],
                             "1" [["a" "a" "b"] ["a" "b" "a"]],
                             "11" [["a" "d" "a"] ["a" "d" "b"]],
                             "2" [["a" "b" "c"]],
                             "16" [["c" "a" "b"]],
                             "10" [["a" "c" "c"]]}}]]
    (let [id-counter (atom 0)
          id-generator (fn [] (str (swap! id-counter inc)))
          store (memstore/new)
          tree (btree/new! store "root" order id-generator)]
      (doseq [value insertions]
        (btree/insert! tree value))
      (t/testing (format "Insert - insertions: %s, order: %s" insertions order)
        (t/is (= expected-state @(:state (:store tree))))))))

;; TODO this is broken, it's searching a b-tree not a b+ tree
(t/deftest test-search
  (t/testing "Searching"
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
               (btree/search btree ["b"]))))))
