(ns unifydb.storage.btree-test
  (:require [clojure.test :as t]
            [unifydb.storage :as store]
            [unifydb.storage.btree :as btree]
            [unifydb.storage.memory :as memstore]))

(t/deftest test-find-leaf-for
  (let [store (memstore/->InMemoryKeyValueStore
               (atom {"root" {:values ["0"
                                       ["a" "b" "b"]
                                       "1"
                                       ["a" "b" "c"]
                                       "2"
                                       ["a" "b" "d"]
                                       "3"
                                       ["a" "e"]
                                       "4"]}
                      "0" {:values [["a" "a" "a" "a"]]}
                      "1" {:values [["a" "b" "b" "a"] ["a" "b" "b" "b"]]}
                      "2" {:values [["a" "b" "c" "a"]]}
                      "3" {:values [["a" "b" "d" "a"]]}
                      "4" {:values [["a" "e" "a" "b"] ["a" "e" "b" "a"]]}}))]
    (doseq [{:keys [value expected-node expected-path]}
            [{:value ["a" "b" "b" "a"]
              :expected-node {:values [["a" "b" "b" "a"] ["a" "b" "b" "b"]]}
              :expected-path ["root" "1"]}
             {:value ["a" "a" "a" "a"]
              :expected-node {:values [["a" "a" "a" "a"]]}
              :expected-path ["root" "0"]}
             {:value ["a" "a" "b" "a"]
              :expected-node {:values [["a" "a" "a" "a"]]}
              :expected-path ["root" "0"]}
             {:value ["a" "f" "a" "b"]
              :expected-node {:values [["a" "e" "a" "b"] ["a" "e" "b" "a"]]}
              :expected-path ["root" "4"]}
             {:value ["a" "b" "c" "d"]
              :expected-node {:values [["a" "b" "c" "a"]]}
              :expected-path ["root" "2"]}
             {:value ["a" "b"]
              :expected-node {:values [["a" "a" "a" "a"]]}
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
            :expected-state {"root" {:values [["a" "b" "c"]]}}}
           {:insertions [["a" "b" "c"]
                         ["a" "b" "a"]
                         ["a" "b" "d"]
                         ["a" "b" "e"]
                         ["a" "c" "a"]
                         ["a" "c" "b"]]
            :expected-state {"root" {:values ["5" ["a" "b" "d"] "6"]}
                             "1" {:values [["a" "b" "a"]] :neighbor "2"}
                             "2" {:values [["a" "b" "c"]] :neighbor "3"}
                             "3" {:values [["a" "b" "d"]] :neighbor "4"}
                             "5" {:values ["1" ["a" "b" "c"] "2"]}
                             "6" {:values ["3" ["a" "b" "e"] "4" ["a" "c"] "7"]}
                             "4" {:values [["a" "b" "e"]] :neighbor "7"}
                             "7" {:values [["a" "c" "a"] ["a" "c" "b"]]}}}
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
            :expected-state {"root" {:values ["13" ["a" "c"] "14"]}
                             "1" {:values [["a" "a" "b"] ["a" "b" "a"]] :neighbor "2"}
                             "2" {:values [["a" "b" "c"]] :neighbor "3"}
                             "3" {:values [["a" "b" "d"]] :neighbor "4"}
                             "4" {:values [["a" "b" "e"]] :neighbor "7"}
                             "5" {:values ["1" ["a" "b" "c"] "2"]}
                             "6" {:values ["3" ["a" "b" "e"] "4"]}
                             "7" {:values [["a" "c" "a"]] :neighbor "8"}
                             "8" {:values [["a" "c" "b"]] :neighbor "10"}
                             "9" {:values ["7" ["a" "c" "b"] "8"]}
                             "10" {:values [["a" "c" "c"]] :neighbor "11"}
                             "11" {:values [["a" "d" "a"] ["a" "d" "b"]] :neighbor "15"}
                             "12" {:values ["10" ["a" "d"] "11"]}
                             "13" {:values ["5" ["a" "b" "d"] "6"]}
                             "14" {:values ["9" ["a" "c" "c"] "12" ["b"] "17"]}
                             "15" {:values [["b" "a" "a"] ["b" "d" "a"]] :neighbor "16"}
                             "16" {:values [["c" "a" "b"]] :neighbor "18"}
                             "17" {:values ["15" ["c"] "16" ["c" "b"] "18"]}
                             "18" {:values [["c" "b" "a"] ["d" "a" "c"]]}}}]]
    (let [id-counter (atom 0)
          id-generator (fn [] (str (swap! id-counter inc)))
          store (memstore/new)
          tree (btree/new! store "root" order id-generator)]
      (doseq [value insertions]
        (btree/insert! tree value))
      (t/testing (format "Insert - insertions: %s, order: %s" insertions order)
        (t/is (= expected-state @(:state (:store tree))))))))

(t/deftest test-search
  (t/testing "Searching"
    (let [store (memstore/->InMemoryKeyValueStore
                 (atom {"root" {:values ["13" ["a" "c"] "14"]}
                        "1" {:values [["a" "a" "b"] ["a" "b" "a"]] :neighbor "2"}
                        "2" {:values [["a" "b" "c"]] :neighbor "3"}
                        "3" {:values [["a" "b" "d"]] :neighbor "4"}
                        "4" {:values [["a" "b" "e"]] :neighbor "7"}
                        "5" {:values ["1" ["a" "b" "c"] "2"]}
                        "6" {:values ["3" ["a" "b" "e"] "4"]}
                        "7" {:values [["a" "c" "a"]] :neighbor "8"}
                        "8" {:values [["a" "c" "b"]] :neighbor "10"}
                        "9" {:values ["7" ["a" "c" "b"] "8"]}
                        "10" {:values [["a" "c" "c"]] :neighbor "11"}
                        "11" {:values [["a" "d" "a"] ["a" "d" "b"]] :neighbor "15"}
                        "12" {:values ["10" ["a" "d"] "11"]}
                        "13" {:values ["5" ["a" "b" "d"] "6"] :neighbor "14"}
                        "14" {:values ["9" ["a" "c" "c"] "12" ["b"] "17"]}
                        "15" {:values [["b" "a" "a"] ["b" "d" "a"]] :neighbor "16"}
                        "16" {:values [["c" "a" "b"]] :neighbor "18"}
                        "17" {:values ["15" ["c"] "16" ["c" "b"] "18"]}
                        "18" {:values [["c" "b" "a"] ["d" "a" "c"]]}}))
          btree (btree/new! store "root" 3)]
      (t/is (= [["b" "a" "a"]
                ["b" "d" "a"]]
               (btree/search btree ["b"])))
      (t/is (= [["a" "b" "a"]
                ["a" "b" "c"]
                ["a" "b" "d"]
                ["a" "b" "e"]]
               (btree/search btree ["a" "b"])))
      (t/is (= [["a" "b" "c"]]
               (btree/search btree ["a" "b" "c"])))
      (t/is (= [["c" "b" "a"]]
               (btree/search btree ["c" "b"])))
      (t/is (= [["c" "b" "a"]]
               (btree/search btree ["c" "b" "a"]))))))

(t/deftest test-delete!
  (t/testing "Deletion"
    (let [store (memstore/->InMemoryKeyValueStore
                 (atom {"root" {:values ["5" ["a" "b" "d"] "6"]}
                        "1" {:values [["a" "b" "a"]] :neighbor "2"}
                        "2" {:values [["a" "b" "c"]] :neighbor "3"}
                        "3" {:values [["a" "b" "d"]] :neighbor "4"}
                        "5" {:values ["1" ["a" "b" "c"] "2"]}
                        "6" {:values ["3" ["a" "b" "e"] "4" ["a" "c"] "7"]}
                        "4" {:values [["a" "b" "e"]] :neighbor "7"}
                        "7" {:values [["a" "c" "a"] ["a" "c" "b"]]}}))
          tree (btree/new! store "root" 3)]
      (btree/delete! tree ["a" "c" "b"])
      (t/is (= {"root" {:values ["5" ["a" "b" "d"] "6"]}
                "1" {:values [["a" "b" "a"]] :neighbor "2"}
                "2" {:values [["a" "b" "c"]] :neighbor "3"}
                "3" {:values [["a" "b" "d"]] :neighbor "4"}
                "5" {:values ["1" ["a" "b" "c"] "2"]}
                "6" {:values ["3" ["a" "b" "e"] "4" ["a" "c"] "7"]}
                "4" {:values [["a" "b" "e"]] :neighbor "7"}
                "7" {:values [["a" "c" "a"]]}}
               @(:state (:store tree)))))
    (let [store (memstore/->InMemoryKeyValueStore
                 (atom {"root" {:values ["5" ["a" "b" "d"] "6"]}
                        "1" {:values [["a" "b" "a"]] :neighbor "2"}
                        "2" {:values [["a" "b" "c"]] :neighbor "3"}
                        "3" {:values [["a" "b" "d"]] :neighbor "4"}
                        "5" {:values ["1" ["a" "b" "c"] "2"]}
                        "6" {:values ["3" ["a" "b" "e"] "4" ["a" "c"] "7"]}
                        "4" {:values [["a" "b" "e"]] :neighbor "7"}
                        "7" {:values [["a" "c" "a"] ["a" "c" "b"]]}}))
          tree (btree/new! store "root" 3)]
      (btree/delete! tree ["a" "b" "e"])
      (t/is (= {"root" {:values ["5" ["a" "b" "d"] "6"]},
                "1" {:values [["a" "b" "a"]], :neighbor "2"},
                "2" {:values [["a" "b" "c"]], :neighbor "3"},
                "3" {:values [["a" "b" "d"]], :neighbor "4"},
                "5" {:values ["1" ["a" "b" "c"] "2"]},
                "6" {:values ["3" ["a" "b" "e"] "4" ["a" "c" "b"] "7"]},
                "4" {:values [["a" "c" "a"]], :neighbor "7"},
                "7" {:values [["a" "c" "b"]]}}
               @(:state (:store tree)))))))

(t/deftest test-find-siblings
  (t/testing "Find siblings"
    (let [store (memstore/->InMemoryKeyValueStore
                 (atom {"root" {:values ["13" ["a" "c"] "14"]}
                        "1" {:values [["a" "a" "b"] ["a" "b" "a"]] :neighbor "2"}
                        "2" {:values [["a" "b" "c"]] :neighbor "3"}
                        "3" {:values [["a" "b" "d"]] :neighbor "4"}
                        "4" {:values [["a" "b" "e"]] :neighbor "7"}
                        "5" {:values ["1" ["a" "b" "c"] "2"]}
                        "6" {:values ["3" ["a" "b" "e"] "4"]}
                        "7" {:values [["a" "c" "a"]] :neighbor "8"}
                        "8" {:values [["a" "c" "b"]] :neighbor "10"}
                        "9" {:values ["7" ["a" "c" "b"] "8"]}
                        "10" {:values [["a" "c" "c"]] :neighbor "11"}
                        "11" {:values [["a" "d" "a"] ["a" "d" "b"]] :neighbor "15"}
                        "12" {:values ["10" ["a" "d"] "11"]}
                        "13" {:values ["5" ["a" "b" "d"] "6"] :neighbor "14"}
                        "14" {:values ["9" ["a" "c" "c"] "12" ["b"] "17"]}
                        "15" {:values [["b" "a" "a"] ["b" "d" "a"]] :neighbor "16"}
                        "16" {:values [["c" "a" "b"]] :neighbor "18"}
                        "17" {:values ["15" ["c"] "16" ["c" "b"] "18"]}
                        "18" {:values [["c" "b" "a"] ["d" "a" "c"]]}}))
          btree (btree/new! store "root" 3)]
      (t/is (= [2 "11"] (btree/next-sibling btree ["root" "14" "12" "10"] (store/get store "10"))))
      (t/is (= nil (btree/prev-sibling btree ["root" "14" "12" "10"] (store/get store "10"))))
      (t/is (= [4 "17"] (btree/next-sibling btree ["root" "14" "12"] (store/get store "12"))))
      (t/is (= nil (btree/next-sibling btree ["root" "13" "6"] (store/get store "6"))))
      (t/is (= [0 "9"] (btree/prev-sibling btree ["root" "14" "12"] (store/get store "12"))))
      (t/is (= nil (btree/prev-sibling btree ["root" "14" "9"] (store/get store "9")))))))
