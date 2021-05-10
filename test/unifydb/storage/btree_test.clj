(ns unifydb.storage.btree-test
  (:require [clojure.pprint :as pprint]
            [clojure.test :as t]
            [unifydb.kvstore :as store]
            [unifydb.kvstore.memory :as memstore]
            [unifydb.storage.btree :as btree]))

(t/deftest test-find-leaf-for
  (let [store (store/new
               (memstore/->InMemoryKeyValueStore
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
                       "4" {:values [["a" "e" "a" "b"] ["a" "e" "b" "a"]]}})))]
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
          [{:insertions [{:key ["a" "b" "c"] :value 0}]
            :expected-state {"root" {:values [{:key ["a" "b" "c"] :value 0}]}}}
           {:insertions [{:key ["a" "b" "c"] :value 0}
                         {:key ["a" "b" "a"] :value 1}
                         {:key ["a" "b" "d"] :value 2}
                         {:key ["a" "b" "e"] :value 3}
                         {:key ["a" "c" "a"] :value 4}
                         {:key ["a" "c" "b"] :value 5}]
            :expected-state {"root" {:values ["5" ["a" "b" "d"] "6"]}
                             "1" {:values [{:key ["a" "b" "a"] :value 1}] :neighbor "2"}
                             "2" {:values [{:key ["a" "b" "c"] :value 0}] :neighbor "3"}
                             "3" {:values [{:key ["a" "b" "d"] :value 2}] :neighbor "4"}
                             "5" {:values ["1" ["a" "b" "c"] "2"]}
                             "6" {:values ["3" ["a" "b" "e"] "4" ["a" "c"] "7"]}
                             "4" {:values [{:key ["a" "b" "e"] :value 3}] :neighbor "7"}
                             "7" {:values [{:key ["a" "c" "a"] :value 4}
                                           {:key ["a" "c" "b"] :value 5}]}}}
           {:insertions [{:key ["a" "b" "c"] :value 0}
                         {:key ["a" "b" "a"] :value 1}
                         {:key ["a" "b" "d"] :value 2}
                         {:key ["a" "b" "e"] :value 3}
                         {:key ["a" "c" "a"] :value 4}
                         {:key ["a" "c" "b"] :value 5}
                         {:key ["a" "c" "c"] :value 6}
                         {:key ["a" "a" "b"] :value 7}
                         {:key ["a" "d" "a"] :value 8}
                         {:key ["b" "a" "a"] :value 9}
                         {:key ["c" "a" "b"] :value 10}
                         {:key ["a" "d" "b"] :value 11}
                         {:key ["c" "b" "a"] :value 12}
                         {:key ["d" "a" "c"] :value 13}
                         {:key ["b" "d" "a"] :value 14}]
            :expected-state {"root" {:values ["13" ["a" "c"] "14"]}
                             "1" {:values [{:key ["a" "a" "b"] :value 7}
                                           {:key ["a" "b" "a"] :value 1}]
                                  :neighbor "2"}
                             "2" {:values [{:key ["a" "b" "c"] :value 0}] :neighbor "3"}
                             "3" {:values [{:key ["a" "b" "d"] :value 2}] :neighbor "4"}
                             "4" {:values [{:key ["a" "b" "e"] :value 3}] :neighbor "7"}
                             "5" {:values ["1" ["a" "b" "c"] "2"]}
                             "6" {:values ["3" ["a" "b" "e"] "4"]}
                             "7" {:values [{:key ["a" "c" "a"] :value 4}] :neighbor "8"}
                             "8" {:values [{:key ["a" "c" "b"] :value 5}] :neighbor "10"}
                             "9" {:values ["7" ["a" "c" "b"] "8"]}
                             "10" {:values [{:key ["a" "c" "c"] :value 6}] :neighbor "11"}
                             "11" {:values [{:key ["a" "d" "a"] :value 8}
                                            {:key ["a" "d" "b"] :value 11}]
                                   :neighbor "15"}
                             "12" {:values ["10" ["a" "d"] "11"]}
                             "13" {:values ["5" ["a" "b" "d"] "6"]}
                             "14" {:values ["9" ["a" "c" "c"] "12" ["b"] "17"]}
                             "15" {:values [{:key ["b" "a" "a"] :value 9}
                                            {:key ["b" "d" "a"] :value 14}]
                                   :neighbor "16"}
                             "16" {:values [{:key ["c" "a" "b"] :value 10}] :neighbor "18"}
                             "17" {:values ["15" ["c"] "16" ["c" "b"] "18"]}
                             "18" {:values [{:key ["c" "b" "a"] :value 12}
                                            {:key ["d" "a" "c"] :value 13}]}}}]]
    (let [id-counter (atom 0)
          id-generator (fn [] (str (swap! id-counter inc)))
          store (store/new (memstore/new))
          tree (btree/new! store "root" order id-generator)]
      (doseq [{:keys [key value]} insertions]
        (btree/insert! tree key value))
      (store/commit! (:store tree))
      (t/testing (format "Insert:\n  insertions:\n%s  order: %s"
                         (with-out-str
                           (pprint/pprint insertions))
                         order)
        (t/is (= expected-state @(:state (:backend @(:store tree)))))))))

(t/deftest test-search
  (t/testing "Searching"
    (let [store (store/new
                 (memstore/->InMemoryKeyValueStore
                  (atom {"root" {:values ["13" ["a" "c"] "14"]}
                         "1" {:values [{:key ["a" "a" "b"] :value 7}
                                       {:key ["a" "b" "a"] :value 1}]
                              :neighbor "2"}
                         "2" {:values [{:key ["a" "b" "c"] :value 0}] :neighbor "3"}
                         "3" {:values [{:key ["a" "b" "d"] :value 2}] :neighbor "4"}
                         "4" {:values [{:key ["a" "b" "e"] :value 3}] :neighbor "7"}
                         "5" {:values ["1" ["a" "b" "c"] "2"]}
                         "6" {:values ["3" ["a" "b" "e"] "4"]}
                         "7" {:values [{:key ["a" "c" "a"] :value 4}] :neighbor "8"}
                         "8" {:values [{:key ["a" "c" "b"] :value 5}] :neighbor "10"}
                         "9" {:values ["7" ["a" "c" "b"] "8"]}
                         "10" {:values [{:key ["a" "c" "c"] :value 6}] :neighbor "11"}
                         "11" {:values [{:key ["a" "d" "a"] :value 8}
                                        {:key ["a" "d" "b"] :value 11}]
                               :neighbor "15"}
                         "12" {:values ["10" ["a" "d"] "11"]}
                         "13" {:values ["5" ["a" "b" "d"] "6"]}
                         "14" {:values ["9" ["a" "c" "c"] "12" ["b"] "17"]}
                         "15" {:values [{:key ["b" "a" "a"] :value 9}
                                        {:key ["b" "d" "a"] :value 14}]
                               :neighbor "16"}
                         "16" {:values [{:key ["c" "a" "b"] :value 10}] :neighbor "18"}
                         "17" {:values ["15" ["c"] "16" ["c" "b"] "18"]}
                         "18" {:values [{:key ["c" "b" "a"] :value 12}
                                        {:key ["d" "a" "c"] :value 13}]}})))
          btree (btree/new! store "root" 3)]
      (t/is (= [{:key ["b" "a" "a"] :value 9}
                {:key ["b" "d" "a"] :value 14}]
               (btree/search btree ["b"])))
      (t/is (= [{:key ["a" "b" "a"] :value 1}
                {:key ["a" "b" "c"] :value 0}
                {:key ["a" "b" "d"] :value 2}
                {:key ["a" "b" "e"] :value 3}]
               (btree/search btree ["a" "b"])))
      (t/is (= [{:key ["a" "b" "c"] :value 0}]
               (btree/search btree ["a" "b" "c"])))
      (t/is (= [{:key ["c" "b" "a"] :value 12}]
               (btree/search btree ["c" "b"])))
      (t/is (= [{:key ["c" "b" "a"] :value 12}]
               (btree/search btree ["c" "b" "a"]))))))

(t/deftest test-delete!
  (t/testing "Deletion"
    (let [store (store/new
                 (memstore/->InMemoryKeyValueStore
                  (atom {"root" {:values ["5" ["a" "b" "d"] "6"]}
                         "1" {:values [{:key ["a" "b" "a"] :value 0}] :neighbor "2"}
                         "2" {:values [{:key ["a" "b" "c"] :value 1}] :neighbor "3"}
                         "3" {:values [{:key ["a" "b" "d"] :value 2}] :neighbor "4"}
                         "5" {:values ["1" ["a" "b" "c"] "2"]}
                         "6" {:values ["3" ["a" "b" "e"] "4" ["a" "c"] "7"]}
                         "4" {:values [{:key ["a" "b" "e"] :value 3}] :neighbor "7"}
                         "7" {:values [{:key ["a" "c" "a"] :value 4}
                                       {:key ["a" "c" "b"] :value 5}]}})))
          tree (btree/new! store "root" 3)]
      (btree/delete! tree ["a" "c" "b"])
      (store/commit! store)
      (t/is (= {"root" {:values ["5" ["a" "b" "d"] "6"]}
                "1" {:values [{:key ["a" "b" "a"] :value 0}] :neighbor "2"}
                "2" {:values [{:key ["a" "b" "c"] :value 1}] :neighbor "3"}
                "3" {:values [{:key ["a" "b" "d"] :value 2}] :neighbor "4"}
                "5" {:values ["1" ["a" "b" "c"] "2"]}
                "6" {:values ["3" ["a" "b" "e"] "4" ["a" "c"] "7"]}
                "4" {:values [{:key ["a" "b" "e"] :value 3}] :neighbor "7"}
                "7" {:values [{:key ["a" "c" "a"] :value 4}]}}
               @(:state (:backend @(:store tree))))))
    (let [store (store/new
                 (memstore/->InMemoryKeyValueStore
                  (atom {"root" {:values ["5" ["a" "b" "d"] "6"]}
                         "1" {:values [{:key ["a" "b" "a"] :value 0}] :neighbor "2"}
                         "2" {:values [{:key ["a" "b" "c"] :value 1}] :neighbor "3"}
                         "3" {:values [{:key ["a" "b" "d"] :value 2}] :neighbor "4"}
                         "5" {:values ["1" ["a" "b" "c"] "2"]}
                         "6" {:values ["3" ["a" "b" "e"] "4" ["a" "c"] "7"]}
                         "4" {:values [{:key ["a" "b" "e"] :value 3}] :neighbor "7"}
                         "7" {:values [{:key ["a" "c" "a"] :value 4}
                                       {:key ["a" "c" "b"] :value 5}]}})))
          tree (btree/new! store "root" 3)]
      (btree/delete! tree ["a" "b" "e"])
      (store/commit! store)
      (t/is (= {"root" {:values ["5" ["a" "b" "d"] "6"]}
                "1" {:values [{:key ["a" "b" "a"] :value 0}] :neighbor "2"}
                "2" {:values [{:key ["a" "b" "c"] :value 1}] :neighbor "3"}
                "3" {:values [{:key ["a" "b" "d"] :value 2}] :neighbor "4"}
                "5" {:values ["1" ["a" "b" "c"] "2"]}
                "6" {:values ["3" ["a" "b" "e"] "4" ["a" "c" "b"] "7"]}
                "4" {:values [{:key ["a" "c" "a"] :value 4}] :neighbor "7"}
                "7" {:values [{:key ["a" "c" "b"] :value 5}]}}
               @(:state (:backend @(:store tree))))))))

(t/deftest test-find-siblings
  (t/testing "Find siblings"
    (let [store (store/new
                 (memstore/->InMemoryKeyValueStore
                  (atom {"root" {:values ["13" ["a" "c"] "14"]}
                         "1" {:values [{:key ["a" "a" "b"] :value 7}
                                       {:key ["a" "b" "a"] :value 1}]
                              :neighbor "2"}
                         "2" {:values [{:key ["a" "b" "c"] :value 0}] :neighbor "3"}
                         "3" {:values [{:key ["a" "b" "d"] :value 2}] :neighbor "4"}
                         "4" {:values [{:key ["a" "b" "e"] :value 3}] :neighbor "7"}
                         "5" {:values ["1" ["a" "b" "c"] "2"]}
                         "6" {:values ["3" ["a" "b" "e"] "4"]}
                         "7" {:values [{:key ["a" "c" "a"] :value 4}] :neighbor "8"}
                         "8" {:values [{:key ["a" "c" "b"] :value 5}] :neighbor "10"}
                         "9" {:values ["7" ["a" "c" "b"] "8"]}
                         "10" {:values [{:key ["a" "c" "c"] :value 6}] :neighbor "11"}
                         "11" {:values [{:key ["a" "d" "a"] :value 8}
                                        {:key ["a" "d" "b"] :value 11}]
                               :neighbor "15"}
                         "12" {:values ["10" ["a" "d"] "11"]}
                         "13" {:values ["5" ["a" "b" "d"] "6"]}
                         "14" {:values ["9" ["a" "c" "c"] "12" ["b"] "17"]}
                         "15" {:values [{:key ["b" "a" "a"] :value 9}
                                        {:key ["b" "d" "a"] :value 14}]
                               :neighbor "16"}
                         "16" {:values [{:key ["c" "a" "b"] :value 10}] :neighbor "18"}
                         "17" {:values ["15" ["c"] "16" ["c" "b"] "18"]}
                         "18" {:values [{:key ["c" "b" "a"] :value 12}
                                        {:key ["d" "a" "c"] :value 13}]}})))
          btree (btree/new! store "root" 3)]
      (t/is (= [2 "11"] (btree/next-sibling btree ["root" "14" "12" "10"] (store/get store "10"))))
      (t/is (= nil (btree/prev-sibling btree ["root" "14" "12" "10"] (store/get store "10"))))
      (t/is (= [4 "17"] (btree/next-sibling btree ["root" "14" "12"] (store/get store "12"))))
      (t/is (= nil (btree/next-sibling btree ["root" "13" "6"] (store/get store "6"))))
      (t/is (= [0 "9"] (btree/prev-sibling btree ["root" "14" "12"] (store/get store "12"))))
      (t/is (= nil (btree/prev-sibling btree ["root" "14" "9"] (store/get store "9")))))))
