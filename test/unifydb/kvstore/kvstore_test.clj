(ns unifydb.kvstore.kvstore-test
  (:require [clojure.test :as t]
            [unifydb.kvstore :as kvstore]
            [unifydb.kvstore.memory :as memstore]))

(t/deftest test-kvstore
  (let [store (kvstore/new (memstore/new))]
    (t/testing "Empty store"
      (t/is (= false @(kvstore/contains-batch? store ["foo" "bar"])))
      (t/is (= false @(kvstore/contains? store "baz")))
      (t/is (= [nil nil] @(kvstore/get-batch store ["foo" "bar"])))
      (t/is (= nil @(kvstore/get store "baz"))))
    (t/testing "Insertion"
      (kvstore/write-batch! store [[:assoc! "foo" 1]
                                   [:assoc! "bar" 2]])
      (kvstore/assoc! store "baz" 3)
      @(kvstore/commit! store)
      (t/is (= true @(kvstore/contains-batch? store ["foo" "bar"])))
      (t/is (= true @(kvstore/contains? store "baz")))
      (t/is (= [1 2] @(kvstore/get-batch store ["foo" "bar"])))
      (t/is (= [1 nil 2] @(kvstore/get-batch store ["foo" "beep" "bar"])))
      (t/is (= 3 @(kvstore/get store "baz"))))
    (t/testing "Deletion"
      (kvstore/dissoc! store "foo")
      @(kvstore/commit! store)
      (t/is (= false @(kvstore/contains? store "foo")))
      (t/is (= nil @(kvstore/get store "foo")))
      (t/is (= true @(kvstore/contains? store "bar")))
      (t/is (= 2 @(kvstore/get store "bar")))
      (t/is (= false @(kvstore/contains-batch? store ["foo" "bar"]))))
    (t/testing "Changes visible before commit"
      (kvstore/write-batch! store [[:assoc! "boom" "bap"]
                                   [:dissoc! "baz"]])
      (t/is (= true @(kvstore/contains? store "boom")))
      (t/is (= "bap" @(kvstore/get store "boom")))
      (t/is (= false @(kvstore/contains? store "baz")))
      (t/is (= nil @(kvstore/get store "baz"))))))
