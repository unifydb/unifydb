(ns unifydb.cache.memory-test
  (:require [clojure.test :refer [deftest is testing]]
            [unifydb.cache :as c]
            [unifydb.cache.memory :as memcache]))

(deftest test-in-memory-cache
  (testing "basic get and set"
    (let [cache (memcache/new)]
      (c/cache-set! cache :foo "bar")
      (is (= "bar" (c/cache-get cache :foo)))))
  (testing "ttl"
    (let [cache (memcache/new)]
      (c/cache-set! cache :foo "bar" 2)
      (is (= "bar" (c/cache-get cache :foo)))
      (Thread/sleep 2000)
      (is (nil? (c/cache-get cache :foo))))))
