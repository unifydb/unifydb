(ns unifydb.test-runner
  (:require [clojure.test :as test]
            [unifydb.query-test]
            [unifydb.server-test]
            [unifydb.transact-test]
            [unifydb.storage.memory-test]
            [unifydb.messagequeue.memory-test]))

(defn run-tests []
  (test/run-tests 'unifydb.query-test
                  'unifydb.server-test
                  'unifydb.transact-test
                  'unifydb.storage.memory-test
                  'unifydb.messagequeue.memory-test))
