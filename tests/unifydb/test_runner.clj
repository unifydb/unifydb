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

(defn -main []
  (let [results (run-tests)]
    (println results)
    (System/exit
     (if (and (= 0 (:fail results))
              (= 0 (:error results)))
       0
       1))))
