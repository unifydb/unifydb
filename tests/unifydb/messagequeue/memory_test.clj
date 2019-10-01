(ns unifydb.messagequeue.memory-test
  (:require [clojure.test :refer [deftest testing is]]
            [unifydb.messagequeue :as q]
            [unifydb.messagequeue.memory :as memq]
            [unifydb.util :refer [take-n!]]))

(deftest memq-test
  (testing "Pubsub"
    (let [memq (memq/new)
          subscription (q/subscribe memq :testq)]
      (doseq [message [:first-message :second-message :third-message]]
        (q/publish memq :testq message))
      (is (= (take-n! 3 subscription ) [:first-message :second-message :third-message])))))
