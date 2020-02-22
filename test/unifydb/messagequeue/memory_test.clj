(ns unifydb.messagequeue.memory-test
  (:require [clojure.test :refer [deftest testing is]]
            [manifold.stream :as s]
            [unifydb.messagequeue :as q]
            [unifydb.messagequeue.memory :as memq]
            [unifydb.util :refer [take-n!]]))

(deftest memq-test
  (testing "Pubsub"
    (let [memq (memq/new)
          subscription (q/subscribe memq :testq)]
      (doseq [message [:first-message :second-message :third-message]]
        (q/publish memq :testq message))
      (is (= (take-n! 3 subscription) [:first-message :second-message :third-message]))))
  (testing "Consumer groups"
    (let [memq (memq/new)
          sub1 (q/subscribe memq :testq :group1)
          sub2 (q/subscribe memq :testq :group2)
          sub3 (q/subscribe memq :testq :group3)
          sub3a (q/subscribe memq :testq :group3)]
      (q/publish memq :testq "message1")
      (is (= "message1" (deref (s/take! sub1) 5 :failed)))
      (is (= "message1" (deref (s/take! sub2) 5 :failed)))
      (let [group-results (map #(deref (s/take! %) 5 :failed)
                               [sub3 sub3a])]
        (is (= {:failed 1 "message1" 1} (frequencies group-results))))))
  (testing "Subscriber cleanup"
    (let [memq (memq/new)
          sub1 (q/subscribe memq :testq-cleanup :group1)]
      (s/close! sub1)
      (let [sub2 (q/subscribe memq :testq-cleanup :group1)]
        (q/publish memq :testq-cleanup "message")
        (is (nil? (deref (s/take! sub1) 5 :failed)))
        (is (= "message" (deref (s/take! sub2) 5 :failed)))))))
