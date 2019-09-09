(ns unifydb.messagequeue.memory-test
  (:require [clojure.test :refer [deftest testing is]]
            [unifydb.messagequeue :as q]
            [unifydb.messagequeue.memory :as memq]
            [unifydb.util :as util]))

(deftest memq-test
  (testing "Pubsub"
    (let [memq (memq/new)
          messages (atom [])]
      (q/subscribe memq
                   :testq
                   (fn [message]
                     (swap! messages #(conj %1 message))))
      (doseq [message [:first-message :second-message :third-message]]
        (q/publish memq :testq message)
        (Thread/sleep 1))
      (Thread/sleep 1)
      (is (= @messages [:first-message :second-message :third-message])))))
