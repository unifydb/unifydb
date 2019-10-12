(ns unifydb.messagequeue.memory-test
  (:require [clojure.test :refer [deftest testing is]]
            [manifold.stream :as s]
            [unifydb.messagequeue :as q]
            [unifydb.messagequeue.memory :as memq]
            [unifydb.util :refer [take-n!]]))

(defmacro defmemqtest [name & body]
  `(deftest ~name
     ~@(map
        (fn [expr]
          (if (and (sequential? expr) (= (first expr) 'testing))
            `(try ~expr (finally (memq/reset-state!)))
            (throw (IllegalArgumentException.
                    (str "defmemqtest top-level forms must be calls to (testing ...), found: "
                         (pr-str expr))))))
        body)))

(defmemqtest memq-test
  (testing "Pubsub"
    (let [memq {:type :memory}
          subscription (q/subscribe memq :testq)]
      (doseq [message [:first-message :second-message :third-message]]
        (q/publish memq :testq message))
      (is (= (take-n! 3 subscription ) [:first-message :second-message :third-message]))))
  (testing "Consumer groups"
    (let [memq {:type :memory}
          sub1 (q/subscribe memq :testq :group1)
          sub2 (q/subscribe memq :testq :group2)
          sub3 (q/subscribe memq :testq :group3)
          results (atom [])]
      (doseq [sub [sub1 sub2 sub3]]
        (s/consume (fn [msg] (swap! results #(conj % msg))) sub))
      (q/publish memq :testq "message1")
      (is (= @results ["message1"])))))
