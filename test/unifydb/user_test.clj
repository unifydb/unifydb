(ns unifydb.user-test
  (:require [clojure.test :refer [deftest is testing]]
            [unifydb.user :as u]))

(deftest test-make-user
  (testing "Valid username and password"
    (let [user (u/make-user "user" "pencil")]
      (is (= "user" (:unifydb/username user)))
      (is (string? (:unifydb/password user)))
      (is (not= "pencil" (:unifydb/password user)))
      (is (string? (:unifydb/salt user))))))
