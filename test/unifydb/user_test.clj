(ns unifydb.user-test
  (:require [clojure.test :refer [deftest is testing]]
            [unifydb.user :as u])
  (:import [java.util Random]))

(deftest test-make-user
  (let [seeded-rand (Random. 12345)]
    (testing "Valid username and password"
      (is (= (u/make-user "user" "pencil" seeded-rand)
             {:unifydb/username "user"
              :unifydb/salt "1iCfXDGzYYMiqdjueAfI6g=="
              :unifydb/i 4096
              :unifydb/server-key
              "lJAkDYrYIBgXilqFrA7i9D72FlwWTJXZhYTD0O7JulU="
              :unifydb/stored-key
              "SVBCFA/4M6QOwY+UWn5mQ00bsWkZnJrT9CrLzYgrXuA="})))
    (testing "Invalid password (fails normalization)"
      (is (thrown-with-msg? Exception #"Prohibited codepoints"
                            (u/make-user "user" "\u0007"))))))
