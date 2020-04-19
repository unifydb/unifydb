(ns unifydb.stringprep-test
  (:require [unifydb.stringprep :as sp]
            [clojure.test :refer [deftest testing is]]))

(deftest test-saslprep
  (testing "Map to nothing"
    (is (= (sp/saslprep "I\u00ADX") "IX")))
  (testing "No transformation"
    (is (= (sp/saslprep "user") "user")))
  (testing "Case preserved"
    (is (= (sp/saslprep "USER") "USER"))
    (is (not= (sp/saslprep "USER") (sp/saslprep "user"))))
  (testing "ISO->NFKC"
    (is (= (sp/saslprep "\u00AA") "a")))
  (testing "NFKC"
    (is (= (sp/saslprep "\u2168") "IX"))
    (is (= (sp/saslprep "\u2168") (sp/saslprep "I\u00ADX"))))
  (testing "Prohibited characters"
    (is (thrown-with-msg? Exception #"Prohibited codepoints"
                          (sp/saslprep "\u0007"))))
  (testing "Bidi check"
    (is (thrown-with-msg? Exception #"Invalid bidirectionality"
                          (sp/saslprep "\u0627\u0031")))))
