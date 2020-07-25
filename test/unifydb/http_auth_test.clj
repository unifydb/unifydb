(ns unifydb.http-auth-test
  (:require [clojure.string :as s]
            [clojure.test :refer [deftest is]]
            [clojure.test.check.generators :as gen]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [unifydb.http-auth :as auth]))

(defn not-includes? [subs s]
  (not (s/includes? s subs)))


(deftest auth-fields->map
  (checking "that auth-fields->map parses the SAML header" 100
    [v (gen/let [n gen/nat
                 keys (gen/vector-distinct
                       (gen/such-that (every-pred not-empty
                                                  (partial not-includes? "%")
                                                  (partial not-includes? " ")
                                                  (partial not-includes? ",")
                                                  (partial not-includes? "="))
                                      gen/string
                                      50)
                       {:num-elements n})
                 vals (gen/vector
                       (gen/such-that (every-pred not-empty
                                                  (partial not-includes? "%")
                                                  (partial not-includes? " ")
                                                  (partial not-includes? ",")
                                                  (partial not-includes? "="))
                                      gen/string
                                      50)
                       n)
                 fmt-str (gen/return (str "SASL "
                                          (s/join ","
                                                  (map (partial format "%s=%%s")
                                                       keys))))]
         (gen/return
          {:n n
           :header (apply format fmt-str vals)
           :the-keys keys
           :the-vals vals}))]
    (let [{:keys [n header the-keys the-vals]} v
          parsed (auth/auth-fields->map header)]
      (is (= n (count parsed)))
      (is (= (frequencies (keys parsed))
             (frequencies (map keyword the-keys))))
      (is (= (frequencies (vals parsed))
             (frequencies the-vals))))))
