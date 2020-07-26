(ns unifydb.http-auth-test
  (:require [clojure.string :as s]
            [clojure.test :refer [deftest is]]
            [clojure.test.check.generators :as gen]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [unifydb.http-auth :as auth]))

(defn not-includes? [subs s]
  (not (s/includes? s subs)))

(def auth-field-str
  "A generator for valid components of a SASL header"
  (gen/such-that (every-pred not-empty
                             (partial not-includes? "%")
                             (partial not-includes? " ")
                             (partial not-includes? ",")
                             (partial not-includes? "="))
                 gen/string-alphanumeric
                 50))

(deftest auth-fields->map
  (checking "that auth-fields->map parses the SASL header" 100
    [v (gen/let [n gen/nat
                 keys (gen/vector-distinct auth-field-str {:num-elements n})
                 vals (gen/vector auth-field-str n)
                 fmt-str (gen/return
                          (s/join "," (map (partial format "%s=%%s")
                                           keys)))]
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

(deftest map->auth-fields
  (checking "that map->auth-fields produces a SASL header" 100
    [v (gen/map (gen/fmap keyword auth-field-str)
                auth-field-str)]
    (let [fields-str (auth/map->auth-fields v)]
      (is (= (count v) (get (frequencies fields-str) \= 0)))
      (is (= (max (- (count v) 1) 0) (get (frequencies fields-str) \, 0)))
      (is (every? #(s/includes? fields-str (name %)) (keys v)))
      (is (every? #(s/includes? fields-str %) (vals v))))))

(deftest strip-sasl-prefix
  (checking "that strip-sasl-prefix strips the prefix" 100
    [v (gen/let [prefix (gen/such-that
                         (partial not-includes? " ")
                         gen/string-alphanumeric)
                 suffix gen/string-alphanumeric]
         (gen/return {:suffix suffix
                      :string (str prefix " " suffix)}))]
    (let [{:keys [suffix string]} v
          stripped (auth/strip-sasl-prefix string)]
      (is (= stripped suffix)))))

(deftest list->map
  (checking "that list->map turns its arguments into a map" 100
    [v (gen/let [l (gen/vector gen/any)
                 ks (gen/vector-distinct gen/keyword
                                         {:min-elements (count l)
                                          :max-elements (+ (count l) 20)})]
         (gen/return [l ks]))]
    (let [[l ks] v
          m (auth/list->map ks l)]
      (is (= (count m) (count l)))
      (is (= (frequencies (keys m)) (frequencies (take (count l) ks))))
      (is (= (frequencies (vals m)) (frequencies l))))))
