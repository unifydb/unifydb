(ns unifydb.scram
  (:require [clojure.string :as string])
  (:import [javax.crypto Mac]
           [javax.crypto.spec SecretKeySpec]))

(defn hmac
  "Calculates the HMAC signature of `string`
  given the `key`. Returns a hex string."
  [key string]
  (let [mac (Mac/getInstance "HMACSHA256")
        secret (SecretKeySpec. (.getBytes key)
                               (.getAlgorithm mac))]
    (-> (doto mac
          (.init secret)
          (.update (.getBytes string)))
        (.doFinal)
        (String. "UTF-8"))))

(defn hi
  "The Hi operation defined in RFC5802.
  See https://tools.ietf.org/html/rfc5802."
  [string salt i]
  (-> (->>
       (reduce (fn [acc _]
                 (conj acc
                       (hmac string
                             (str salt
                                  (or (last acc)
                                      (str (char 1)))))))
               []
               (range i))
       (map #(.getBytes %))
       (apply map bit-xor))
      (byte-array)
      (String. "UTF-8")))
