(ns unifydb.scram
  (:require [clojure.string :as string])
  (:import [javax.crypto Mac]
           [javax.crypto.spec SecretKeySpec]))

(defn hex-string
  "Returns the hexadecimal representation of `byte-array`
  as a string"
  [byte-array]
  (string/join (map #(format "%x" %) byte-array)))

(defn hmac
  "Calculates the HMAC signature of `string`
  given the `key`. Returns a hex string."
  [key string]
  (let [mac (Mac/getInstance "HMACSHA256")
        secret (SecretKeySpec. (.getBytes key)
                               (.getAlgorithm mac))]
    (->> (doto mac
           (.init secret)
           (.update (.getBytes string)))
         (.doFinal)
         (hex-string))))

;; TODO is this anywhere close?...
(defn hi
  "The Hi operation defined in RFC5802.
  See https://tools.ietf.org/html/rfc5802."
  [string salt i]
  (hex-string
   (apply map bit-xor
          (map #(.getBytes %)
               (reduce (fn [acc _]
                         (conj acc
                               (hmac string
                                     (str salt
                                          (or (last acc)
                                              (str (char 1)))))))
                       []
                       (range i))))))
