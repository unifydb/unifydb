(ns unifydb.scram
  (:require [clojure.string :as string])
  (:import [javax.crypto Mac]
           [javax.crypto.spec SecretKeySpec]))

(defn hmac
  "Calculates the HMAC signature of `string`
  given the `key`. `string` and `key` should be
  byte arrays. Returns a byte array."
  [key string]
  (let [mac (Mac/getInstance "HMACSHA256")
        secret (SecretKeySpec. key
                               (.getAlgorithm mac))]
    (-> (doto mac
          (.init secret)
          (.update string))
        (.doFinal))))

(defn bit-xor-array
  "Computes the piecewise exclusive-or of the input byte arrays.
  Returns the result as a byte-array.

  All input byte arrays must be the same length."
  [arr1 arr2 & arrs]
  (let [len (count arr1)
        res (byte-array len)]
    (doseq [arr (into arrs [arr2 arr1])
            i (range len)]
      (aset-byte res i (bit-xor (nth res i) (nth arr i))))
    res))

(defn hi
  "The Hi operation defined in RFC5802.
  See https://tools.ietf.org/html/rfc5802."
  [string salt i]
  (let [arrs (reduce
              (fn [acc _]
                (conj acc
                      (hmac string
                            (or (last acc)
                                (byte-array
                                 (apply concat
                                        [salt
                                         ;; 1 as a 4-byte int:
                                         [0 0 0 0 0 0 0 0
                                          0 0 0 0 0 0 0 0
                                          0 0 0 0 0 0 0 0
                                          0 0 0 0 0 0 0 1]]))))))
              []
              (range i))]
    (apply bit-xor-array arrs)))
