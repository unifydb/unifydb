(ns unifydb.scram
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [unifydb.stringprep :as stringprep])
  (:import [java.util Base64]
           [javax.crypto Mac SecretKeyFactory]
           [javax.crypto.spec PBEKeySpec SecretKeySpec]
           [java.security MessageDigest]))

(s/fdef hmac
  :args (s/cat :key bytes? :string bytes)
  :ret bytes?)
(defn hmac
  "Calculates the HMAC signature of `string`
  given the `key`. `string` and `key` should be
  byte arrays. Returns a byte array."
  [key string]
  (let [mac (Mac/getInstance "HMACSHA256")
        secret (SecretKeySpec. key
                               (.getAlgorithm mac))]
    (.doFinal
     (doto mac
       (.init secret)
       (.update string)))))

(s/fdef bit-xor-array
  :args (s/and (s/cat :arr1 bytes? :arr2 bytes? :arrs (s/* bytes?))
               #(apply =
                       (count (:arr1 %))
                       (count (:arr2 %))
                       (map count (:arrs %))))
  :ret bytes?
  :fn #(apply =
              (count (-> % :args :arr1))
              (count (-> % :args :arr2))
              (count (:ret %))
              (map count (-> % :args :arrs))))
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

(s/fdef hash-sha256
  :args (s/cat :input bytes?)
  :ret bytes?)
(defn hash-sha256
  "Computes the SHA-256 hash of the input."
  [input]
  (let [message-digest (MessageDigest/getInstance "SHA-256")]
    (.digest input)))

(defn bytes->chars
  "Converts a byte array to a char array."
  [input]
  (let [len (count input)
        res (char-array len)]
    (doseq [i (range len)]
      (aset-char res i (char (nth input i))))
    res))

(s/fdef pbk-df2-hmac-sha256
  :args (s/cat :string bytes? :salt bytes? :i int?)
  :ret bytes?)
(defn pbk-df2-hmac-sha256
  "The H^i operation defined in RFC5802.
  See https://tools.ietf.org/html/rfc5802."
  [string salt i]
  (let [secret-factory (SecretKeyFactory/getInstance "PBKDF2WithHmacSHA256")
        spec (PBEKeySpec. (bytes->chars string) salt i 32)]
    (.getEncoded (.generateSecret secret-factory spec))))

(defn normalize
  "Normalizes a Unicode string."
  [input]
  ;; TODO handle error case here?
  (stringprep/saslprep input))

(s/fdef encode
  :args (s/cat :to-encode bytes?)
  :ret string?)
(defn encode
  "Base64-encodes `to-encode`, returning a string."
  [to-encode]
  (.encodeToString (Base64/getEncoder) (.getBytes to-encode)))

(s/fdef decode
  :args (s/cat :to-decode string?)
  :ret bytes?)
(defn decode
  "Base64-decodes the encoded string `to-decode`, returning a byte array."
  [to-decode]
  (.decode (Base64/getDecoder) to-decode))
