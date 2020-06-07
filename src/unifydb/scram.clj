(ns unifydb.scram
  (:require [unifydb.stringprep :as stringprep])
  (:import [java.util Base64]
           [javax.crypto Mac SecretKeyFactory]
           [javax.crypto.spec PBEKeySpec SecretKeySpec]
           [java.security MessageDigest SecureRandom]))

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

(defn hash-sha256
  "Computes the SHA-256 hash of the input."
  [input]
  (let [message-digest (MessageDigest/getInstance "SHA-256")]
    (.digest message-digest input)))

(defn bytes->chars
  "Converts a byte array to a char array."
  [input]
  (let [len (count input)
        res (char-array len)]
    (doseq [i (range len)]
      (aset-char res i (char (nth input i))))
    res))

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

(defn salt
  "Generates a cryptographically-secure random byte array.
  For deterministic output, pass a pre-seeded Random instance."
  ([random]
   (let [arr (byte-array 16)]
     (.nextBytes random arr)
     arr))
  ([] (salt (SecureRandom.))))

(defn encode
  "Base64-encodes `to-encode`, returning a string."
  [to-encode]
  (.encodeToString (Base64/getEncoder) to-encode))

(defn decode
  "Base64-decodes the encoded string `to-decode`, returning a byte array."
  [to-decode]
  (.decode (Base64/getDecoder) to-decode))
