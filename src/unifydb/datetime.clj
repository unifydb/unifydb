(ns unifydb.datetime
  (:require [clojure.string :as str])
  (:import [java.time ZonedDateTime ZoneOffset]
           [java.time.format DateTimeFormatter]
           [java.time.temporal ChronoUnit]))

(defn utc-now []
  (ZonedDateTime/now ZoneOffset/UTC))

(defn iso-format [zoned-date-time]
  (.format zoned-date-time DateTimeFormatter/ISO_DATE_TIME))

(defn chrono-unit [unit]
  (ChronoUnit/valueOf (str/upper-case (name unit))))

(defn between [chrono-unit t1 t2]
  (.between chrono-unit t1 t2))

(defn from-iso [iso-str]
  (ZonedDateTime/parse iso-str DateTimeFormatter/ISO_DATE_TIME))
