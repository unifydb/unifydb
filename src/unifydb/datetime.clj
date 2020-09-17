(ns unifydb.datetime
  (:import [java.time ZonedDateTime ZoneOffset]
           [java.time.format DateTimeFormatter]))

(defn utc-now []
  (ZonedDateTime/now ZoneOffset/UTC))

(defn iso-format [zoned-date-time]
  (.format zoned-date-time DateTimeFormatter/ISO_DATE_TIME))
