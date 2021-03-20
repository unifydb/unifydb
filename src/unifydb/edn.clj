(ns unifydb.edn
  (:refer-clojure :exclude [read-string])
  (:require [clojure.edn :as edn]
            [unifydb.id :as id]))

(defn read-string [s]
  (edn/read-string {:readers {'unifydb/id id/id}} s))
