(ns unifydb.service
  (:require [clojure.tools.logging :as log]
            [manifold.stream :as s]
            [unifydb.messagequeue :as queue]
            [unifydb.util :as util]))

(defprotocol IService
  (start! [self])
  (stop! [self]))
