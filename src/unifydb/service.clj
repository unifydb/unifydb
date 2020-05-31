(ns unifydb.service)

(defprotocol IService
  (start! [self])
  (stop! [self]))
