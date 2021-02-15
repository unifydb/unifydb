(ns unifydb.main
  (:require [unifydb.cli.unifydb :as unifydb]
            [unifydb.id]
            [unifydb.structlog :as structlog]))

(defn -main [& args]
  (structlog/init!)
  (let [{:keys [exit-message ok?]} (apply unifydb/unifydb args)]
    (when exit-message (println exit-message))
    (System/exit (if ok? 0 1))))
