(ns unifydb.rules)

(defn rule-conclusion [rule]
  (first rule))

(defn rule-body [rule]
  (or (second rule) [:always-true]))
