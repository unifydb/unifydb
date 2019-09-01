(ns unifydb.facts)

(defn fact-entity [fact]
  "Returns the entity component of a fact"
  (nth fact 0))

(defn fact-attribute [fact]
  "Returns the attribute component of a fact"
  (nth fact 1))

(defn fact-value [fact]
  "Returns the value component of a fact"
  (nth fact 2))

(defn fact-tx-id [fact]
  "Returns the transaction id component of a fact"
  (nth fact 3))

(defn fact-added? [fact]
  "Returns the added? component of a fact"
  (nth fact 4))
