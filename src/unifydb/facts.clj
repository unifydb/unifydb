(ns unifydb.facts)

(defn fact-entity
  "Returns the entity component of a fact"
  [fact]
  (nth fact 0))

(defn fact-attribute
  "Returns the attribute component of a fact"
  [fact]
  (nth fact 1))

(defn fact-value
  "Returns the value component of a fact"
  [fact]
  (nth fact 2))

(defn fact-tx-id
  "Returns the transaction id component of a fact"
  [fact]
  (nth fact 3))

(defn fact-added?
  "Returns the added? component of a fact"
  [fact]
  (nth fact 4))
