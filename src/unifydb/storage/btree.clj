(ns unifydb.storage.btree
  "An implementation of a b-tree built on top of a KV store"
  (:require [unifydb.storage :as store]))

(defn insert!
  "Inserts `key` into `tree`, rebalancing the tree if necessary."
  [tree key])

(defn delete!
  "Deletes `key` from `tree`, rebalancing the tree if necessary."
  [tree key])

(defn traverse
  "Traverses `tree`, returning all keys that start with `prefix`."
  [tree prefix])
