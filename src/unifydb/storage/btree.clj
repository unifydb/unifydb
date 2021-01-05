(ns unifydb.storage.btree
  "An implementation of a b-tree built on top of a KV store"
  (:require [unifydb.storage :as store]))

;; This is a pure b-tree - the nodes store complete facts (i.e. the
;; keys are the values). This means that a node (stored as a value in
;; the KV store) is a vector where each item is either a serialized
;; fact 5-tuple or the key to a child node

(defn insert!
  "Inserts `key` into `tree`, rebalancing the tree if necessary."
  [tree key]
  ;;
  )

(defn delete!
  "Deletes `key` from `tree`, rebalancing the tree if necessary."
  [tree key])

(defn pointer?
  "Pointers in b-tree nodes are strings, whereas values are vectors."
  [val]
  (string? val))

(defn lower-bound
  "Returns the lower bound of the region in `node` prefixed with
  `prefix`. Returns `nil` if no values in `node` have the `prefix`."
  [node prefix]
  (letfn [(search [node prefix left right found?]
            (if (>= left right)
              (and found? left)
              (let [middle (+ left (quot (- right left) 2))
                    middle (if (pointer? (get node middle))
                             (inc middle)
                             middle)
                    val (subvec (get node middle) 0 (count prefix))]
                (cond
                  (>= middle right) (and found? middle)
                  (neg? (compare val prefix)) (recur node prefix (+ middle 1) right found?)
                  (zero? (compare val prefix)) (recur node prefix left middle true)
                  (pos? (compare val prefix)) (recur node prefix left middle found?)))))]
    (search node prefix 0 (count node) nil)))

(defn upper-bound
  "Returns the upper bound of the region in `node` prefixed with
  `prefix`. Returns `nil` if no values in `node` have the
  `previx`. Note that the return value is actually the index of the
  last prefixed value + 1, so that (subvec node lower-bound
  upper-bound) returns the prefixed section."
  [node prefix]
  (letfn [(search [node prefix left right found?]
            (if (>= left right)
              (and found? left)
              (let [middle (+ left (quot (- right left) 2))
                    middle (if (pointer? (get node middle))
                             (inc middle)
                             middle)
                    val (subvec (get node middle) 0 (count prefix))]
                (cond
                  (>= middle right) (and found? middle)
                  (neg? (compare prefix val)) (recur node prefix left middle found?)
                  (zero? (compare prefix val)) (recur node prefix (+ middle 1) right true)
                  (pos? (compare prefix val)) (recur node prefix (+ middle 1) right found?)))))]
    (search node prefix 0 (count node) nil)))


(defn traverse-iter
  [store node prefix acc]
  (let [lower-bound (lower-bound node prefix)
        upper-bound (upper-bound node prefix)
        lower-bound (if (pointer? (get node (dec lower-bound)))
                      (dec lower-bound)
                      lower-bound)
        upper-bound (if (pointer? (get node upper-bound))
                      (inc upper-bound)
                      upper-bound)]
    (if (and lower-bound upper-bound)
      (vec
       (concat acc
               (mapcat (fn [val]
                         (if (pointer? val)
                           (let [child (store/get store val)]
                             (traverse-iter store child prefix acc))
                           [val]))
                       (subvec node lower-bound upper-bound))))
      acc)))

(defn traverse
  "Traverses `tree`, returning all keys that start with `prefix`."
  [tree prefix]
  ;; start at root node

  ;; find all keys in root node that start with the prefix, plus any
  ;; pointers immediately before the first prefixed key and
  ;; immediately after the last prefixed key

  ;; fetch the children nodes if we found any pointers

  ;; recurse into each child and repeat the process
  (let [root (store/get (:store tree) (:root-key tree))]
    (traverse-iter (:store tree) root prefix [])))

(defn new!
  "Instantiates a new `store`-backed b-tree with order
  `order` whose root node is the value in the KV-store with key
  `root-key`. If the root node does not exist, it is created."
  [store root-key order]
  (when-not (store/contains? store root-key)
    (store/assoc! store root-key []))
  {:store store
   :order order
   :root-key root-key})
