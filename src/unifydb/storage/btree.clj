(ns unifydb.storage.btree
  "An implementation of a b-tree built on top of a KV store"
  (:require [unifydb.storage :as store]))

;; This is a pure b-tree - the nodes store complete facts (i.e. the
;; keys are the values). This means that a node (stored as a value in
;; the KV store) is a vector where each item is either a serialized
;; fact 5-tuple or the key to a child node. It may be helpful to think
;; of this as an implementation of a sorted set backed by an external
;; key-value store.

(defn pointer?
  "Pointers in b-tree nodes are strings, whereas values are vectors."
  [val]
  (string? val))

(defn compare-to-prefix
  "Given a partial fact-tuple `prefix` and a fact-tuple `val`, returns
  1 if the prefix comes after the value, 0 if the value is prefixed by
  the prefix, and -1 if the prefix comes before the value.

  For example:
      (compare-to-prefix [\"a\" \"b\"] [\"b\" \"c\" \"d\"]) ;; => -1
      (compare-to-prefix [\"b\" \"c\"] [\"b\" \"c\" \"d\"]) ;; => 0
      (compare-to-prefix [\"c\" \"a\"] [\"b\" \"c\" \"d\"]) ;; => 1"

  [prefix val]
  (let [val-prefix (subvec val 0 (count prefix))]
    (compare prefix val-prefix)))

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
                    val (get node middle)]
                (cond
                  (>= middle right) (and found? middle)
                  (pos? (compare-to-prefix prefix val)) (recur node prefix (+ middle 1) right found?)
                  (zero? (compare-to-prefix prefix val)) (recur node prefix left middle true)
                  (neg? (compare-to-prefix prefix val)) (recur node prefix left middle found?)))))]
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
                    val (get node middle)]
                (cond
                  (>= middle right) (and found? middle)
                  (neg? (compare-to-prefix prefix val)) (recur node prefix left middle found?)
                  (zero? (compare-to-prefix prefix val)) (recur node prefix (+ middle 1) right true)
                  (pos? (compare-to-prefix prefix val)) (recur node prefix
                                                               (+ middle 1)
                                                               right
                                                               found?)))))]
    (search node prefix 0 (count node) nil)))


(defn search-iter
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
                             (search-iter store child prefix acc))
                           [val]))
                       (subvec node lower-bound upper-bound))))
      acc)))

(defn search
  "Searchs `tree`, returning all keys that start with `prefix`."
  [tree prefix]
  (let [root (store/get (:store tree) (:root-key tree))]
    (search-iter (:store tree) root prefix [])))

(defn find-node-for
  [node value]
  ;; Base case: value is already in node or node has no children that
  ;; could contain the value
  ;;
  ;; Otherwise, recurse into the child that could contain the value
  ;; Return the found node plus the path we took to get there?
  )

(defn insert!
  "Inserts `key` into `tree`, rebalancing the tree if necessary."
  [tree value]
  ;; Find the node that the value belongs in by traversing the tree
  ;; using binary search on the value to be inserted until we find a
  ;; node with no children
  ;;
  ;; If there is room in the node, just insert the value
  ;;
  ;; If there's not room in the node (its length is >= 2*order - 1,
  ;; e.g. it contains <order> child pointers and <order - 1>
  ;; elements), split the current node into two nodes, one of which
  ;; will contain the new value. Then update the parent node to have a
  ;; pointer to the new node and the central value of the old node
  ;;
  ;; If this operation causes the parent node to be too big, repeat the operation
  ;;
  ;; If we recurse all the way to the root, it splits in two as well
  ;; and a new root node is created
  (let [root (store/get (:store tree) (:root-key tree))]
    ))

(defn delete!
  "Deletes `key` from `tree`, rebalancing the tree if necessary."
  [tree key])

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
