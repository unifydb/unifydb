(ns unifydb.storage.btree
  "An implementation of a b-tree built on top of a KV store"
  (:require [unifydb.storage :as store]))

(defn pointer?
  "Pointers in b-tree nodes are strings, whereas values are vectors."
  [val]
  (string? val))

(defn leaf?
  "The first value of a branch node is always a pointer"
  [node]
  (not (pointer? (get node 0))))

(defn compare-search-keys
  "Given a search key `first` and a search key `second`, returns 1 if
  first comes after second and -1 if first comes before second. Note
  that this never returns zero: if the search keys are the same, it
  returns 1 since a separator keys guarantees that the right child
  pointer contains all values greater than or equal to it.

  For example:
      (compare-search-keys [\"a\" \"b\" \"c\"] [\"a\" \"b\"])       ;; => 1
      (compare-search-keys [\"a\" \"b\"] [\"a\" \"b\" \"c\"])       ;; => -1
      (compare-search-keys [\"a\" \"b\"] [\"b\" \"c\" \"d\"])       ;; => -1
      (compare-search-keys [\"b\" \"c\" \"d\"] [\"b\" \"c\" \"d\"]) ;; => 1
      (compare-search-keys [\"c\" \"a\" \"b\"] [\"b\" \"c\"])       ;; => 1"

  [first second]
  (let [min-length (min (count first) (count second))
        first-trunc (subvec first 0 min-length)
        second-trunc (subvec second 0 min-length)
        zero-val (if (>= (count first) (count second)) 1 -1)
        comparison (compare first-trunc second-trunc)]
    (if (zero? comparison) zero-val comparison)))

(defn search-key-<
  [first second]
  (< (compare-search-keys first second) 0))

(defn lower-bound
  "Returns the lower bound of the region in `node` prefixed with
  `prefix`."
  [node prefix]
  (letfn [(search [node prefix left right]
            (if (>= left right)
              left
              (let [middle (+ left (quot (- right left) 2))
                    middle (if (pointer? (get node middle))
                             (inc middle)
                             middle)
                    val (get node middle)]
                (cond
                  (>= middle right) middle
                  (pos? (compare-search-keys prefix val)) (recur node prefix (+ middle 1) right)
                  (zero? (compare-search-keys prefix val)) (recur node prefix left middle)
                  (neg? (compare-search-keys prefix val)) (recur node prefix left middle)))))]
    (search node prefix 0 (count node))))

(defn upper-bound
  "Returns the upper bound of the region in `node` prefixed with
  `prefix`. Returns `nil` if no values in `node` have the
  `previx`. Note that the return value is actually the index of the
  last prefixed value + 1, so that (subvec node lower-bound
  upper-bound) returns the prefixed section."
  [node prefix]
  (letfn [(search [node prefix left right]
            (if (>= left right)
              left
              (let [middle (+ left (quot (- right left) 2))
                    middle (if (pointer? (get node middle))
                             (inc middle)
                             middle)
                    val (get node middle)]
                (cond
                  (>= middle right) middle
                  (neg? (compare-search-keys prefix val)) (recur node prefix left middle)
                  (zero? (compare-search-keys prefix val)) (recur node prefix (+ middle 1) right)
                  (pos? (compare-search-keys prefix val)) (recur node prefix
                                                                 (+ middle 1)
                                                                 right)))))]
    (search node prefix 0 (count node))))


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

(defn find-leaf-for
  "Returns a vector whose first element is the smallest leaf node that
  could contain `value` and whose second element is a vector of the
  path of node pointer keys we took to arrive at the leaf (most recent
  pointer key last)"
  [store node value]
  (letfn [(find-leaf-iter [node value path]
            (if (leaf? node)
              [node path]
              (let [lower-sep-idx (lower-bound node value)
                    child-ptr-idx (cond
                                    (>= lower-sep-idx (count node)) (dec (count node))
                                    (search-key-<
                                     value
                                     (get node lower-sep-idx)) (dec lower-sep-idx)
                                    :else (inc lower-sep-idx))
                    child-ptr (get node child-ptr-idx)]
                (recur (store/get store child-ptr)
                       value
                       (conj path child-ptr)))))]
    (find-leaf-iter node value [])))

(defn insert!
  "Inserts `key` into `tree`, rebalancing the tree if necessary."
  [tree value]
  ;; First search the tree to find the leaf that the value belongs in
  ;;
  ;; If there is space in that leaf, simply insert the value at the
  ;; correct point in the leaf and write the leaf node back to the K/V
  ;; store
  ;;
  ;; If the leaf node is full after insertion of the new value (length
  ;; > 2*order - 1), split the leaf node on the mid point into two
  ;; leaf nodes, determine the shortest possible separator key between
  ;; the two nodes, and insert that separator key as well as a pointer
  ;; to the new leaf node at the appropriate place in the parent
  ;; node. The original leaf node can be reused for one of the new
  ;; leaf nodes. This step requires a pointer to the parent node.
  ;;
  ;; If the parent node is full after insertion of the new separator
  ;; key (length > 2*order - 1), then it too splits and a new
  ;; separator key/child pointer is put into its parent. This process
  ;; repeats recursively until the parent has space, or we reach the
  ;; root node. If it is the root node, a new root node is created -
  ;; in this special case, the new root node needs to have the root
  ;; pointer set to point to it and the old root node needs a new
  ;; pointer generated.
  ;;
  ;; (insert!) should return the updated btree object
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
