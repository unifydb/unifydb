(ns unifydb.storage.btree
  "An implementation of a b-tree built on top of a KV store"
  (:require [unifydb.storage :as store])
  (:import [java.util UUID]))

(defn pointer?
  "Pointers in b-tree nodes are strings, whereas values are vectors."
  [val]
  (string? val))

(defn node-values
  [node]
  (:values node))

(defn node-neighbor
  [node]
  (:neighbor node))

(defn node-get
  [node index]
  (get (node-values node) index))

(defn node-count
  [node]
  (count (node-values node)))

(defn leaf?
  "The first value of a branch node is always a pointer"
  [node]
  (not (pointer? (node-get node 0))))

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
                    middle (if (pointer? (node-get node middle))
                             (inc middle)
                             middle)
                    val (node-get node middle)]
                (cond
                  (>= middle right) middle
                  (pos? (compare-search-keys prefix val)) (recur node prefix (+ middle 1) right)
                  (zero? (compare-search-keys prefix val)) (recur node prefix left middle)
                  (neg? (compare-search-keys prefix val)) (recur node prefix left middle)))))]
    (search node prefix 0 (node-count node))))

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
                    middle (if (pointer? (node-get node middle))
                             (inc middle)
                             middle)
                    val (node-get node middle)]
                (cond
                  (>= middle right) middle
                  (neg? (compare-search-keys prefix val)) (recur node prefix left middle)
                  (zero? (compare-search-keys prefix val)) (recur node prefix (+ middle 1) right)
                  (pos? (compare-search-keys prefix val)) (recur node prefix
                                                                 (+ middle 1)
                                                                 right)))))]
    (search node prefix 0 (node-count node))))

(defn find-leaf-for
  "Returns a vector whose first element is the smallest leaf node that
  could contain `value` and whose second element is a vector of the
  path of node pointer keys we took to arrive at the leaf (most recent
  pointer key last)"
  [store node value path]
  (if (leaf? node)
    [node path]
    (let [lower-sep-idx (lower-bound node value)
          child-ptr-idx (cond
                          (>= lower-sep-idx
                              (node-count node)) (dec (node-count node))
                          (search-key-<
                           value
                           (node-get node lower-sep-idx)) (dec lower-sep-idx)
                          :else (inc lower-sep-idx))
          child-ptr (node-get node child-ptr-idx)]
      (recur store
             (store/get store child-ptr)
             value
             (conj path child-ptr)))))

(defn prefixed-by?
  [value prefix]
  (= (subvec value 0 (count prefix)) prefix))

(defn search
  "Searchs `tree`, returning all keys that start with `prefix`."
  [tree prefix]
  (letfn [(search-iter [node acc]
            (let [start-idx (lower-bound node prefix)]
              (if (and (node-neighbor node)
                       (prefixed-by? (peek (node-values node)) prefix))
                (recur (store/get (:store tree) (node-neighbor node))
                       (concat acc (subvec (node-values node) start-idx)))
                (concat acc (for [val (subvec (node-values node) start-idx)
                                  :while (prefixed-by? val prefix)] val)))))]
    (let [root (store/get (:store tree) (:root-key tree))
          [leaf _] (find-leaf-for (:store tree) root prefix [(:root-key tree)])]
      (vec (search-iter leaf [])))))

(defn generate-node-id
  []
  (str (UUID/randomUUID)))

(defn separator-for
  "Returns the shortest search key that separates the nodes `lower`
  and `upper`."
  [tree lower upper]
  (letfn [(greatest-value [node]
            (if (not (pointer? (peek (node-values node))))
              (peek (node-values node))
              (let [child (store/get (:store tree) (peek (node-values node)))]
                (recur child))))
          (least-value [node]
            (if (not (pointer? (first (node-values node))))
              (first (node-values node))
              (let [child (store/get (:store tree) (first (node-values node)))]
                (recur child))))
          (find-common-prefix [a b acc]
            (if (not= (first a) (first b))
              (if (empty? acc)
                [(first b)]
                (conj acc (first b)))
              (recur (rest a) (rest b) (conj acc (first b)))))]
    (let [a (greatest-value lower)
          b (least-value upper)]
      (find-common-prefix a b []))))

(defn insert-into
  "Inserts `value` into `node`, splitting the node if
  necessary. `value` should be a vector of values to insert; the first
  element determines where the values will be inserted.  Returns a map
  of node keys to new node values to be written to the store."
  [tree node value path-from-root]
  ;; Base case: path-from-root is empty, meaning we are at the root
  ;;
  ;; Find index at which to insert value and put it in
  ;;
  ;; If the node is now too big, split it and use insert-into! to put
  ;; a separator key in the parent
  (letfn [(insert-into-iter [node value path-from-root acc]
            (let [node-key (peek path-from-root)
                  parent-path-from-root (pop path-from-root)
                  search-key (first value)
                  target-idx (lower-bound node search-key)
                  [lower upper] (map vec (split-at target-idx (node-values node)))
                  node (if (= (node-get node target-idx) value)
                         node
                         (assoc node :values (vec (concat lower value upper))))]
              (if (or (and (leaf? node)
                           (> (node-count node) (- (:order tree) 1)))
                      (> (node-count node) (- (* 2 (:order tree)) 1)))
                (let [[lower upper] (if (leaf? node)
                                      (map vec (split-at (quot (node-count node) 2)
                                                         (node-values node)))
                                      [(subvec (node-values node)
                                               0
                                               (quot (node-count node) 2))
                                       (subvec (node-values node)
                                               (+ (quot (node-count node) 2) 1))])
                      new-key ((:id-generator tree))
                      lower-node (if (leaf? node)
                                   {:values lower :neighbor new-key}
                                   {:values lower})
                      upper-node {:values upper}
                      separator (separator-for tree lower-node upper-node)
                      parent-id (peek parent-path-from-root)
                      parent (store/get (:store tree) parent-id)]
                  (if (empty? parent-path-from-root)
                    (let [new-upper-key ((:id-generator tree))
                          new-root [new-key separator new-upper-key]
                          lower-node (assoc lower-node :neighbor new-upper-key)]
                      (assoc acc
                             (:root-key tree) {:values new-root}
                             new-key lower-node
                             new-upper-key upper-node))
                    (assoc (insert-into-iter parent [separator new-key] parent-path-from-root acc)
                           node-key lower-node
                           new-key upper-node)))
                (assoc acc node-key node))))]
    (insert-into-iter node value path-from-root {})))

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
  (let [root (store/get (:store tree) (:root-key tree))
        [leaf path] (find-leaf-for (:store tree) root value [(:root-key tree)])
        modifications (insert-into tree leaf [value] path)]
    (doseq [[key node] modifications]
      (store/assoc! (:store tree) key node))
    tree))

(defn delete!
  "Deletes `key` from `tree`, rebalancing the tree if necessary."
  [tree key])

(defn new!
  "Instantiates a new `store`-backed b-tree with order
  `order` whose root node is the value in the KV-store with key
  `root-key`. If the root node does not exist, it is created."
  ([store root-key order]
   (new! store root-key order generate-node-id))
  ([store root-key order id-generator]
   (when-not (store/contains? store root-key)
     (store/assoc! store root-key {:values []}))
   {:store store
    :order order
    :root-key root-key
    :id-generator id-generator}))
