(ns unifydb.storage.btree
  "An implementation of a b-tree built on top of a KV store.
  WRITING IS NOT THREAD SAFE, only write in the single-threaded
  transactor."
  (:require [unifydb.comparison :as comparison]
            [unifydb.kvstore :as store])
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

(defn node-set
  [node index val]
  (update node :values #(assoc % index val)))

(defn node-range
  [node start end]
  (subvec (node-values node) start end))

(defn node-count
  [node]
  (count (node-values node)))

(defn node-insert
  "Returns a new node with `vals` inserted into `node` at `idx`."
  [node idx vals]
  (update node :values #(vec (concat (subvec % 0 idx) vals (subvec % idx)))))

(defn node-delete
  "Returns a new node with the values from `start` to `end` deleted from `node`."
  [node start end]
  (update node :values #(vec (concat (subvec % 0 start) (subvec % end)))))

(defn leaf?
  "The first value of a branch node is always a pointer"
  [node]
  (not (pointer? (node-get node 0))))

(defn cmp-trunc
  "Truncate vectors `first` and `second` to the same length then compare them."
  [first second]
  (if (and (vector? first) (vector? second))
    (let [min-length (min (count first) (count second))
          first-trunc (subvec first 0 min-length)
          second-trunc (subvec second 0 min-length)]
      (comparison/cc-cmp first-trunc second-trunc))
    (comparison/cc-cmp first second)))

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
  (let [first (if (:key first) (:key first) first)
        second (if (:key second) (:key second) second)
        zero-val (if (>= (count first) (count second)) 1 -1)
        comparison (cmp-trunc first second)]
    (if (zero? comparison) zero-val comparison)))

(defn search-key-<
  [first second]
  (< (compare-search-keys first second) 0))

(defn lower-bound
  "Returns the lower bound of the region in `node` prefixed with
  `prefix`."
  [node prefix]
  (letfn [(binary-search [node prefix left right]
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
    (binary-search node prefix 0 (node-count node))))

(defn lower-bound-exact
  "Like `lower-bound`, but allows for an exact match of `value`
  instead of treating it as a search key."
  [node value]
  (let [bound (lower-bound node value)
        idx (max 0 (dec bound))
        test-val (node-get node idx)
        test-val (if (:key test-val) (:key test-val) test-val)]
    (if (= (cmp-trunc test-val value) 0)
      idx
      bound)))

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
  (let [value (if (:key value) (:key value) value)]
    (= (comparison/cc-cmp (subvec value 0 (count prefix)) prefix) 0)))

(defn search
  "Searchs `tree`, returning all keys that start with `prefix`."
  [tree prefix]
  (letfn [(search-iter [node acc]
            (let [start-idx (lower-bound-exact node prefix)]
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
          b (least-value upper)
          a (if (:key a) (:key a) a)
          b (if (:key b) (:key b) b)]
      (find-common-prefix a b []))))

(defn insert-into
  "Inserts `value` into `node`, splitting the node if
  necessary. `value` should be a vector of values to insert; the first
  element determines where the values will be inserted.  Returns a map
  of node keys to new node values to be written to the store."
  [tree node value path-from-root]
  (letfn [(insert-into-iter [node value path-from-root acc]
            (let [node-key (peek path-from-root)
                  parent-path-from-root (pop path-from-root)
                  search-key (first value)
                  target-idx (lower-bound node search-key)
                  [lower upper] (map vec (split-at target-idx (node-values node)))
                  node (assoc node :values (vec (concat lower value upper)))]
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
                          lower-node (if (leaf? lower-node)
                                       (assoc lower-node :neighbor new-upper-key)
                                       lower-node)]
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
  "Inserts `value` into `tree` sorted by `key`, rebalancing the tree
  if necessary. Changes are not committed until a call to
  (kvstore/commit!) on the backing store object."
  [tree key value]
  (let [root (store/get (:store tree) (:root-key tree))
        val {:key key :value value}
        [leaf path] (find-leaf-for (:store tree) root val [(:root-key tree)])
        modifications (insert-into tree leaf [val] path)]
    (store/write-batch! (:store tree)
                        (map (fn [[key node]]
                               [:assoc! key node])
                             modifications))
    tree))

(defn next-sibling
  "Returns [index, next-greatest sibling key] or `nil` if the
  sibling does not exit."
  [tree path node]
  (let [parent-key (nth path (- (count path) 2))
        parent (store/get (:store tree) parent-key)
        upper-val (if (leaf? node)
                    (peek (node-values node))
                    (get (node-values node) (- (node-count node) 2)))
        sibling-idx (inc (lower-bound-exact parent upper-val))]
    (when-let [key (node-get parent sibling-idx)]
      [sibling-idx key])))

(defn prev-sibling
  "Returns [index, next-least sibling key] or `nil` if the sibling
  does not exist"
  [tree path node]
  (let [parent-key (nth path (- (count path) 2))
        parent (store/get (:store tree) parent-key)
        lower-val (if (leaf? node)
                    (first (node-values node))
                    (second (node-values node)))
        sibling-idx (- (lower-bound-exact parent lower-val) 3)]
    (when-let [key (node-get parent sibling-idx)]
      [sibling-idx key])))

(defn delete-from
  "Delete `value` from `node`, rebalancing the tree if necessary. Does
  not actually mutate `tree`, but returns a map of node keys to new
  node values."
  [tree node key path]
  (letfn [(delete-from-iter [node start end path acc]
            (let [node-key (peek path)
                  parent-key (nth path (- (count path) 2))
                  new-node (node-delete node start end)
                  min (cond
                        (= node-key (:root-key tree)) 0
                        (leaf? node) (quot (:order tree) 2)
                        :else (+ (quot (:order tree) 2)
                                 (- (quot (:order tree) 2) 1)))]
              (if (< (node-count new-node) min)
                (let [[prev-idx prev-key] (prev-sibling tree path node)
                      [next-idx next-key] (next-sibling tree path node)
                      parent (store/get (:store tree) parent-key)
                      prev-node (and prev-key (store/get (:store tree) prev-key))
                      next-node (and next-key
                                     (<= (node-count prev-node) min)
                                     (store/get (:store tree) next-key))]
                  (cond
                    ;; Pull value from previous sibling
                    (and prev-node
                         (> (node-count prev-node)
                            min))
                    (let [start (if (leaf? prev-node)
                                  (dec (node-count prev-node))
                                  (- (node-count prev-node) 2))
                          end (node-count prev-node)
                          vals (node-range prev-node start end)
                          new-node (node-insert new-node 0 vals)
                          new-prev (node-delete prev-node start end)
                          new-sep (separator-for tree new-prev new-node)
                          new-parent (node-set parent (inc prev-idx) new-sep)]
                      (assoc acc
                             node-key new-node
                             prev-key new-prev
                             parent-key new-parent))
                    ;; Pull value from next sibling
                    (and next-node
                         (> (node-count next-node)
                            min))
                    (let [start (if (leaf? next-node) 0 1)
                          end (if (leaf? next-node) 1 3)
                          vals (node-range next-node start end)
                          new-node (node-insert new-node
                                                (node-count new-node)
                                                vals)
                          new-next (node-delete next-node start end)
                          new-sep (separator-for tree new-node new-next)
                          new-parent (node-set parent (dec next-idx) new-sep)]
                      (assoc acc
                             node-key new-node
                             next-key new-next
                             parent-key new-parent))
                    ;; Merge node with sibling
                    :else (let [parent-is-root? (= (count path) 2)
                                [sib-idx sib] (if prev-node
                                                [prev-idx prev-node]
                                                [next-idx next-node])
                                sib-key (node-get parent sib-idx)
                                insert-idx (if prev-node
                                             (node-count sib)
                                             0)
                                delete-start (if prev-node
                                               (inc sib-idx)
                                               (dec sib-idx))
                                delete-end (if prev-node
                                             (+ sib-idx 3)
                                             (inc sib-idx))
                                merged (node-insert sib
                                                    insert-idx
                                                    (node-values new-node))
                                merged (if (and prev-node
                                                (leaf? merged)
                                                (:neighbor new-node))
                                         (assoc merged :neighbor (:neighbor new-node))
                                         merged)]
                            (if (and parent-is-root?
                                     (<= (node-count parent) 3))
                              ;; Merged node becomes new root
                              (assoc acc
                                     parent-key :delete
                                     node-key :delete
                                     (:root tree) merged)
                              ;; Recursively delete the old
                              ;; separator + pointer from parent
                              (recur parent
                                     delete-start
                                     delete-end
                                     (pop path)
                                     (assoc acc
                                            node-key :delete
                                            sib-key merged))))))
                (assoc acc node-key new-node))))]
    (let [idx (lower-bound-exact node key)
          test-val (node-get node idx)
          test-val (if (:key test-val) (:key test-val) test-val)]
      (if (not= test-val key)
        {}
        (delete-from-iter node idx (inc idx) path {})))))

(defn delete!
  "Deletes the value with `key` from `tree`, rebalancing the tree if
  necessary. Changes are not committed until a call to
  (kvstore/commi!) on the backing store object."
  [tree key]
  (let [root (store/get (:store tree) (:root-key tree))
        [leaf path] (find-leaf-for (:store tree) root key [(:root-key tree)])
        modifications (delete-from tree leaf key path)]
    (store/write-batch! (:store tree)
                        (map (fn [[key node]]
                               (if (= node :delete)
                                 [:dissoc! key]
                                 [:assoc! key node]))
                             modifications))
    tree))

(defn new!
  "Instantiates a new `store`-backed b-tree with order
  `order` whose root node is the value in the KV-store with key
  `root-key`. If the root node does not exist, it is enqueued for
  creation. Call (kvstore/commit!) to persist the new root node."
  ([store root-key order]
   (new! store root-key order generate-node-id))
  ([store root-key order id-generator]
   (when-not (store/contains? store root-key)
     (store/assoc! store root-key {:values []}))
   {:store store
    :order order
    :root-key root-key
    :id-generator id-generator}))
