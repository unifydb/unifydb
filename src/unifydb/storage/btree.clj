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

(defn prefixed?
  "True if `val` starts with `prefix`."
  [prefix val]
  (when val
    (and (vector? val)
         ;; TODO will this need to change to support string-matching queries?
         ;; I.e. what if val is [:attr "foobar" 123] and we are looking for
         ;; [:attr "foo%"] (or whatever that syntax will look like)?
         (= prefix (subvec val 0 (count prefix))))))

(defn boundary?
  "True if `n` is prefixed with `prefix` but (next-fn n) is not."
  [node prefix n next-fn]
  (and (prefixed? prefix (get node n))
       (not (prefixed? prefix (get node (next-fn n))))))

(defn find-traverse-bounds
  "Returns the lower and upper bounds of indices into `node` in which
  values start with the `prefix`. Returns nil if the prefix isn't
  found in the node at all."
  [node prefix lower-bound upper-bound interval]
  ;; if lower and upper are both on boundaries, return them
  ;; if lower is on a boundary but upper is not, upper = upper - i and halve i
  ;; if upper is on a boundary but lower is not, lower = lower + i and halve i
  ;; if neither upper nor lower are on boundaries, upper = upper - i, lower = lower + i, halve i
  ;; if upper <= lower, return nil (prefix was not found)
  (cond
    (<= upper-bound lower-bound) nil
    ;; This is always returning true when one of the bounds is on a pointer
    ;; I think we need to treat pointers as never on boundaries, then expand the boundaries by 1
    ;; at the end if lower- and greater-than indices are pointers
    (and (boundary? node prefix lower-bound dec)
         (boundary? node prefix (- upper-bound 1) inc))
    (let [lower (if (pointer? (get node (dec lower-bound)))
                  (dec lower-bound)
                  lower-bound)
          upper (if (pointer? (get node (inc upper-bound)))
                  (inc upper-bound)
                  upper-bound)]
      [lower upper])
    :else (let [next-lower (if (boundary? node prefix lower-bound dec)
                             lower-bound
                             (+ lower-bound interval))
                next-upper (if (boundary? node prefix upper-bound inc)
                             upper-bound
                             (- upper-bound interval))
                next-interval (/ interval 2)]
            (recur node prefix next-lower next-upper next-interval))))

(defn traverse-iter
  [store node prefix acc]
  (if-let [[lower-bound upper-bound] (find-traverse-bounds node
                                                           prefix
                                                           0
                                                           (count node)
                                                           (/ (count node) 2))]
    (vec
     (concat acc
             (mapcat (fn [val]
                       (if (pointer? val)
                         (let [child (store/get store val)]
                           (traverse-iter store child prefix acc))
                         [val]))
                     (subvec node lower-bound upper-bound))))
    acc))

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
