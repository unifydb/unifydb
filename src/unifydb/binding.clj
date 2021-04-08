(ns unifydb.binding
  (:refer-clojure :exclude [var?])
  (:require [unifydb.util :as util]))

(defn var?
  "Checks if `exp` is a variable.
   A variable is a sequence starting with '?."
  [exp]
  (and (sequential? exp) (= (first exp) '?)))

(defn var-name [var]
  (when (var? var) (second var)))

(defn frame-binding
  "Returns the binding for `var` in `frame` or nil."
  [frame var]
  (get frame (var-name var)))

(defn extend-frame
  "Binds `var` to `val` in `frame`."
  [frame var val]
  (assoc frame (var-name var) val))

(defn instantiate
  "Instantiates the `query` with the bindings in `frame`,
   calling `unbound-var-handler` if there exists a variable
   in the query with no binding in the frame."
  [frame query unbound-var-handler]
  (letfn [(copy [exp]
            (cond
              (var? exp) (let [binding-value (frame-binding frame exp)]
                           (if (not (nil? binding-value))
                             (copy binding-value)
                             (unbound-var-handler exp frame)))
              (util/not-nil-seq? exp) (cons (copy (first exp)) (copy (rest exp)))
              :else exp))]
    (copy query)))
