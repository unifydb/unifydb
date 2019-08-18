(ns unifydb.binding
  (:require [unifydb.util :as util]))

(defn var? [exp]
  "Checks if `exp` is a variable.
   A variable is a sequence starting with '?."
  (and (sequential? exp) (= (first exp) '?)))

(defn var-name [var]
  (when (var? var) (second var)))

(defn frame-binding [frame var]
  "Returns the binding for `var` in `frame` or nil."
  (get frame var))

(defn extend-frame [frame var val]
  "Binds `var` to `val` in `frame`."
  (assoc frame (var-name var) val))

(defn instantiate [query frame unbound-var-handler]
  "Instantiates the `query` with the bindings in `frame`,
   calling `unbound-var-handler` if there exists a variable
   in the query with no binding in the frame."
  (letfn [(copy [exp]
            (cond
              (var? exp) (let [binding-value (get frame (var-name exp))]
                           (if binding-value
                             binding-value
                             (unbound-var-handler exp frame)))
              (util/not-nil-seq? exp) (cons (copy (first exp)) (copy (rest exp)))
              :else exp))]
    (copy query)))
