(ns unifydb.unify
  (:require [unifydb.binding :refer [var? frame-binding extend-frame]]
            [unifydb.util :as util]))

(declare unify-match)

(defn depends-on? [exp var frame]
  "Determines if `exp` contains `var` in the context
   of `frame` by recursively walking through `exp`
   looking for `var`."
  (letfn [(tree-walk [node]
            (cond
              (var? node) (if (= var node)
                            true
                            (let [binding-value (frame-binding frame node)]
                              (if binding-value
                                (tree-walk binding-value)
                                false)))
              (util/not-nil-seq? node) (or (tree-walk (first node))
                                           (tree-walk (rest node)))
              :else false))]
    (tree-walk exp)))

(defn extend-if-possible [var val frame]
  "Extends the frame by binding `var` to `val` unless that
   results in an invalid state, in which case return :failed."
  (let [binding-value (frame-binding frame var)]
    (cond
      ;; If the var is already bound in the frame, attempt to unify its value with the new value
      binding-value (unify-match binding-value val frame)
      ;; If the value is a variable that is already bound, attempt to unify its value
      ;; with the variable currently being bound. If the value is an unbound variable,
      ;; bind the var to the val as normal (resulting in the var being bound to another variable)
      (var? val) (let [binding-value (frame-binding frame val)]
                   (if binding-value
                     (unify-match var binding-value frame)
                     (extend-frame frame var val)))
      ;; If the var is found somewhere in the val, fail, since it is not possible
      ;; to generally solve equations of the form y = <expression involving y>
      (depends-on? val var frame) :failed
      :else (extend-frame frame var val))))


(defn unify-match [pattern1 pattern2 frame]
  "Unifies `pattern1` with `pattern2` by binding variables
   in `frame` such that both patterns could have the same
   value. Some pattern variables in either pattern may remain
   unbound.
   
   For example, (unify-match '[?a ?b foo ?f] '[?c [?d bar] ?e ?f] {}) yields
   the new frame '{a ?c, b [?d bar], e foo}, leaving f unbound."
  (cond
    ;; If the unification has already failed, short-circuit
    (= frame :failed) :failed
    ;; If the patterns are the same, the frame already has the correct bindings
    (= pattern1 pattern2) frame
    ;; If pattern1 is a rest-pattern (e.g. [& ?rest]), unify its rest part
    ;; with pattern2 in the current frame
    (and (sequential? pattern1) (= (first pattern1) '&)) (unify-match (second pattern1) pattern2 frame)
    ;; If pattern1 is a variable, try to bind it to pattern2
    (var? pattern1) (extend-if-possible pattern1 pattern2 frame)
    ;; If pattern1 is not a variable but pattern2 is, try to bind pattern2 to pattern1
    (var? pattern2) (extend-if-possible pattern2 pattern1 frame)
    ;; If both patterns are lists, recursively unify them
    (and (sequential? pattern1) (sequential? pattern2)) (unify-match (rest pattern1)
                                                                     (rest pattern2)
                                                                     (unify-match (first pattern1)
                                                                                  (first pattern2)
                                                                                  frame))
    :else :failed))
