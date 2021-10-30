(ns unifydb.query.pull
  (:require [clojure.string :as str]
            [unifydb.binding :as bind]
            [unifydb.id :refer [id?]]
            [unifydb.util :as util :refer [deep-merge]]))

(defn pull-exp? [exp]
  (and (list? exp) (= (first exp) 'pull)))

(defn pull-exp [entity query]
  (list 'pull entity query))

(defn pull-entity
  "Extracts the entity part of a pull expression."
  [pull-exp]
  (second pull-exp))

(defn pull-query
  "Extracts the query part of a pull expression"
  [pull-exp]
  (nth pull-exp 2))

;; ---------
;; Overview of the strategy here:
;; 1. Replace all instances of (pull) expressions in the parent query
;;    with bindings that will return entity IDs
;; 2. Execute the parent query. Now we have a list of entity ids to
;;    pull for each pull expression
;; 3. Transform the pull expressions into real queries and execute
;;    them, constraining each pull query using the list of entity IDs
;; 4. Collect the results and fill in the result of the parent query
;;
;; So we end up doing one subquery per pull expression in the parent
;; query.

(defn var-sym [id type]
  (symbol (str "?" id "-" type)))

(defn make-pull-query-accum
  "Accumulator function to recursively build a pull query."
  [acc depth current-id current-ent-var pull-exp id-gen]
  (cond
    (keyword? pull-exp) (assoc acc
                               :where [current-ent-var
                                       pull-exp
                                       (var-sym current-id "val")]
                               :depth (assoc (:depth acc)
                                             current-id depth)
                               :find (conj (:find acc)
                                           (var-sym current-id "attr")
                                           (var-sym current-id "val")))
    (map? pull-exp) (let [attr (first (keys pull-exp))
                          sub-exp (get pull-exp attr)
                          new-ent-id (id-gen)
                          new-depth (inc depth)
                          subquery (make-pull-query-accum acc
                                                          new-depth
                                                          new-ent-id
                                                          (var-sym current-id "val")
                                                          sub-exp
                                                          id-gen)]
                      (assoc acc
                             :where [:and
                                     [current-ent-var
                                      attr
                                      (var-sym current-id "val")]
                                     (:where subquery)]
                             :find (concat (:find acc) (:find subquery))
                             :depth (merge (:depth acc) (:depth subquery))))
    (vector? pull-exp) (let [subqueries (map #(make-pull-query-accum acc
                                                                     depth
                                                                     current-id
                                                                     current-ent-var
                                                                     %
                                                                     id-gen)
                                             pull-exp)]
                         (assoc acc
                                :where `[:and
                                         [:or
                                          ~@(map :where subqueries)]
                                         [~current-ent-var
                                          ~(var-sym current-id "attr")
                                          ~(var-sym current-id "val")]]
                                :find (set (mapcat :find subqueries))
                                :depth (apply merge (:depth acc) (map :depth subqueries))))))

(defn make-pull-query
  "Generates a pull query given the input pull expression and a list
  of entity ids to pull."
  ([pull-exp ids]
   (make-pull-query pull-exp ids gensym))
  ([pull-exp ids id-gen]
   (let [ent-id (id-gen)
         query-data (make-pull-query-accum {:depth {ent-id 0}
                                            :find #{(var-sym ent-id "id")
                                                    (var-sym ent-id "attr")
                                                    (var-sym ent-id "val")}}
                                           0
                                           ent-id
                                           (var-sym ent-id "id")
                                           pull-exp
                                           id-gen)]
     {:depth (:depth query-data)
      :query {:find (vec (:find query-data))
              :where (into [(into [:or]
                                  (map #(vector %
                                                (var-sym ent-id "attr")
                                                (var-sym ent-id "val"))
                                       ids))]
                           [(:where query-data)])}})))

(defn parse-var
  "Parses a pull variable into a name and a type, where type is one of
  id, attr, or val."
  [var]
  (let [name (str (bind/var-name var))
        last-dash-idx (str/last-index-of name "-")]
    {:var-name (subs name 0 last-dash-idx)
     :var-type (subs name (inc last-dash-idx))}))

(defn parse-raw-pull-rows
  "Given a set of raw rows from a pull query, parses the rows into a
  nested map that matches the initial pull expression. Note that
  cardinality-many attributes will be represented as maps keyed by
  entity in the resulting map and will need to be further processed
  into a list."
  [raw-rows pull-find-clause depths]
  (let [named-rows (map #(zipmap pull-find-clause %) raw-rows)
        sorted-names (sort #(compare (depths %1) (depths %2)) (keys depths))
        sorted-attrs (map #(bind/var (str % "-" "attr")) sorted-names)]
    (reduce (fn [acc named-row]
              (let [defined-attr-names (map (comp :var-name parse-var)
                                            (filter (partial get named-row)
                                                    sorted-attrs))
                    defined-vars (concat
                                  [(bind/var (str (first sorted-names)
                                                  "-"
                                                  "id"))]
                                  (mapcat #(for [type ["attr" "val"]]
                                             (bind/var (str % "-" type)))
                                          defined-attr-names))
                    vals (map (partial get named-row) defined-vars)
                    row-map (assoc-in {} (butlast vals) (last vals))]
                (deep-merge acc row-map)))
            {}
            named-rows)))

(defn fix-cardinalities
  "Given a map of parsed rows from a pull query, returns a map that is
  a proper answer to the query with all nested entities as either
  submaps or lists depending on cardinality."
  [cardinalities parsed-rows]
  (util/mapm (fn [attr val]
               ;; TODO this opens up an edge case - if someone
               ;; transacts a fact whose value is a map where all the
               ;; keys are unifydb ids, this code will consider that
               ;; map to be a sub-entity and parse it as such. Not
               ;; sure the best way to handle this, but seems like a
               ;; fairly unlikely edge case for now.
               (if (and (map? val)
                        (every? id? (keys val)))
                 (if (= (get cardinalities attr) :cardinality/many)
                   [attr (vec (map (partial fix-cardinalities cardinalities) (vals val)))]
                   ;; TODO validate the assumption that we'll only
                   ;; have 1 entity for cardinality one attributes in
                   ;; the raw rows
                   [attr (fix-cardinalities cardinalities (first (vals val)))])
                 [attr val]))
             parsed-rows))

(defn parse-pull-rows
  [raw-rows pull-find-clause depths cardinalities]
  (let [rows (map (fn [row]
                    (map (fn [exp]
                           (if (bind/var? exp) nil exp))
                         row))
                  raw-rows)
        parsed (parse-raw-pull-rows rows pull-find-clause depths)]
    (util/mapm (fn [id entity]
                 [id (fix-cardinalities cardinalities entity)])
               parsed)))

(defn pull-exp-attrs
  "Returns all attributes present in the `pull-exp`."
  [pull-exp]
  (let [accum (fn accum [exp]
                (cond
                  (vector? exp) (into [] (mapcat accum exp))
                  (map? exp) (into [(first (keys exp))]
                                   (mapcat accum (first (vals exp))))
                  (keyword? exp) [exp]))]
    (vec (set (accum (pull-query pull-exp))))))
