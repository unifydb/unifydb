(ns unifydb.pull-test
  (:require [clojure.test :refer [is deftest testing]]
            [unifydb.query.pull :as pull]))

(defn next-from [l]
  (let [w (atom l)]
    (fn []
      (let [f (first @w)]
        (when f (swap! w rest))
        f))))

(deftest test-make-pull-query
  (doseq [{:keys [case pull-exp ids expected]}
          [{:case "Nested pull expression"
            :ids [#unifydb/id 1 #unifydb/id 2]
            :pull-exp [:name
                       :favorite-color
                       {:status [:text]}
                       {:friends [:name
                                  :favorite-color
                                  {:status [:text]}]}]
            :expected '{:query
                        {:find [?c-attr
                                ?a-attr
                                ?a-val
                                ?c-val
                                ?b-val
                                ?d-val
                                ?d-attr
                                ?a-id
                                ?b-attr]
                         :where [[:or
                                  [#unifydb/id 1 ?a-attr ?a-val]
                                  [#unifydb/id 2 ?a-attr ?a-val]]
                                 [:and
                                  [:or
                                   [?a-id :name ?a-val]
                                   [?a-id :favorite-color ?a-val]
                                   [:and
                                    [?a-id :status ?a-val]
                                    [:and
                                     [:or
                                      [?a-val :text ?b-val]]
                                     [?a-val ?b-attr ?b-val]]]
                                   [:and
                                    [?a-id :friends ?a-val]
                                    [:and
                                     [:or
                                      [?a-val :name ?c-val]
                                      [?a-val :favorite-color ?c-val]
                                      [:and
                                       [?a-val :status ?c-val]
                                       [:and
                                        [:or
                                         [?c-val :text ?d-val]]
                                        [?c-val ?d-attr ?d-val]]]]
                                     [?a-val ?c-attr ?c-val]]]]
                                  [?a-id ?a-attr ?a-val]]]}
                        :depth {"a" 0
                                "b" 1
                                "c" 1
                                "d" 2}}}]]
    (testing case
      (is (= expected
             (pull/make-pull-query pull-exp
                                   ids
                                   (next-from
                                    ["a" "b" "c" "d" "e" "f" "g" "h" "i" "j"])))))))

(deftest test-row-parsing
  (let [raw-rows '[[#unifydb/id 2
                    :name "Alice"
                    nil nil
                    nil nil
                    nil nil]
                   [#unifydb/id 2
                    :favorite-color "red"
                    nil nil
                    nil nil
                    nil nil]
                   [#unifydb/id 2
                    :status #unifydb/id 6
                    :text "Feeling good"
                    nil nil
                    nil nil]
                   [#unifydb/id 2
                    :friends #unifydb/id 4
                    nil nil
                    :name "Carl"
                    nil nil]
                   [#unifydb/id 2
                    :friends #unifydb/id 3
                    nil nil
                    :name "Bob"
                    nil nil]
                   [#unifydb/id 2
                    :friends #unifydb/id 4
                    nil nil
                    :favorite-color "yellow"
                    nil nil]
                   [#unifydb/id 2
                    :friends #unifydb/id 3
                    nil nil
                    :favorite-color "green"
                    nil nil]
                   [#unifydb/id 2
                    :friends #unifydb/id 4
                    nil nil
                    :status #unifydb/id 7
                    :text "Feeling bad"]]
        find-clause '[[? a-id]
                      [? a-attr]
                      [? a-val]
                      [? b-attr]
                      [? b-val]
                      [? c-attr]
                      [? c-val]
                      [? d-attr]
                      [? d-val]]
        depths '{"a" 0
                 "b" 1
                 "c" 1
                 "d" 2}
        cardinalities {:friends :cardinality/many}]
    (is (= {#unifydb/id 2
            {:name "Alice",
             :favorite-color "red",
             :status {:text "Feeling good"},
             :friends
             [{:name "Carl",
               :favorite-color "yellow",
               :status {:text "Feeling bad"}}
              {:name "Bob", :favorite-color "green"}]}}
           (pull/parse-pull-rows raw-rows find-clause depths cardinalities)))))
