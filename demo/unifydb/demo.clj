(ns unifydb.demo
  (:require [unifydb.repl :refer [start-system! state] :rename {state db-state}]
            [unifydb.util :refer [query]]
            [unifydb.transact :refer [transact]]))

;; Start the database services
(start-system!)

;; Transact some facts
(def tx-data [[:unifydb/add "ben" :name "Ben Bitdiddle"]
              [:unifydb/add "ben" :job [:computer :wizard]]
              [:unifydb/add "ben" :salary 60000]
              [:unifydb/add "alyssa" :name "Alyssa P. Hacker"]
              [:unifydb/add "alyssa" :job [:computer :programmer]]
              [:unifydb/add "alyssa" :salary 45000]
              [:unifydb/add "alyssa" :supervisor "ben"]
              [:unifydb/add "ben" :address [:slumerville [:ridge :road] 10]]
              [:unifydb/add "alyssa" :address [:cambridge [:mass :ave] 78]]
              [:unifydb/add "lem" :name "Lem E. Tweakit"]
              [:unifydb/add "lem" :job [:computer :programmer]]
              [:unifydb/add "lem" :salary 40000]
              [:unifydb/add "lem" :supervisor "ben"]
              [:unifydb/add "lem" :address [:cambridge [:canal :street] 12]]
              [:unifydb/add "louis" :name "Louis Reasoner"]
              [:unifydb/add "louis" :salary 20000]
              [:unifydb/add "louis" :job [:chief :intern]]
              [:unifydb/add "louis" :supervisor "alyssa"]
              [:unifydb/add "louis" :address [:slumerville [:davis :square] 42]]
              [:unifydb/add "oliver" :name "Oliver Warbucks"]
              [:unifydb/add "oliver" :job [:chief :executive]]
              [:unifydb/add "oliver" :salary 150000]
              [:unifydb/add "oliver" :address [:swellesley [:top :heap :road] 1]]
              [:unifydb/add "ben" :supervisor "oliver"]
              [:unifydb/add "unifydb.tx" :doc "Initial data load"]])

@(transact (:queue @db-state) tx-data)
;; => {:tx-report
;;     {:db-after {:tx-id #unifydb/id 6},
;;      :tx-data
;;      [[#unifydb/id 1 :name "Ben Bitdiddle" #unifydb/id 6 true]
;;       [#unifydb/id 1 :job [:computer :wizard] #unifydb/id 6 true]
;;       [#unifydb/id 1 :salary 60000 #unifydb/id 6 true]
;;       [#unifydb/id 2 :name "Alyssa P. Hacker" #unifydb/id 6 true]
;;       [#unifydb/id 2 :job [:computer :programmer] #unifydb/id 6 true]
;;       [#unifydb/id 2 :salary 45000 #unifydb/id 6 true]
;;       [#unifydb/id 2 :supervisor #unifydb/id 1 #unifydb/id 6 true]
;;       [#unifydb/id 1
;;        :address
;;        [:slumerville [:ridge :road] 10]
;;        #unifydb/id 6
;;        true]
;;       [#unifydb/id 2
;;        :address
;;        [:cambridge [:mass :ave] 78]
;;        #unifydb/id 6
;;        true]
;;       [#unifydb/id 3 :name "Lem E. Tweakit" #unifydb/id 6 true]
;;       [#unifydb/id 3 :job [:computer :programmer] #unifydb/id 6 true]
;;       [#unifydb/id 3 :salary 40000 #unifydb/id 6 true]
;;       [#unifydb/id 3 :supervisor #unifydb/id 1 #unifydb/id 6 true]
;;       [#unifydb/id 3
;;        :address
;;        [:cambridge [:canal :street] 12]
;;        #unifydb/id 6
;;        true]
;;       [#unifydb/id 4 :name "Louis Reasoner" #unifydb/id 6 true]
;;       [#unifydb/id 4 :salary 20000 #unifydb/id 6 true]
;;       [#unifydb/id 4 :job [:chief :intern] #unifydb/id 6 true]
;;       [#unifydb/id 4 :supervisor #unifydb/id 2 #unifydb/id 6 true]
;;       [#unifydb/id 4
;;        :address
;;        [:slumerville [:davis :square] 42]
;;        #unifydb/id 6
;;        true]
;;       [#unifydb/id 5 :name "Oliver Warbucks" #unifydb/id 6 true]
;;       [#unifydb/id 5 :job [:chief :executive] #unifydb/id 6 true]
;;       [#unifydb/id 5 :salary 150000 #unifydb/id 6 true]
;;       [#unifydb/id 5
;;        :address
;;        [:swellesley [:top :heap :road] 1]
;;        #unifydb/id 6
;;        true]
;;       [#unifydb/id 1 :supervisor #unifydb/id 5 #unifydb/id 6 true]
;;       [#unifydb/id 6 :doc "Initial data load" #unifydb/id 6 true]
;;       [#unifydb/id 6
;;        :unifydb/txInstant
;;        1617925287750
;;        #unifydb/id 6
;;        true]],
;;      :tempids
;;      {"ben" #unifydb/id 1,
;;       "alyssa" #unifydb/id 2,
;;       "lem" #unifydb/id 3,
;;       "louis" #unifydb/id 4,
;;       "oliver" #unifydb/id 5,
;;       "unifydb.tx" #unifydb/id 6}}}

;; A helper function to query the database
(defn do-query
  ([query-data] (do-query query-data {:tx-id :latest}))
  ([query-data db]
   (:results @(query (:queue @db-state) db query-data))))

;; Basic pattern matching - what is Ben's salary?
(do-query '{:find [?e ?salary]
            :where [[?e :name "Ben Bitdiddle"]
                    [?e :salary ?salary]]})
;; => ([#unifydb/id 1 60000])

;; Graph query - find backreferences from Ben to those he supervises up to two levels deep
(do-query '{:find [?employee ?supervisor]
            :where [[?e :name ?employee]
                    [?s :name ?supervisor]
                    [:or
                     [:and
                      [?e :supervisor ?s]
                      [?s :name "Ben Bitdiddle"]]
                     [:and
                      [?e :supervisor ?s]
                      [?s :supervisor ?ben]
                      [?ben :name "Ben Bitdiddle"]]]]})
;; => (["Alyssa P. Hacker" "Ben Bitdiddle"]
;;     ["Lem E. Tweakit" "Ben Bitdiddle"]
;;     ["Louis Reasoner" "Alyssa P. Hacker"])

;; Unification recurses into collection-type values
(do-query '{:find [?name ?job-subtype]
            :where [[?e :job [:computer ?job-subtype]]
                    [?e :name ?name]]})
;; => (["Alyssa P. Hacker" :programmer]
;;     ["Lem E. Tweakit" :programmer]
;;     ["Ben Bitdiddle" :wizard])

;; Destructure lists into first & rest
(do-query '{:find [?address]
            :where [[_ :address [:slumerville & ?address]]]})
;; => ([((:davis :square) 42)] [((:ridge :road) 10)])

;; Query using predicate functions
(do-query '{:find [?name ?salary]
            :where [[?e :salary ?salary]
                    [(> ?salary 40000)]
                    [?e :name ?name]]})
;; => (["Alyssa P. Hacker" 45000]
;;     ["Ben Bitdiddle" 60000]
;;     ["Oliver Warbucks" 150000])

;; Predicate functions can be any predicate in clojure.core
(do-query '{:find [?name]
            :where [[?e :name ?name]
                    [(re-matches #"^L.*" ?name)]]})
;; => (["Lem E. Tweakit"] ["Louis Reasoner"])

;; What if Alyssa moves?
(def move-facts [[:unifydb/retract #unifydb/id 2 :address [:cambridge [:mass :ave] 78]]
                 [:unifydb/add #unifydb/id 2 :address [:boston [:boylston :street] 112]]
                 [:unifydb/add "unifydb.tx" :doc "Alyssa moved!"]])

@(transact (:queue @db-state) move-facts)
;; => {:tx-report
;;     {:db-after {:tx-id #unifydb/id 7},
;;      :tx-data
;;      [[#unifydb/id 2
;;        :address
;;        [:cambridge [:mass :ave] 78]
;;        #unifydb/id 7
;;        false]
;;       [#unifydb/id 2
;;        :address
;;        [:boston [:boylston :street] 112]
;;        #unifydb/id 7
;;        true]
;;       [#unifydb/id 7 :doc "Alyssa moved!" #unifydb/id 7 true]
;;       [#unifydb/id 7
;;        :unifydb/txInstant
;;        1617925849008
;;        #unifydb/id 7
;;        true]],
;;      :tempids {"unifydb.tx" #unifydb/id 7}}}

;; Which transactions changed Alyssa's address?
;; This will be nicer once aggregation operations are added (group by tx-id)
(do-query '{:find [?tx-id ?address ?added ?doc]
            :where [[?e :name "Alyssa P. Hacker"]
                    [?e :address ?address ?tx-id ?added]
                    [?tx-id :doc ?doc]]}
          {:tx-id :latest
           :historical true})
;; => ([#unifydb/id 7 (:cambridge (:mass :ave) 78) false "Alyssa moved!"]
;;     [#unifydb/id 6
;;      (:cambridge (:mass :ave) 78)
;;      true
;;      "Initial data load"]
;;     [#unifydb/id 7
;;      (:boston (:boylston :street) 112)
;;      true
;;      "Alyssa moved!"])

;; We can query using the latest version of the database
(do-query '{:find [?address]
            :where [[#unifydb/id 2 :address ?address]]}
          ;; equivalent to {:tx-id #unifydb/id 7}
          {:tx-id :latest})
;; => ([(:boston (:boylston :street) 112)])

;; Or using a previous transaction as a historical anchor
(do-query '{:find [?address]
            :where [[#unifydb/id 2 :address ?address]]}
          {:tx-id #unifydb/id 6})
;; => ([(:cambridge (:mass :ave) 78)])

;; Rules let us derive new facts from existing ones
(do-query '{:find [?name]
            :where [[?ben :name "Ben Bitdiddle"]
                    (:lives-near ?who ?ben)
                    [?who :name ?name]]
            :rules [[(:lives-near ?person1 ?person2)
                     [?person1 :address [?town & _]]
                     [?person2 :address [?town & _]]
                     [:not (:same ?person1 ?person2)]]
                    [(:same ?x ?x)]]})
;; => (["Louis Reasoner"])
