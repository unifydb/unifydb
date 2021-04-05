(ns unifydb.demo
  (:require [unifydb.repl :refer [start-system! state] :rename {state db-state}]
            [unifydb.util :refer [query]]
            [unifydb.transact :refer [transact]]))

(start-system!)

(def tx-data [[:unifydb/add "ben" :name "Ben Bitdiddle"]
              [:unifydb/add "ben" :job [:computer :wizard]]
              [:unifydb/add "ben" :salary 60000]
              [:unifydb/add "alyssa" :name "Alyssa P. Hacker"]
              [:unifydb/add "alyssa" :job [:computer :programmer]]
              [:unifydb/add "alyssa" :salary 40000]
              [:unifydb/add "alyssa" :supervisor "ben"]
              [:unifydb/add "ben" :address [:slumerville [:ridge :road] 10]]
              [:unifydb/add "alyssa" :address [:cambridge [:mass :ave] 78]]
              [:unifydb/retract "alyssa" :address [:cambridge [:mass :ave] 78]]
              [:unifydb/add "louis" :name "Louis Reasoner"]
              [:unifydb/add "louis" :salary 20000]
              [:unifydb/add "louis" :job [:chief :intern]]
              [:unifydb/add "louis" :supervisor "ben"]
              [:unifydb/add "louis" :address [:slumerville [:davis :square] 42]]])

@(transact (:queue @db-state) tx-data)

@(query (:queue @db-state)
        {:tx-id :latest}
        '{:find [?e ?salary]
          :where [[?e :name "Ben Bitdiddle"]
                  [?e :salary ?salary]]})
