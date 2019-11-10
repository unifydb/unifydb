(ns unifydb.schema
  (:require [manifold.deferred :as d]
            [unifydb.query :as query]))

(defn join [facts]
  "Takes a list of entity-attribute-value tuples
   and returns a list of maps, where each map represents
   all the facts about a particular entity."
  (reduce
   (fn [acc v]
     (assoc-in acc [(first v) (second v)] (nth v 2)))
   {}
   facts))

;; TODO add caching to this

(defn get-schemas [queue-backend attrs tx-id]
  "Retrieves the schema entities, if any,
   of `attrs` as of `tx-id`. Returns a
   Manifold deferred of a seq of map,
   where each map is a single schema entity."
  (->
   (query/query queue-backend
                {:tx-id tx-id}
                {:find '[?schema ?attr ?val]
                 :where `[[:or
                           ~@(map
                              (fn [attr]
                                [:and ['?e :unifydb/schema attr]
                                      ['?e :unifydb/schema '?schema]
                                      ['?e '?attr '?val]])
                              attrs)]]})
   (d/chain :results #'join)))
