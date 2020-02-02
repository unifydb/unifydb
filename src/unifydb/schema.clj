(ns unifydb.schema
  (:require [manifold.deferred :as d]
            [unifydb.util :as util]))

(defn make-schema-query [attrs]
  "Returns the query to get the schema facts of `attrs`."
  {:find '[?schema ?attr ?val]
   :where `[[:or
             ~@(map
                (fn [attr]
                  [:and ['?e :unifydb/schema attr]
                   ['?e :unifydb/schema '?schema]
                   ['?e '?attr '?val]])
                attrs)]]})

;; TODO add caching to this

(defn get-schemas [queue-backend tx-id attrs]
  "Retrieves the schema entities, if any,
   of `attrs` as of `tx-id`. Returns a
   Manifold deferred of a seq of map,
   where each map is a single schema entity."
  (if (empty? attrs)
    (d/future [])
   (->
    (util/query queue-backend
                {:tx-id tx-id}
                (make-schema-query attrs))
    (d/chain :results #'util/join))))
