(ns unifydb.id)

(deftype ID [value]
  :load-ns true
  Object
  (equals [this o] (and
                    (instance? ID o)
                    (= (.value this) (.value o))))
  (hashCode [this] (hash (format "#unifydb/id %s" (.value this))))
  (toString [this] (str (.value this)))
  Comparable
  (compareTo [this o] (if (instance? ID o)
                        (compare (.value this) (.value o))
                        (compare (.value this) o))))

(defmethod print-method ID
  [v w]
  (.write w (format "#unifydb/id %s" (.value v))))

(defn id? [val]
  (instance? ID val))

(defn id [val]
  (if (id? val)
    val
    (->ID val)))
