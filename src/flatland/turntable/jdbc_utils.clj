(ns flatland.turntable.jdbc-utils
  (:require [clojure.java.jdbc :as sql]))

(defn prepare
  "Prepare a query, count its args and then duplicate arg that many times for
  passing to the query. Returns a vector suitable for passing to with-query-results."
  [query arg]
  (into [query] (repeat (-> (sql/prepare-statement (sql/connection) query)
                            (.getParameterMetaData)
                            (.getParameterCount))
                        arg)))

(defn table-exists?
  "Check if a table exists."
  [table]
  (sql/with-query-results rows
    ["select count(*) from information_schema.tables where table_name = ?" table]
    (-> rows first :count pos?)))
