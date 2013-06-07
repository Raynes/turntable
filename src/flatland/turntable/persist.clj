(ns flatland.turntable.persist
  (:require [clojure.java.jdbc :as sql]
            [flatland.turntable.jdbc-utils :refer [prepare table-exists?]])
  (:import (java.sql Timestamp)))

(defn persist-results-to-atom
  "Returns a function tresults to the @running atom."
  [config running query results]
  (swap! running update-in [(:name query) :results] conj results)
  results)

(defn create-results-table
  "Create a results table for the query if one does not already exist.
   If it does not exist, uses CREATE TABLE AS and then adds metadata keys
   prefixed with underscores to the table for the other items."
  [{:keys [query name]}]
  (when-not (table-exists? name)
    (let [[prepared-sql & args] (prepare (format "create table \"%s\" as %s" name query) (Timestamp. (System/currentTimeMillis)))]
      (sql/do-prepared prepared-sql args)
      (sql/do-commands (format "truncate \"%s\"" name)
                       (format "alter table \"%s\"
                                  add column _start timestamp,
                                  add column _stop timestamp,
                                  add column _time timestamp,
                                  add column _elapsed integer"
                               name)
                       (format "create index on \"%s\" (_time)" name)))))

(defn persist-results-to-db
  "Assumes a sql connection is already in place. Persists the results of the query
   to a table named after the query. If the table doesn't already exist it creates it."
  [config query results]
  (create-results-table query)
  (let [{:keys [results start stop time elapsed]} results]
    (apply sql/insert-records (str "\"" (:name query) "\"")
           (for [result results]
             (merge result {"_start" (Timestamp. (.getMillis start))
                            "_stop" (Timestamp. (.getMillis stop))
                            "_time" time
                            "_elapsed" elapsed})))))

(defn persist-results
  "Persist results with the functions in the config's :persist-fns."
  [config query results]
  (doseq [f (:persist-fns config)]
    (f config query results)))
