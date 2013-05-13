(ns flatland.turntable.db
  (:require [clojure.java.jdbc :as sql]
            [clj-time.core :refer [in-msecs interval now]]
            [clojure.pprint :refer [pprint]]
            [clojure.stacktrace :refer [print-stack-trace]]
            [flatland.chronicle :refer [times-for]]
            [flatland.turntable.jdbc-utils :refer [prepare]]
            [flatland.turntable.persist :refer [persist-results]])
  (:import (java.sql Timestamp)
           (org.joda.time DateTime)))

(defn get-db
  "Given a config map and a database name, extract the db from the config."
  [config db]
  (let [config (get-in config [:servers db])]
    (if (contains? config :subname)
      config
      (assoc config :subname (str "//" db)))))

(defn run-query
  "Run a query and return the results as a vector."
  [config sql time]
  (sql/with-query-results rows (prepare sql time)
    (into [] rows)))

(defn query-fn
  "Returns a function that runs a query, records start and end time,
   and updates running with the results and times when finished."
  ([config {:keys [query name] :as query-map} db]
   (fn qfn
     ([] (qfn (System/currentTimeMillis)))
     ([time]
      (try
        (sql/with-connection (get-db config db)
          (let [start (now)
                time (Timestamp. time)
                results (run-query config query time)
                stop (now)]
            (persist-results config query-map
                             {:results results
                              :start start
                              :stop stop
                              :time time
                              :elapsed (in-msecs (interval start stop))})))
        (catch Exception e (.printStackTrace e)))))))

(defn stage
  "Run a query the same way turntable would run it for staging purposes."
  [config db sql]
  (try
    (with-out-str (pprint (sql/with-connection (get-db config db)
                            (run-query config sql (Timestamp. (System/currentTimeMillis))))))
    (catch Exception e (with-out-str (print-stack-trace e)))))

(defn backfill-query [start period qfn]
  (doseq [t (times-for period (DateTime. start))
          :while (< (.getMillis t) (.getMillis (now)))]
    (qfn (.getMillis t))))
