(ns flatland.turntable.service
  (:require [compojure.core :refer [GET POST ANY defroutes routes]]
            [compojure.route :refer [not-found]]
            [compojure.handler :refer [api]]
            [clj-time.core :refer [in-msecs now interval]]
            [clojure.java.jdbc :as sql]
            (ring.middleware [format-params :refer :all]
                             [format-response :refer :all])
            [me.raynes.fs :refer [exists?]]
            [lamina.query :as query]
            [cheshire.core :as json]
            [clojure.edn :as edn]
            [flatland.useful.utils :refer [with-adjustments]]
            [flatland.useful.seq :refer [groupings]]
            [clojure.string :as s :refer [join]]
            [flatland.chronicle :refer [times-for]]
            [ring.middleware.cors :refer [wrap-cors]]
            [flatland.turntable.timer :refer [schedule]])
  (:import (java.sql Timestamp)
           (java.util Date Calendar)
           (org.joda.time DateTime))
  (:use flatland.useful.debug))

(defonce ^{:doc "Queries that are currently running. It is a hash of the names associated
                with the queries to a map containing the query, the interval between runs,
                and the results from the last run."}
  running
  (atom {}))

(defn persist-queries
  "Persist all of the current queries so that they can be run again at startup."
  [config queries]
  (spit (:query-file config) (pr-str (for [[k v] queries]
                                       [k (dissoc v :scheduled-fn :results)]))))

(defn read-queries
  "Read all previously persisted queries."
  [config]
  (let [f (:query-file config)]
    (when (exists? f)
      (read-string (slurp f)))))

(defn get-db
  "Given a config map and a database name, extract the db from the config."
  [config db]
  (let [config (get-in config [:servers db])]
    (if (contains? config :subname)
      config
      (assoc config :subname (str "//" db)))))

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

(defn create-results-table
  "Create a results table for the query if one does not already exist.
   If it does not exist, uses CREATE TABLE AS and then adds metadata keys
   prefixed with underscores to the table for the other items."
  [{:keys [sql name]}]
  (when-not (table-exists? name)
    (let [[prepared-sql & args] (prepare (format "create table \"%s\" as %s" name sql) (Timestamp. (System/currentTimeMillis)))]
      (sql/do-prepared prepared-sql args)
      (sql/do-commands (format "truncate \"%s\"" name)
                       (format "alter table \"%s\"
                                  add column _start timestamp,
                                  add column _stop timestamp,
                                  add column _time timestamp,
                                  add column _elapsed integer"
                               name)))))

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

(defn persist-results-to-atom
  "Returns a function tresults to the @running atom."
  [config query results]
  (swap! running update-in [(:name query) :results] conj results)
  results)

(defn persist-results
  "Persist results with the functions in the config's :persist-fns."
  [config query results]
  (doseq [f (:persist-fns config)]
    (f config query results)))

(defn run-query
  "Run a query and return the results as a vector."
  [config sql time]
  (sql/with-query-results rows (prepare sql time)
    (into [] rows)))

(defn query-fn
  "Returns a function that runs a query, records start and end time,
   and updates running with the results and times when finished."
  ([config {:keys [sql name] :as query} db]
   (fn qfn
     ([] (qfn (System/currentTimeMillis)))
     ([time]
      (try
        (sql/with-connection (get-db config db)
          (let [start (now)
                time (Timestamp. time)
                results (run-query config sql time)
                stop (now)]
            (persist-results config query
                             {:results results
                              :start start
                              :stop stop
                              :time time
                              :elapsed (in-msecs (interval start stop))})))
        (catch Exception e (.printStackTrace e)))))))

(defn backfill-query [start period qfn]
  (doseq [t (times-for period (DateTime. start))
          :while (< (.getMillis t) (.getMillis (now)))]
    (qfn (.getMillis t)))
  (println "Backfill finished."))

(defn add-query
  "Add a query to run at scheduled times (via the cron-like map used by schejulure)."
  [config name db sql period backfill]
  (when-not (contains? @running name)
    (let [query {:sql sql
                 :name name
                 :db db
                 :period period}
          period (edn/read-string period)
          qfn (query-fn config query db)]
      (when backfill
        (.start (Thread. (fn [] (backfill-query (Long/parseLong backfill) period qfn)))))
      (swap! running update-in [name] assoc
             :query query
             :scheduled-fn (schedule qfn period)))))

(defn remove-query
  "Stop a scheduled query and remove its entry from @running."
  [config name]
  (.cancel (get-in @running [name :scheduled-fn]))
  (persist-queries config (swap! running dissoc name)))

(defn get-query
  "Fetch the currently running query."
  [name]
  (dissoc (@running name) :scheduled-fn))

(defn list-queries
  "List all of the queries."
  []
  (into {}
        (for [[k v] @running]
          [k (dissoc v :scheduled-fn :results)])))

(defn init-saved-queries
  "Startup persisted queries."
  [config]
  (doseq [[name {{:keys [db sql period]} :query}] (read-queries config)]
    (add-query config name db sql period nil)))

(defn absolute-time [t ref]
  (if (neg? t)
    (+ ref t)
    t))

(defn unix-time
  "Number of seconds since the unix epoch, as by Linux's time() system call."
  [^Date date]
  (-> date (.getTime) (quot 1000)))

(defn subtract-day
  "Subtract one day from the given date and return the output of getTime."
  [^Date d]
  (.getTime (doto (Calendar/getInstance)
              (.setTime d)
              (.add Calendar/DATE -1))))

(defn split-targets
  "Takes a seq of strings that look like \"query.field\" and returns a map of query
   to all the fields to be extracted from them."
  [targets]
  (groupings first second))

(defn to-ms [x]
  "Convert sections to milliseconds."
  (* x 1000))

(defn fetch-data [config query field from until]
  (let [q (get-in @running [query :query])
        key-field (keyword field)]
    (sql/with-connection (get-db config (:db q))
      (sql/with-query-results rows
        [(format "SELECT %s AS value, _time FROM \"%s\" WHERE _start >= ?::timestamp AND _start <= ?::timestamp"
                 field
                 (:name q))
         (Timestamp. (to-ms from))
         (Timestamp. (to-ms until))]
        (doall rows)))))

(defn points [config targets from until]
  (let [query->target (into {} (for [target targets]
                                 [(str "&" (s/replace target "/" ":"))
                                  target]))]
    (for [[target datapoints]
          ,,(query/query-seqs (zipmap (keys query->target)
                                      (repeat nil))
                              {:payload :value :timestamp #(.getTime ^Date (:_time %))
                               :seq-generator (fn [query]
                                                (let [segments (s/split query #":")
                                                      query (s/join ":" (butlast segments))
                                                      field (last segments)]
                                                  (fetch-data config query field from until)))})]
      {:target (query->target target)
       :datapoints (for [datapoint datapoints]
                     ((juxt :value :timestamp) datapoint))})))

(defn render-api [config]
  (GET "/render" {{:strs [target from until]} :query-params}
    (let [targets (if (coll? target) ; if there's only one target it's a string, but if multiple are
                    target           ; specified then compojure will make a list of them
                    [target])
          now-date (Date.)
          unix-now (unix-time now-date)]
      (with-adjustments #(when (seq %) (Long/parseLong %)) [from until]
        (let [until (if until
                      (absolute-time until unix-now)
                      unix-now)
              from (if from
                     (absolute-time from until)
                     (unix-time (subtract-day now-date)))]
          (or (points config targets from until)
              {:status 404}))))))

(defn turntable-routes
  "Return API routes for turntable."
  [config]
  (-> (routes
        (render-api config)
        (POST "/add" [name db sql period backfill]
              (if-let [added (add-query config name db sql period backfill)]
                (do (persist-queries config added)
                    {:status 204})
                {:status 409
                 :headers {"Content-Type" "application/json;charset=utf-8"}
                 :body (json/encode {:error "Query by this name already exists. Remove it first."})}))
        (POST "/remove" [name]
              (remove-query config name)
              {:status 204})
        (ANY "/get" [name]
             (if-let [query (get-query name)]
               {:body query}
               {:status 404}))
        (ANY "/queries" []
             {:body (list-queries)})
        (not-found nil))
      (api)
      (wrap-cors
        :access-control-allow-origin #".*"
        :access-control-allow-headers "X-Requested-With, X-File-Name, Origin, Content-Type"
        :access-control-allow-methods [:get, :post])
      (wrap-json-params)
      (wrap-json-response)))
