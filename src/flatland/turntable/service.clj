(ns flatland.turntable.service
  (:require [compojure.core :refer [GET POST ANY defroutes routes]]
            [compojure.route :refer [not-found]]
            [compojure.handler :refer [api]]
            [clj-time.core :refer [in-minutes now interval]]
            [overtone.at-at :refer [mk-pool every stop]]
            [clojure.java.jdbc :as sql]
            (ring.middleware [format-params :refer :all]
                             [format-response :refer :all])))

(def ^:const minute
  "One minute in millseconds."
  60000)

(def pool
  "A thread pool for usage with at-at."
  (mk-pool))

(defn mins
  "Minutes to milliseconds."
  [m]
  (* m minute))

(def running
  "Queries that are currently running. It is a hash of the names associated
   with the queries to a map containing the query, the interval between runs,
   and the results from the last run."
  (atom {}))

(defn persist-queries
  "Persist all of the current queries so that they can be run again at startup."
  [config queries]
  (spit (:query-file config) (pr-str (for [[k v] queries]
                                       [k (dissoc v :scheduled-fn :results)]))))

(defn read-queries
  "Read all previously persisted queries."
  [config]
  (-> (:query-file config)
      (slurp)
      (read-string)))

(defn sql-date
  "Get an SQL date for the current time."
  []
  (java.sql.Date. (System/currentTimeMillis)))

(defn prepare
  "Prepare a query, count its args and then duplicate arg that many times for
   passing to the query. Returns a vector suitable for passing to with-query-results."
  [query arg]
  (let [statement (sql/prepare-statement (sql/connection) query)]
    (into [statement] (repeat (-> statement
                                  (.getParameterMetaData)
                                  (.getParameterCount))
                              arg))))

(defn run-query [config query server db]
  (sql/with-connection (get-in config [:servers server db])
    (sql/with-query-results rows (prepare query (sql-date))
      (into [] rows))))

(defn query-fn
  "Returns a function that runs a query, records start and end time,
   and updates running with the results and times when finished."
  [config {:keys [query name]} server db]
  (fn []
    (let [start (now)
          results (run-query config query server db)
          stop (now)]
      (swap! running assoc-in [name :results]
             {:results results
              :start (str start)
              :stop (str stop)
              :elapsed (in-minutes (interval start stop))}))))

(defn add-query
  "Add a query name to run at minutes intervals."
  [config name server db query minutes]
  (let [query {:query query
               :name name
               :server server
               :db db
               :minutes minutes}
        scheduled (every (mins minutes)
                         (query-fn config query server db)
                         pool)]
    (swap! running update-in [name] assoc
           :query query
           :scheduled-fn scheduled)))

(defn remove-query
  "Stop a scheduled query and remove its entry from @running."
  [config name]
  (stop (get-in @running [name :scheduled-fn])) 
  (persist-queries config (swap! running dissoc name)))

(defn get-query [name]
  (dissoc (@running name) :scheduled-fn))

(defn list-queries []
  (into {}
        (for [[k v] @running]
          [k (dissoc v :scheduled-fn)])))

(defn init-saved-queries [config]
  (doseq [[name {{:keys [server db query minutes]} :query}] (read-queries config)]
    (add-query config name server db query minutes)))

(defn turntable-routes [config]
  (-> (routes
       (POST "/add" [name server db query minutes] 
         (persist-queries config (add-query config name server db query (Long. minutes))))
       (POST "/remove" [name]
         (remove-query config name))
       (ANY "/get" [name]
            {:body (get-query name)})
       (ANY "/list" []
            {:body (list-queries)})
       (not-found nil))
      (api)
      (wrap-json-params)
      (wrap-json-response)))
