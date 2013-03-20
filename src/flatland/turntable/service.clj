(ns flatland.turntable.service
  (:refer-clojure :exclude [second])
  (:require [compojure.core :refer [GET POST ANY defroutes routes]]
            [compojure.route :refer [not-found]]
            [compojure.handler :refer [api]]
            [clj-time.core :refer [in-msecs now interval]]
            [overtone.at-at :refer [mk-pool every stop]]
            [clojure.java.jdbc :as sql]
            (ring.middleware [format-params :refer :all]
                             [format-response :refer :all])
            [me.raynes.fs :refer [exists?]]
            [cheshire.core :as json]))

(def ^:const second
  "One second in millseconds."
  1000)

(def pool
  "A thread pool for usage with at-at."
  (mk-pool))

(defn secs
  "Seconds to milliseconds."
  [s]
  (* s second))

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

(defn persist-results-to-atom
  "Returns a function tresults to the @running atom."
  [config name results]
  (swap! running update-in [name :results] conj results)
  results)

(defn persist-results [config name results]
  (doseq [f (:persist-fns config)]
    (f config name results)))

(defn run-query [config sql db]
  (sql/with-connection (get-db config db)
    (sql/with-query-results rows (prepare sql (sql-date))
      (into [] rows))))

(defn query-fn
  "Returns a function that runs a query, records start and end time,
   and updates running with the results and times when finished."
  [config {:keys [sql name]} db]
  (fn []
    (let [start (now)
          results (run-query config sql db)
          stop (now)]
      (persist-results config name
                       {:results results
                        :start (str start)
                        :stop (str stop)
                        :elapsed (in-msecs (interval start stop))}))))

(defn add-query
  "Add a query name to run at period intervals."
  [config name db sql period]
  (when-not (contains? @running name)
    (let [query {:sql sql
                 :name name
                 :db db
                 :period period}
          scheduled (every (secs period)
                           (query-fn config query db)
                           pool)]
      (swap! running update-in [name] assoc
             :query query
             :scheduled-fn scheduled))))

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
          [k (dissoc v :scheduled-fn :results)])))

(defn init-saved-queries [config]
  (doseq [[name {{:keys [db sql period]} :query}] (read-queries config)]
    (add-query config name db sql period)))

(defn turntable-routes [config]
  (-> (routes
       (POST "/add" [name db sql period]
         (if-let [added (add-query config name db sql (Long. period))]
           (do (persist-queries config added)
               {:status 204})
           {:status 409
            :headers {"Content-Type" "application/json;charset=utf-8"}
            :body (json/encode {:error "Query by this name already exists. Remove it first."})}))
       (POST "/remove" [name]
         (remove-query config name)
         {:status 204})
       (ANY "/get" [name]
            {:body (get-query name)})
       (ANY "/list" []
            {:body (list-queries)})
       (not-found nil))
      (api)
      (wrap-json-params)
      (wrap-json-response)))
