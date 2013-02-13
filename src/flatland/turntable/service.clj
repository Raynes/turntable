(ns flatland.turntable.service
  (:require [compojure.core :refer [GET POST ANY defroutes routes]]
            [clj-time.core :refer [in-minutes now interval]]
            [overtone.at-at :refer [mk-pool every stop]]))

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

(defn query-fn
  "Returns a function that runs a query, records start and end time,
   and updates running with the results and times when finished."
  [name query minutes]
  (fn []
    (let [start (now)
          results (println query)
          stop (now)]
      (swap! running update-in [name] assoc
             :results results
             :start (str start)
             :stop (str stop)
             :elapsed (in-minutes (interval start stop))))))

(defn add-query
  "Schedule a query to run at most once at minutes intervals. Adheres to a
   one minute cooldown between scheduling queries to help prevent too much
   database load."
  [name server db query minutes]
  (swap! running
         assoc name {:query query
                     :server server
                     :db db
                     :minutes minutes
                     :scheduled-fn (future
                                     (every (mins minutes)
                                            (query-fn name query minutes)
                                            pool))}))

(defn remove-query
  "Stop a scheduled query and remove its entry from @running."
  [name]
  (stop @(get-in @running [name :scheduled-fn]))
  (swap! running dissoc name))

(defn get-query [name]
  (dissoc (@running name) :scheduled-fn))

(defn list-queries []
  (keys @running))

(defroutes writers
  (POST "/add" [name server db query minutes]
    (add-query name server db query (Long. minutes)))
  (POST "/remove" [name]
    (remove-query name)))

(defroutes readers
  (ANY "/get" [name]
       {:body (get-query name)})
  (ANY "/list" []
    (list-queries)))