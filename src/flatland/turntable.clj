(ns flatland.turntable
  (:require [compojure.core :refer [GET POST ANY defroutes routes]]
            (ring.middleware [format-params :refer :all]
                             [params :refer :all]
                             [keyword-params :refer :all])
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

(def cooldowns
  "A map of intervals to cooldowns."
  (atom {}))

(defn cooldown
  "Start a cooldown of 1 minute on this interval."
  [minutes]
  (swap! cooldowns assoc minutes (promise))
  (future
    (Thread/sleep (mins 1))
    (swap! cooldowns update-in [minutes] deliver true)))

(defn await-cooldown
  "Check for a cooldown in place on this interval and if present, wait for
   the promise to be delivered (the cooldown ending)."
  [minutes]
  (if-let [cd (@cooldowns (min minutes))]
    @cd
    true))

(defn query-fn
  "Returns a function that runs a query, records start and end time,
   and updates running with the results and times when finished."
  [name query]
  (fn []
    (let [start (now)
          results (println "This will eventually do stuff.")
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
  [name query minutes]
  (let [minutes (mins minutes)]
    (when (await-cooldown minutes)
      (swap! running
             assoc name {:query query
                         :minutes minutes
                         :scheduled-fn (every minutes
                                              (query-fn name query)
                                              pool)}))))

(defn remove-query
  "Stop a scheduled query and remove its entry from @running."
  [name]
  (stop (get-in @running [name :scheduled-fn]))
  (swap! running dissoc name))

(defn get-query [name])

(defn list-queries [])

(defroutes writers
  (POST "/add" [name query minutes]
    (add-query name query minutes))
  (POST "/remove" [name]
    (remove-query name)))

(defroutes readers
  (ANY "/get" [name]
    (get-query name))
  (GET "/list" []
    (list-queries)))

(def handler
  (-> (routes readers writers)
      (wrap-keyword-params)
      (wrap-params)
      (wrap-json-params)))