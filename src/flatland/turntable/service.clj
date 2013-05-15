(ns flatland.turntable.service
  (:require [cheshire.core :as json]
            [clojure.edn :as edn]
            [compojure.core :refer [ANY GET POST routes]]
            [compojure.handler :refer [api]]
            [compojure.route :refer [not-found resources]]
            [flatland.turntable.db :refer [backfill-query
                                           query-fn
                                           stage]]
            [flatland.turntable.render :refer [render-api]]
            [flatland.turntable.timer :refer [schedule]]
            [me.raynes.fs :refer [exists?]]
            [ring.middleware.cors :refer [wrap-cors]]
            [ring.middleware.format-params :refer [wrap-json-params]]
            [ring.middleware.format-response :refer [wrap-json-response]]))

(defonce ^{:doc "Queries that are currently running. It is a hash of the names associated
                with the queries to a map containing the query, the interval between runs,
                and the results from the last run."}
  running
  (atom {}))

(defn persist-queries
  "Persist all of the current queries so that they can be run again at startup."
  [config queries]
  (spit (:query-file config)
        (pr-str (for [[k v] queries]
                  [k (select-keys v [:query])]))))

(defn read-queries
  "Read all previously persisted queries."
  [config]
  (let [f (:query-file config)]
    (when (exists? f)
      (read-string (slurp f)))))

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

(defn add-query
  "Add a query to run at scheduled times (via the cron-like map used by schejulure)."
  [config name db query period-edn added-time backfill]
  (when-not (contains? @running name)
    (let [period (edn/read-string period-edn)
          query-map {:query query
                     :period period-edn
                     :added (or added-time (java.util.Date.))
                     :name name
                     :db db}
          qfn (query-fn config query-map db)]
      (when backfill
        (.start (Thread. (fn [] (backfill-query (Long/parseLong backfill) period qfn)))))
      (swap! running update-in [name] assoc
             :parsed-period period
             :query query-map
             :qfn qfn
             :scheduled-fn (schedule qfn period)))))

(defn remove-query
  "Stop a scheduled query and remove its entry from @running."
  [config name]
  (if-let [scheduled (get-in @running [name :scheduled-fn])]
    (do (.cancel scheduled)
        (persist-queries config (swap! running dissoc name))
        true)))

(defn get-query
  "Fetch the currently running query."
  [name]
  (select-keys (@running name) [:query]))

(defn list-dbs
  "List all of the configured databases."
  [config]
  (keys (:servers config)))

(defn list-queries
  "List all of the queries."
  []
  (map :query (vals @running)))

(defn init-saved-queries
  "Startup persisted queries."
  [config]
  (doseq [[name {{:keys [db query period added]} :query}] (read-queries config)]
    (add-query config name db query period added nil)))

(defn turntable-routes
  "Return API routes for turntable."
  [config]
  (-> (routes
        (render-api config running)
        (POST "/add" [name db query period backfill]
              (if-let [{{:keys [query]} name :as added} (add-query config name db query period nil backfill)]
                (do (persist-queries config added)
                    {:body query})
                {:status 409
                 :headers {"Content-Type" "application/json;charset=utf-8"}
                 :body (json/encode {:error "Query by this name already exists. Remove it first."})}))
        (POST "/remove" [name]
              (if (remove-query config name)
                {:status 204}
                {:status 404}))
        (GET "/stage" [db query]
             (stage config db query))
        (ANY "/get" [name]
             (if-let [query (get-query name)]
               {:body query}
               {:status 404}))
        (ANY "/queries" []
             {:body (list-queries)})
        (ANY "/schema" []
             {:body {:db (list-dbs config)}})
        (ANY "/backfill" [name start end]
             (if-let [query (@running name)]
               (do (.start (Thread. (fn []
                                      (let [[start end] (map #(Long/parseLong %) [start end])]
                                        (backfill-query start end (:period query) (:qfn query))))))
                   {:status 200})
               {:status 404}))
        (resources "/")
        (not-found nil))
      (api)
      (wrap-cors
        :access-control-allow-origin #".*"
        :access-control-allow-headers "X-Requested-With, X-File-Name, Origin, Content-Type"
        :access-control-allow-methods [:get, :post])
      (wrap-json-params)
      (wrap-json-response)))
