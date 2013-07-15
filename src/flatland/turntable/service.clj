(ns flatland.turntable.service
  (:require [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.string :as s]
            [compojure.core :refer [ANY GET POST routes]]
            [compojure.handler :refer [api]]
            [compojure.route :refer [not-found resources]]
            [flatland.turntable.db :refer [backfill-query
                                           query-fn
                                           stage]]
            [flatland.turntable.timer :refer [schedule]]
            [me.raynes.fs :refer [exists?]]
            [ring.middleware.cors :refer [wrap-cors]]
            [ring.middleware.format-params :refer [wrap-json-params]]
            [ring.middleware.format-response :refer [wrap-json-response]]
            [noir.util.middleware :refer [wrap-rewrites]])
  (:import java.util.concurrent.Executors))

(defonce ^{:doc "Serialize running queries."}
  serializing-executor
  (Executors/newSingleThreadExecutor))

(defn persist-queries
  "Persist all of the current queries so that they can be run again at startup."
  [queries config]
  (.submit serializing-executor
           (fn []
             (spit (:query-file config)
                   (pr-str (for [[k v] queries]
                             [k (select-keys v [:query])]))))))

(defn read-queries
  "Read all previously persisted queries."
  [config]
  (let [f (:query-file config)]
    (when (exists? f)
      (read-string (slurp f)))))

(defn persist-results-to-atom
  "Returns a function tresults to the @running atom."
  [config query results]
  (swap! (:running config) update-in [(:name query) :results] conj results)
  results)

(defn persist-results
  "Persist results with the functions in the config's :persist-fns."
  [config query results]
  (doseq [f (:persist-fns config)]
    (f config query results)))

(defn add-query
  "Add a query to run at scheduled times (via the cron-like map used by schejulure)."
  [config name db query period-edn added-time]
  (let [running (:running config)]
    (when-not (contains? @running name)
      (let [period (edn/read-string period-edn)
            query-map {:query query
                       :period period-edn
                       :added (or added-time (java.util.Date.))
                       :name name
                       :db db}
            qfn (query-fn config query-map db)]
        (doto (swap! running update-in [name] assoc
                     :parsed-period period
                     :query query-map
                     :qfn qfn
                     :scheduled-fn (schedule qfn period))
          (persist-queries config))))))

(defn remove-query
  "Stop a scheduled query and remove its entry from @running."
  [config name]
  (let [running (:running config)]
    (if-let [scheduled (get-in @running [name :scheduled-fn])]
      (do (.cancel scheduled)
          (persist-queries (swap! running dissoc name) config)
          true))))

(defn get-query
  "Fetch the currently running query."
  [config name]
  (select-keys (@(:running config) name) [:query]))

(defn list-dbs
  "List all of the configured databases."
  [config]
  (keys (:servers config)))

(defn list-queries
  "List all of the queries."
  [config]
  (map :query (vals @(:running config))))

(defn init-saved-queries
  "Startup persisted queries."
  [config]
  (doseq [[name {{:keys [db query period added]} :query}] (read-queries config)]
    (add-query config name db query period added)))

(defn init-config [config]
  (assoc config :running (atom {})))

(defn turntable-routes
  "Return API routes for turntable."
  [config]
  (-> (routes
        (POST "/add" [name db query period]
          (if-let [{{:keys [query]} name :as added} (add-query config name db query period nil)]
            (do
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
             (if-let [query (get-query config name)]
               {:body query}
               {:status 404}))
        (ANY "/queries" []
             {:body (list-queries config)})
        (ANY "/schema" []
             {:body {:db (list-dbs config)}})
        (ANY "/backfill" [name start end]
             (if-let [query (@(:running config) name)]
               (do (.start (Thread. (fn []
                                      (let [[start end] (map #(Long/parseLong %) [start end])]
                                        (backfill-query start end (:parsed-period query) (:qfn query))))))
                   {:status 200})
               {:status 404}))
        (-> (resources "/")
            (wrap-rewrites #"^/turntable/?$" "/turntable/index.html"))
        (not-found nil))
      (api)
      (wrap-cors
        :access-control-allow-origin #".*"
        :access-control-allow-headers "X-Requested-With, X-File-Name, Origin, Content-Type"
        :access-control-allow-methods [:get, :post])
      (wrap-json-params)
      (wrap-json-response)))
