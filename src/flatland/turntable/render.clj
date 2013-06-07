(ns flatland.turntable.render
  (:require [clojure.java.jdbc :as sql]
            [clojure.string :as s]
            [lamina.query :as query]
            [compojure.core :refer [GET]]
            [flatland.turntable.db :refer [get-db]]
            [flatland.useful.seq :refer [groupings]]
            [flatland.useful.utils :refer [with-adjustments]]
            [flatland.useful.map :refer [keyed]]
            [flatland.laminate.render :as laminate]
            [flatland.laminate.time :as time])
  (:import (java.sql Timestamp)
           (java.util Date Calendar)))

(defn split-targets
  "Takes a seq of strings that look like \"query.field\" and returns a map of query
   to all the fields to be extracted from them."
  [targets]
  (groupings first second))

(defn fetch-data [config query from until limit]
  (let [running @(:running config)
        q (get-in running [query :query])]
    (sql/with-connection (get-db config (:db q))
      (try
        (sql/with-query-results rows
          (if limit
            [(format "SELECT * FROM \"%s\" LIMIT ?::int" (:name q))
             limit]
            [(format "SELECT * FROM \"%s\" WHERE _start >= ?::timestamp AND _start <= ?::timestamp"
                     (:name q))
             (Timestamp. (time/s->ms from))
             (Timestamp. (time/s->ms until))])
          (doall (for [row rows]
                   {:time (get row :_time)
                    :value (dissoc row :_time :_start :_end)})))
        (catch org.postgresql.util.PSQLException e
          (when-not (re-find #"does not exist" (.getMessage e))
            (throw e)))))))

(defn render-points [config targets {:keys [from until limit period offset]}]
  (let [query-opts (merge {:payload :value :timestamp #(.getTime ^Date (:time %))
                           :seq-generator #(fetch-data config % from until limit)}
                          (when period {:period period}))]
    (laminate/points targets offset query-opts)))

(defn add-error [points error target]
  (conj points {:target target
                :error error}))

(defn render-api [config]
  (GET "/render" {{:strs [target limit from until shift period align timezone]} :query-params}
    (let [targets (if (coll? target) ; if there's only one target it's a string, but if multiple are
                    target           ; specified then compojure will make a list of them
                    [target])
          now (System/currentTimeMillis)
          render-opts (laminate/parse-render-opts (keyed [now from until shift period align timezone]))]
      (render-points config targets (assoc render-opts :limit limit)))))
