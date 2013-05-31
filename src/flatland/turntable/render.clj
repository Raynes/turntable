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

(defn fetch-data [config running query field from until limit]
  (let [q (get-in running [query :query])
        key-field (keyword field)]
    (sql/with-connection (get-db config (:db q))
      (try
        (sql/with-query-results rows
          (if limit
            [(format "SELECT %s AS value, _time FROM \"%s\" LIMIT ?::int" field (:name q))
             limit]
            [(format "SELECT %s AS value, _time FROM \"%s\" WHERE _start >= ?::timestamp AND _start <= ?::timestamp"
                     field
                     (:name q))
             (Timestamp. (time/s->ms from))
             (Timestamp. (time/s->ms until))])
          (doall rows))
        (catch org.postgresql.util.PSQLException e
          (when-not (re-find #"does not exist" (.getMessage e))
            (throw e)))))))

(defn split-target [target]
  (when target
    (let [segments (s/split target #":")]
      [(s/join ":" (butlast segments)) (last segments)])))

(defn fetcher [config running from until limit]
  (fn [query]
    (let [[query field] (split-target query)]
      (fetch-data config running query field from until limit))))

(defn render-points [config running targets {:keys [from until limit period offset]}]
  (let [query-opts (merge {:payload :value :timestamp #(.getTime ^Date (:_time %))
                           :seq-generator (fetcher config running from until limit)}
                          (when period {:period period}))]
    (laminate/points targets offset query-opts)))

(defn add-error [points error target]
  (conj points {:target target
                :error error}))

(defn render-api [config running]
  (GET "/render" {{:strs [target limit from until shift period align timezone]} :query-params}
    (let [targets (if (coll? target) ; if there's only one target it's a string, but if multiple are
                    target           ; specified then compojure will make a list of them
                    [target])
          [existing not-existing] (when (seq target)
                                    ((juxt filter remove)
                                     (comp (partial contains? @running)
                                           first
                                           split-target)
                                     targets))
          now (System/currentTimeMillis)
          render-opts (laminate/parse-render-opts (keyed [now from until shift period align timezone]))]
      (reduce #(add-error % "Target does not exist." %2)
              (render-points config @running existing (assoc render-opts :limit limit))
              not-existing))))
