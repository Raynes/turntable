(ns flatland.turntable.render
  (:require [clojure.java.jdbc :as sql]
            [clojure.string :as s]
            [lamina.query :as query]
            [compojure.core :refer [GET]]
            [flatland.turntable.db :refer [get-db]]
            [flatland.useful.seq :refer [groupings]]
            [flatland.useful.utils :refer [with-adjustments]]
            [lamina.query.struct :refer [parse-time-interval]])
  (:import (java.sql Timestamp)
           (java.util Date Calendar)))

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

(defn fetch-data [config running query field from until limit]
  (let [q (get-in running [query :query])
        key-field (keyword field)]
    (sql/with-connection (get-db config (:db q))
      (sql/with-query-results rows
        (if limit
          [(format "SELECT %s AS value, _time FROM \"%s\" LIMIT ?::int" field (:name q))
           limit]
          [(format "SELECT %s AS value, _time FROM \"%s\" WHERE _start >= ?::timestamp AND _start <= ?::timestamp"
                   field
                   (:name q))
           (Timestamp. (to-ms from))
           (Timestamp. (to-ms until))])
        (doall rows)))))

(defn split-target [target]
  (when target
    (let [segments (s/split target #":")]
      [(s/join ":" (butlast segments)) (last segments)])))

(defn points [config running targets from until limit offset]
  (let [query->target (into {} (for [target targets]
                                 [(str "&" (s/replace target "/" ":"))
                                  target]))]
    (for [[target datapoints]
          ,,(query/query-seqs (zipmap (keys query->target)
                                      (repeat nil))
                              {:payload :value :timestamp #(.getTime ^Date (:_time %))
                               :seq-generator (fn [query]
                                                (let [[query field] (split-target query)]
                                                  (fetch-data config running query field from until limit)))})]
      {:target (query->target target)
       :datapoints (for [{:keys [value timestamp]} datapoints]
                     [value (-> timestamp
                                (- offset)
                                (quot 1000))])})))

(defn parse-interval [^String s]
  (let [[sign s] (if (.startsWith s "-")
                   [- (subs s 1)]
                   [+ s])]
    (long (sign (lamina.query.struct/parse-time-interval s)))))

(defn add-error [points error target]
  (conj points {:target target
                :error error}))

(defn render-api [config running]
  (GET "/render" {{:strs [target from limit until shift]} :query-params}
       (let [targets (if (coll? target) ; if there's only one target it's a string, but if multiple are
                       target           ; specified then compojure will make a list of them
                       [target])
             [existing not-existing] (when (seq target)
                                       ((juxt filter remove)
                                        (comp (partial contains? @running)
                                              first
                                              split-target)
                                        targets))
             offset (or (and (seq shift)
                             (parse-interval shift))
                        0)
             now-date (Date. (+ offset (.getTime (Date.))))
             now-ms (.getTime now-date)
             [from until] (for [[timespec default] [[from (subtract-day now-date)]
                                                    [until now-date]]]
                            (unix-time
                              (if (seq timespec)
                                (Date. (+ now-ms (parse-interval timespec)))
                                default)))]
         (if-let [datapoints (points config @running existing from until limit offset)]
           (reduce #(add-error % "Target does not exist." %2) datapoints not-existing)
           []))))
