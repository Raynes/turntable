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

(defn points [config running targets from until limit offset]
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
                                                  (fetch-data config running query field from until limit)))})]
      {:target (query->target target)
       :datapoints (for [{:keys [value timestamp]} datapoints]
                     [value (-> timestamp
                                (- offset)
                                (quot 1000))])})))

(defn parse-timespec [timespec]
  (-> timespec
      (s/replace "-" "")
      (lamina.query.struct/parse-time-interval)
      (long)))

(defn render-api [config running]
  (GET "/render" {{:strs [target from limit until shift]} :query-params}
       (let [targets (if (coll? target) ; if there's only one target it's a string, but if multiple are
                       target           ; specified then compojure will make a list of them
                       [target])
             now-date (Date.)
             now-ms (.getTime now-date)
             negate? (when shift (.startsWith shift "-"))
             shift (when shift (partial (if negate? #(- %2 %) +) (parse-timespec shift)))
             [from until] (for [[timespec default] [[from (subtract-day now-date)]
                                                    [until now-date]]]
                            (unix-time
                              (if (seq timespec)
                                (let [unshifted-time (- now-ms (parse-timespec timespec))]
                                  (Date. (if shift
                                           (shift unshifted-time)
                                           unshifted-time)))
                                default)))]
         (or (points config @running targets from until limit shift)
             {:status 404}))))
