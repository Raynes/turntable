(ns flatland.turntable.persist
  (:require [flatland.turntable.jdbc-utils :refer [prepare table-exists?]]))

(defn persist-results-to-atom
  "Returns a function tresults to the @running atom."
  [config running query results]
  (swap! running update-in [(:name query) :results] conj results)
  results)

(defn persist-results-to-telemetry
  "Expects a :telemetry key in config which will be a telemetry client connection."
  [config query results]
  (let [{:keys [results time]} results
        {:keys [telemetry]} config
        log (:send telemetry)]
    (log (str "turntable:" (:name query)) {:time (quot time 1000) :rows results})))

(defn persist-results
  "Persist results with the functions in the config's :persist-fns."
  [config query results]
  (doseq [f (:persist-fns config)]
    (f config query results)))
