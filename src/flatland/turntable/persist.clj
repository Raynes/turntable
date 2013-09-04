(ns flatland.turntable.persist
  (:require [flatland.turntable.jdbc-utils :refer [prepare table-exists?]]))

(defn persist-results-to-atom
  "Returns a function tresults to the @running atom."
  [config running query results _]
  (swap! running update-in [(:name query) :results] conj results)
  results)

(defn persist-results-to-telemetry
  "Expects a :telemetry key in config which will be a telemetry client connection."
  [config query results replay?]
  (let [{:keys [results time]} results
        {:keys [telemetry]} config
        log (:send telemetry)
        topic (str "turntable:" (:name query))]
    (log (if replay? "turntable-replay" topic)
         (merge {:time (quot time 1000) :rows results}
                (when replay? {:replay-topic topic})))))

(defn persist-results
  "Persist results with the functions in the config's :persist-fns."
  [config query results replay?]
  (doseq [f (:persist-fns config)]
    (f config query results replay?)))
