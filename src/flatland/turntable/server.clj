(ns flatland.turntable.server
  (:require [flatland.turntable.service :refer [turntable-routes init-saved-queries persist-results-to-atom]]))

(def config (merge (read-string (slurp "sample.config.clj"))
                   {:persist-fns [persist-results-to-atom]}))

(init-saved-queries config)

(def handler (turntable-routes config))