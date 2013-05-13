(ns flatland.turntable.server
  (:require [flatland.turntable.service :refer [turntable-routes init-saved-queries]]
            [flatland.turntable.persist :refer [persist-results-to-db]]))

(def config (merge (read-string (slurp "sample.config.clj"))
                   {:persist-fns [persist-results-to-db]}))

(init-saved-queries config)

(def handler (turntable-routes config))
