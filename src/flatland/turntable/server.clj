(ns flatland.turntable.server
  (:require [flatland.turntable.service :refer [turntable-routes init-saved-queries persist-results-to-atom
                                                persist-results-to-db]]))

(def config (merge (read-string (slurp "sample.config.clj"))
                   {:persist-fns [#_persist-results-to-atom persist-results-to-db]}))

(init-saved-queries config)

(def handler (turntable-routes config))
