(ns flatland.turntable.server
  (:require [flatland.turntable.service :refer [turntable-routes init-saved-queries init-config]]
            [flatland.turntable.persist :refer [persist-results-to-db]]))

(def config (merge (read-string (slurp "sample.config.clj"))
                   (init-config {:persist-fns [persist-results-to-db]})))

(init-saved-queries config)

(def handler (turntable-routes config))
