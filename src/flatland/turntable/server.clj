(ns flatland.turntable.server
  (:require [flatland.turntable.service :refer [turntable-routes init-saved-queries]]))

(def config (read-string (slurp "sample.config.clj")))

(init-saved-queries config)

(def handler (turntable-routes config))