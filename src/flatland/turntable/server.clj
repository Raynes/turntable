(ns flatland.turntable.server
  (:require [flatland.turntable.service :refer [turntable-routes init-saved-queries]]))

(def config {:query-file "queries.clj"})

(init-saved-queries config)

(def handler turntable-routes)