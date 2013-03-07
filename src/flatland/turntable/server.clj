(ns flatland.turntable.server
  (:require [flatland.turntable.service :refer [turntable-routes init-saved-queries]]
            [compojure.handler :refer [api]]
            [compojure.core :refer [routes]]
            [compojure.route :refer [not-found]]
            (ring.middleware [format-params :refer :all]
                             [format-response :refer :all])))

(def config {:query-file "queries.clj"})

(init-saved-queries config)

(def handler
  (-> (routes (turntable-routes config)
              (not-found nil)) 
      (api)
      (wrap-json-params)
      (wrap-json-response)))