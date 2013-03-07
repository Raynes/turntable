(ns flatland.turntable.server
  (:require [flatland.turntable.service :refer [turntable-routes]]
            [compojure.handler :refer [api]]
            [compojure.core :refer [routes]]
            [compojure.route :refer [not-found]]
            (ring.middleware [format-params :refer :all]
                             [format-response :refer :all])))

(def handler
  (-> (routes turntable-routes (not-found nil)) 
      (api)
      (wrap-json-params)
      (wrap-json-response)))