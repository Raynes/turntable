(ns flatland.turntable.server
  (:require [flatland.turntable.service :refer [readers writers]]
            [compojure.handler :refer [api]]
            [compojure.core :refer [routes]]
            [compojure.route :refer [not-found]]
            (ring.middleware [format-params :refer :all]
                             [format-response :refer :all])))

(def handler
  (-> (routes readers writers (not-found nil)) 
      (api)
      (wrap-json-params)
      (wrap-json-response)))