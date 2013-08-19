(defproject org.flatland/turntable "0.3.1"
  :description "A service for running SQL queries every n minutes."
  :url "https://github.com/flatland/turntable"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.flatland/useful "0.10.1"]
                 [compojure "1.1.5"]
                 [cheshire "5.0.1"]
                 [ring-middleware-format "0.2.4"]
                 [flatland/ring-cors "0.0.7"]
                 [org.clojure/java.jdbc "0.2.3"]
                 [postgresql/postgresql "8.4-702.jdbc4"]
                 [me.raynes/fs "1.4.0"]
                 [org.flatland/chronicle "0.2.1"]
                 [org.flatland/turntable-ui "0.3.1"]
                 [org.flatland/telemetry-client "0.2.0"]
                 [lib-noir "0.5.5"]]
  :plugins [[lein-ring "0.8.2"]]
  :ring {:handler flatland.turntable.server/handler
         :open-browser? false})
