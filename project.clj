(defproject crawler "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main crawler.core
  :uberjar-name "crawler.jar"
  :profiles {:uberjar {:aot [crawler.core]}}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [com.velisco/clj-ftp "0.3.3"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [http-kit "2.1.18"]
                 [clucy "0.4.0"]
                 [com.cognitect/transit-clj "0.8.281"]
                 [enlive "1.1.6"]
                 [com.taoensso/nippy "2.9.0"]
                 [ring.middleware.logger "0.5.0"]
                 [org.clojure/data.json "0.2.6"]
                 [ring/ring-defaults "0.1.5"]
                 [compojure "1.4.0"]
                 [com.cemerick/url "0.1.1"]
                 [org.clojure/tools.logging "0.3.1"]])
