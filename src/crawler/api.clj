(ns crawler.api
  (:require [org.httpkit.server :as server]
            [crawler.util :as util]
            [clojure.tools.logging :as log]
            [compojure.route :as route]
            [compojure.core :as compojure]
            [ring.middleware.defaults :as defaults]
            [ring.middleware.logger :as ring-logger]
            [clojure.data.json :as json]))

(defn parse-data [data]
  (json/read-str data))

(def cross-origin-headers {"Access-Control-Allow-Origin" "*"
                           "Access-Control-Allow-Headers" "Origin, X-Requested-With, Content-Type, Accept"
                           "Access-Control-Allow-Methods" "POST"})

(def results-limit 1000)

(defn search [crawler-state req]
  (let [{query "query"} (parse-data (String. (slurp (:body req))))
        headers (merge {"Content-Type" "application/json"}
                       cross-origin-headers)]
    (try
      (if query
        (let [results (util/search crawler-state query)]
          (log/info "searching" query "found" (count results) "results")
          {:body (json/json-str results)
           :status 200
           :headers headers})
        {:body (json/json-str {"success" "false"})
         :status 400
         :headers headers})
      (catch Throwable e
        (log/error e)
        {:body (json/json-str {"success" "false"})
         :status 400
         :headers headers}))))

(defn options [req]
  {:status 200
   :headers cross-origin-headers})

(defn stats [crawler-state req]
  (let [headers (merge {"Content-Type" "application/json"}
                       cross-origin-headers)]
    {:body (json/json-str (:stats @crawler-state))
     :status 200
     :headers headers}))

(defn app [crawler-state]
  (compojure/routes
   (compojure/POST "/" [] #(search crawler-state %))
   (compojure/OPTIONS "/" [] options)
   (compojure/GET "/" [] #(stats crawler-state %))))

(defn routes [crawler-state]
  (->
   (app crawler-state)
   (ring-logger/wrap-with-logger)
   (defaults/wrap-defaults  defaults/api-defaults)))

(defn start [crawler-state & {:keys [port] :or {port 8081}}]
  (server/run-server (routes crawler-state) {:port port
                                             :thread 10
                                             :queue-size 50}))

(defn stop [server]
  (server :timeout 30000))
