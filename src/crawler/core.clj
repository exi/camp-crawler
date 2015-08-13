(ns crawler.core
  (:gen-class)
  (:require [miner.ftp :as ftp]
            [net.cgrand.enlive-html :as html]
            [cemerick.url :as cem-url]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [clojure.pprint :refer [pprint]]
            [crawler.manager :as manager]
            [crawler.util :as util]
            [crawler.api :as api]))

(def wikipage "https://events.ccc.de/camp/2015/wiki/FTP")

(defn get-wikipage []
  (->> (java.net.URL. wikipage)
       (html/html-resource)))

(defn extract-ftp-urls [content]
  (->> (html/select content [:li :a.external])
       (map #(get-in % [:attrs :href]))
       (remove nil?)
       (filter #(= "ftp://" (subs % 0 6)))))

(defn extract-ftp-ip [url]
  (if-let [match (re-matches #"ftp://(\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3})" url)]
    (second match)
    nil))

(defn get-ftp-ips []
  (->> (get-wikipage)
       (extract-ftp-urls)
       (map extract-ftp-ip)
       (remove nil?)))

(defn stop-crawlers [crawler-list]
  (doall (for [f crawler-list]
           (future-cancel f))))

(defn wait-for-crawlers [crawler-list & {:keys [timeout]}]
  (doall (for [f crawler-list]
           (try
             (deref f timeout :timeout)
             (catch Throwable e
               (log/error "error" e))))))

(defonce state (atom nil))
(defonce server (atom nil))

(defn start []
  (reset! state (manager/start))
  (dorun (map (partial manager/add-ip @state) (get-ftp-ips)))
  (manager/schedule @state)
  (reset! server (api/start @state))
  nil)

(defn stop []
  (manager/stop @state)
  (api/stop @server)
  nil)

(defn -main []
  (start)
  (loop []
    (Thread/sleep 60000)
    (try
      (dorun (map (partial manager/add-ip @state) (get-ftp-ips)))
      (catch Throwable e
        (log/error "error" e)))
    (manager/schedule @state)
    (recur)))
