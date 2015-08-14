(ns crawler.crawler
  (:import [org.apache.commons.net.ftp FTPFile])
  (:require [miner.ftp :as ftp]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [crawler.util :as util]))

(defn blacklisted-dir? [path]
  (let [depth (count (string/split path #"/"))]
    (or (> depth 6) (re-find #"lost\+found" path))))

(defn map-ftp-file [current-dir ^FTPFile f]
  {:crawl-timestamp (System/currentTimeMillis)
   :name (str current-dir (.getName f))
   :size (.getSize f)})

(defn crawl [state ip]
  (let [store (:store @state)
        start (System/currentTimeMillis)]
    (log/info "crawling" ip)
    (try
      (ftp/with-ftp [client (util/build-ftp-url ip "/")]
        (loop [to-analyze #{"/"}]
          (let [current-dir (first to-analyze)
                _ (ftp/client-cd client current-dir)
                sub-dirs (ftp/client-directory-names client)
                new-to-analyze (into (disj to-analyze current-dir)
                                     (->> (map #(str current-dir % "/") sub-dirs)
                                          (remove blacklisted-dir?)))
                _ (Thread/sleep 150)
                files (map (partial map-ftp-file current-dir)
                           (ftp/client-FTPFiles client))]
            (util/add-files-to-store state ip files)
            (when (seq new-to-analyze)
              (recur new-to-analyze)))))
      (catch Throwable e
        (log/error "While crawling" ip e))
      (finally
        (log/info "crawling" ip
                  "took" (quot (- (System/currentTimeMillis)
                                               start)
                               (* 1000 60))
                  "minutes")))))
