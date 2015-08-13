(ns crawler.manager
  (:import [java.io File DataOutputStream FileOutputStream FileInputStream DataInputStream])
  (:require [crawler.crawler :as crawler]
            [crawler.util :as util]
            [clucy.core :as clucy]
            [clojure.tools.logging :as log]
            [clojure.string :as string]
            [cognitect.transit :as transit]
            [clojure.core.async :as as]
            [clojure.edn :as edn]
            [taoensso.nippy :as nippy]
            [clojure.java.io :as io]))

(def nippy-store-file "store.nippy")
(def transit-store-file "store.transit")

(defn new-state [] (atom {:ips #{}
                          :crawlers {}
                          :store (atom {})
                          :writer nil
                          :stats {}
                          :stats-writer nil
                          :cleaner nil
                          :index (clucy/disk-index "lucene.index")
                          :indexer nil
                          :index-input (as/chan 1000)}))

(defn cleanup-finished-crawlers [state]
  (->> (let [crawlers (:crawlers @state)]
         (reduce
          (fn [crawlers [ip instance]]
            (if (realized? instance)
              (do
                (log/info "cleaning worker for ip" ip)
                (dissoc crawlers ip))
              crawlers))
          crawlers crawlers))
       (swap! state (fn [old crawlers] (assoc old :crawlers crawlers)))))

(defn start-crawlers [state-atom]
  (let [state @state-atom
        crawlers (:crawlers state)
        ips (:ips state)
        new-crawlers (reduce
                      (fn [crawlers ip]
                        (if (contains? crawlers ip)
                          crawlers
                          (assoc crawlers ip (future (crawler/crawl state-atom ip)))))
                      crawlers
                      ips)]
    (swap! state-atom assoc :crawlers new-crawlers)))

(defn stop-crawlers [state]
  (dorun (for [[ip instance] (:crawlers @state)]
           (do
             (log/info "stopping worker for ip" ip)
             (future-cancel instance))))
  (cleanup-finished-crawlers state)
  nil)

(defn write-store [store]
  (let [f (File/createTempFile "store" "tmp" (File. "."))]
    (with-open [out (FileOutputStream. f)]
      (transit/write (transit/writer out :msgpack) store))
    (.delete (File. transit-store-file))
    (.renameTo f (File. transit-store-file))))

(defn read-store []
  (cond
    (.exists (File. transit-store-file)) (do
                                           (log/info "reading" transit-store-file)
                                           (with-open [in (FileInputStream. (File. transit-store-file))]
                                             (transit/read (transit/reader in :msgpack))))
    (.exists (File. nippy-store-file)) (do
                                   (log/info "reading" nippy-store-file)
                                   (with-open [in (FileInputStream. (File. nippy-store-file))]
                                     (nippy/thaw-from-in! (DataInputStream. in))))
    :default {}))

(defn import-store [state]
  (let [store (read-store)]
    (dorun (for [[ip files] store]
             (util/add-files-to-store state ip files)))
    (log/info "imported" (reduce + (map count (mapcat second store))) "files")))

(defn start-writer [state]
  (let [writer (future (try (loop []
                              (Thread/sleep (* 1000 60 10))
                              (log/info "writing store")
                              (write-store @(:store @state))
                              (recur))
                            (catch Throwable e
                              (log/error "writer died" e))))]
    (swap! state assoc :writer writer)))

(defn stop-writer [state]
  (when-let [writer (:writer @state)]
    (log/info "killing store writer")
    (future-cancel writer))
  (swap! state assoc :writer nil))

(defn update-stats [state]
  (swap! state assoc :stats (util/index-size state)))

(defn start-stats [state]
  (let [stats-writer (future (try (loop []
                                    (Thread/sleep 5000)
                                    (log/info "writing stats")
                                    (update-stats state)
                                    (recur))
                                  (catch Throwable e
                                    (log/error "stats died" e))))]
    (swap! state assoc :stats-writer stats-writer)))

(defn stop-stats [state]
  (when-let [stats-writer (:stats-writer @state)]
    (log/info "killing stats writer")
    (future-cancel stats-writer))
  (swap! state assoc :stats-writer nil))

(defn start-indexer [state]
  (let [index-input (:index-input @state)
        index (:index @state)
        indexer (future (try (loop []
                               (let [[cmd documents] (as/<!! index-input)]
                                 (case cmd
                                   :add (do
                                          (log/info "indexing add" (count documents) "documents")
                                          (apply (partial clucy/delete index) documents)
                                          (apply (partial clucy/add index) documents))
                                   :delete (do
                                             (log/info "indexing delete" (count documents) "documents")
                                             (apply (partial clucy/delete index) documents)))
                                 (recur)))
                             (catch Throwable e
                               (log/error "indexer died" e)
                               e)))]
    (swap! state assoc :indexer indexer)
    nil))

(defn stop-indexer [state]
  (when-let [indexer (:indexer @state)]
    (log/info "killing indexer")
    (future-cancel indexer))
  (swap! state assoc :indexer nil)
  nil)

(defn start-cleaner [state]
  (let [index (:index @state)
        store (:store @state)
        cleaner (future (try (loop []
                               (log/info "cleaner run")
                               (dorun
                                (for [[ip files] (util/get-old-files-from-store @store)]
                                  (util/remove-files-from-store state ip files)))
                               (Thread/sleep 60000)
                               (recur))
                             (catch Throwable e
                               (log/error "cleaner died" e)
                               e)))]
    (swap! state assoc :cleaner cleaner)
    nil))

(defn stop-cleaner [state]
  (when-let [cleaner (:cleaner @state)]
    (log/info "killing cleaner")
    (future-cancel cleaner))
  (swap! state assoc :cleaner nil)
  nil)

(defn schedule [state]
  (cleanup-finished-crawlers state)
  (start-crawlers state)
  nil)

(defn add-ip [state ip]
  (swap! state (fn [old] (assoc old :ips (conj (:ips old) ip))))
  nil)

(defn start []
  (let [state (new-state)]
    (start-stats state)
    (start-indexer state)
    (start-cleaner state)
    (import-store state)
    (start-writer state)
    (schedule state)
    state))

(defn stop [state]
  (stop-crawlers state)
  (stop-writer state)
  (stop-stats state)
  (stop-cleaner state)
  (stop-indexer state)
  nil)
