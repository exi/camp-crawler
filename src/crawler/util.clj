(ns crawler.util
  (:require [clojure.string :as string]
            [clojure.core.async :as as]
            [clucy.core :as clucy]
            [cemerick.url :as cem-url]))

(def max-age-millis (* 1000 60 60 3))

(defn build-ftp-url [^String ip ^String path]
  (let [parts (string/split path #"/")]
    (apply str
           (concat
            ["ftp://anonymous@"
             ip]
            (->> (map cem-url/url-encode parts)
                 (string/join "/" ))
            (if (= \/ (last path))
              ["/"]
              [])))))

(defn index-size [state]
  (for [[k v] @(:store @state)] {:ip k
                                 :files (count v)
                                 :size (reduce + (map :size v))}))

(defn search [state pattern]
  (as-> (:index @state) x
    (clucy/search x pattern 10000)
    (into #{} x)
    (sort-by :name x)))

(defn merge-files-into-store [old ip items]
  (let [paths (into #{} items)]
    (assoc old ip (into (get old ip #{})
                        paths))))

(defn add-url-to-file [ip file]
  (assoc file :url (build-ftp-url ip (:name file))))

(defn add-files-to-store [state ip files]
  (swap! (:store @state) merge-files-into-store ip files)
  (let [files-with-url (map (partial add-url-to-file ip) files)]
    (as/>!! (:index-input @state) [:add files-with-url])))

(defn remove-files-from-store [state ip files]
  (swap! (:store @state)
         (fn [old]
           (let [new-files (apply disj (get old ip) files)]
             (if (seq new-files)
               (assoc old ip new-files)
               (dissoc old ip)))))
  (let [files-with-url (map (partial add-url-to-file ip) files)]
    (as/>!! (:index-input @state) [:delete files-with-url])))

(defn get-old-files-from-store [store]
  (into {}
        (for [[ip files] store]
          [ip (->> files
                   (filter
                    (fn [file]
                      (let [timestamp (get file :crawl-timestamp 0)]
                        (< timestamp (- (System/currentTimeMillis) max-age-millis))))))])))
