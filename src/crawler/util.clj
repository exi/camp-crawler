(ns crawler.util
  (:require [clojure.string :as string]
            [clojure.core.async :as as]
            [clucy.core :as clucy]
            [cemerick.url :as cem-url]))

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
    (as/>!! (:index-input @state) files-with-url)))
