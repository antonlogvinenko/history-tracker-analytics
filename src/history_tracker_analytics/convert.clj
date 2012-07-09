(ns history-tracker-analytics.collect
  (:require [clojure.contrib.sql :as sql]
            [clojure.contrib.seq :as seq]
            [clj-time.local :as time]
            [clojure.contrib.json :as json])
  (:use [clojureql.core :only (table select where)]
        [clojure.contrib.json :only (read-json)]
        [clojure.contrib.prxml :only (prxml)]
        [clojure.contrib.duck-streams :only (with-out-writer)])
  (:import [java.io StringReader]
           [javax.xml.parsers DocumentBuilderFactory]
           [org.xml.sax InputSource]
           [java.text.SimpleDateFormat]
           [javax.xml.transform.dom DOMResult]))


(def remote-mysql-config "remote.ini")
(def local-mysql-config "local.ini")
(def mysql-settings {:classname "com.mysql.jdbc.Driver" :subprotocol "mysql"})
(defn- add-row [parameters row]
  (let [pair (.split row "=")]
    (->> pair
         second
         (assoc parameters (-> pair first keyword)))))
(defn- init-db [file]
    (reduce add-row mysql-settings (.split file "\n")))
(defn- configure []
  {:remote-db (init-db (slurp remote-mysql-config))
   :local-db (init-db (slurp local-mysql-config))})
(defn- dump-object [db object]
  (sql/with-connection db
    (sql/insert-records :history2 object)))
(defn- get-objects [db]
  (sql/with-connection db
    (sql/with-query-results rs ["select distinct user_space_id, type from history where type='bulletin' limit 200"]
      (vec rs))))


(defn parse-json-attribute [json-map attribute]
  (assoc json-map (. attribute getAttribute "name") (. attribute getAttribute "value")))
(defn parse-json-attributes [attributes]
  (let [attributes (map #(.item attributes %) (range (.getLength attributes)))]
    (reduce parse-json-attribute {} attributes)))
(defn- xml-to-json [xml]
 (-> xml get-root (. getElementsByTagName "attribute") parse-json-attributes))
(defn- join-json [history
                 {context :context state :state
                  local-revision :local_revision
                  user-space-revision :user_space_revision}]
  (let [state (xml-to-json (. state substring 54))
        context (xml-to-json (. context substring 54))]
    (conj history
          {:local-revision local-revision
           :user-space-revision user-space-revision
           :context context
           :state state})))
(defn- create-json-object-from [rs]
  (-> rs
      first
      (select-keys [:type :user_space_id])
      (assoc :history (->> rs
                           (reduce join-json [])
                           json/json-str))))


(defn convert [create-object-from]
  (let [{db :local-db} (configure)
        objects (get-objects db)]
    (println "objects total" objects)
    (doseq [index (-> objects count range)]
      (print index)
      (->> index
           (nth objects)
           (create-object db create-object-from)
           (dump-object db)))))