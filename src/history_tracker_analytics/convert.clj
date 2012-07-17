(ns history-tracker-analytics.convert
  (:require [clojure.contrib.sql :as sql]
            [clojure.contrib.seq :as seq]
            [clj-time.local :as time]
            [clojure.contrib.profile :as profile])
  (:use [clojureql.core :only (table select where)]
        [clojure.data.json :only (json-str read-json)]
        [clojure.contrib.prxml :only (prxml)]
        [clojure.contrib.duck-streams :only (with-out-writer)])
  (:import [java.io StringReader]
           [javax.xml.parsers DocumentBuilderFactory]
           [org.xml.sax InputSource]
           [java.text.SimpleDateFormat]
           [javax.xml.transform.dom DOMResult]))


(def df (java.text.SimpleDateFormat. "yyyy-MM-dd HH:mm:ss"))
(def df2 (java.text.SimpleDateFormat. "yyy-MM-dd'T'HH:mm:ss"))
(def do-display true)

(defn- get-int [value]
  (try (Integer/parseInt value) (catch Exception e)))

(defn- get-double [value]
  (try (Double/parseDouble value) (catch Exception e)))

(defn- get-date [value]
  (try (->> value (.parse df) (.format df))
       (catch Exception e (try (->> value (.parse df2) (.format df))
                               (catch Exception e value)))))

(def convert-attr)

(defn- get-array [attribute]
  (let [elements (-> attribute
                     (.getElementsByTagName "array")
                     (.item 0)
                     (.getElementsByTagName "element"))]
    (map #(->> % (.item elements) convert-attr) (range (.getLength elements)))))
;;xml to clojure data structures
(defn get-value [attribute]
  (let [field (->> "value" (. attribute getAttribute))]
    (if (-> field empty? not)
      field
      (let [text (-> (. attribute getTextContent))]
        (if (-> text empty? not)
          text
          "")))))

(defn- convert-attr [attribute]
  (let [value (get-value attribute)
        type (.getAttribute attribute "type")]
    (case type
      "removed" nil
      "string" (get-date value)
      "array" (get-array attribute)
      "integer" (Integer/parseInt value)
      "float" (Float/parseFloat value)
      "datetime" (get-date value)
      "boolean" (Boolean/parseBoolean value)
      value)))


;;database settings
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
(defn- get-objects [db type start end]
  (sql/with-connection db
    (sql/with-query-results rs
      ["select distinct user_space_id, type from history where type=? and user_space_id >= ? and user_space_id < ?" type start end]
      (vec rs))))
(defn- dump-object [db objects]
  (sql/with-connection db
    (apply sql/insert-values :history2 [:type :user_space_id :history] objects)))
(defn- create-object [db create-object-from object]
  (sql/with-connection db
    (sql/with-query-results rs
      [(str "select * from history "
            "where user_space_id = ? and type = ? "
            "order by user_space_revision asc, local_revision asc")
       (object :user_space_id) (object :type)]
      ;;      (print (object :user_space_id) ", ")
      (create-object-from rs))))



(defn get-root [text]
  (let [source
        (InputSource. (StringReader. text))
        document-builder
        (.newDocumentBuilder
         (doto (DocumentBuilderFactory/newInstance)
           (.setNamespaceAware false)
           (.setValidating false)
           (.setFeature "http://xml.org/sax/features/namespaces" false)
           (.setFeature "http://xml.org/sax/features/validation" false)
           (.setFeature "http://apache.org/xml/features/nonvalidating/load-dtd-grammar" false)
           (.setFeature "http://apache.org/xml/features/nonvalidating/load-external-dtd" false)))]
    (.. document-builder (parse source) getDocumentElement)))
(defn parse-json-attribute [json-map attribute]
  (assoc json-map (. attribute getAttribute "name") (convert-attr attribute)))
(defn parse-json-attributes [attributes]
  (let [attributes (map #(.item attributes %) (range (.getLength attributes)))]
    (reduce parse-json-attribute (array-map) attributes)))
(defn- xml-to-json [xml]
 (-> xml get-root (. getElementsByTagName "attribute") parse-json-attributes))
(defn- join-json [history
                  {context :context state :state
                   state-type :state_type
                   state-time :state_time
                  user-space-revision :user_space_revision}]
  (let [state (xml-to-json state)
        context (xml-to-json context)]
    (merge
     history
     (assoc
         (if (nil? user-space-revision) {} {:user-space-revision user-space-revision})
       :state-type state-type
       :state-time (->> state-time .getTime (java.util.Date.) (.format df))
       :context context
       :state state))))


(defmulti history-merge (fn [x y] (every? map? [x y])))
(defmethod history-merge false [x y] y)
(defmethod history-merge true [x y]
  (dissoc
   (if (-> :state-type y (= "snapshot")) y
       (->>
        (merge-with history-merge x y)
        (filter (comp not nil? val))
        (into (array-map))))
   :state-type))
(defn history-reductions [increments]
  (->> increments
       (reductions history-merge (array-map))
       rest))


;;history to increments
(defn state-diff [a b]
  (if (every? (partial instance? java.util.Map) [a b])
    (reduce
     (fn [diff key]
       (let [av (a key) bv (b key)]
         (if (= av bv)
           diff
           (assoc diff key (if (and av bv) (state-diff av bv) bv)))))
     (array-map) (concat (keys a) (keys b)))
    b))
(defn history-to-increments [history]
  (loop [prev (array-map), history history, increments []]
    (if (empty? history) increments
        (recur
         (first history)
         (rest history)
         (->> history first (state-diff prev) (conj increments))))))

(defn typed-pack [type rs]
  (if (= type "bulletin")
    {:ol rs :ul []}
    {:ul rs :ol []}))

(defn display[x] (if do-display (println x)) x)
(defn send-json[x] (json-str x :escape-unicode false))
(defn- create-json-object-from [rs]
  (let [a (first rs)]
    [(a :type) (a :user_space_id)
     (->> 
          (reduce join-json [] rs)
          history-reductions
          history-to-increments
          (typed-pack (a :type))
          send-json)]))


(defn convert [create-object-from type start end]
  (let [{db :local-db} (configure)
        objects (get-objects db type start end)
        new-objects (vec (map (partial create-object db create-object-from) objects))]
    (println "objects total" (count objects))
    (dump-object db new-objects)))

(defn parts[start step times]
  (->> times
       range
       (map (partial * step))
       (map (partial + start))
       (map #(list % (+ % step)))))


(def do-display false)
(defn convert-json [type start end]
  (time (seq? (convert create-json-object-from type start end))))


(defn convert-json-parallel [type start step times]
  (->>
   times
   (parts start step)
   (pmap #(apply convert-json type %))))


;;8 threads

;;1. calculate, then update +11% 
;;2. batch insert


(defn crazy []
  (sql/with-connection (:remote-db (configure))
    (sql/with-query-results rs ["select history from history2 where type=? and user_space_id=?"
                                "bulletin" 2839686]
      (sql/update-or-insert-values
       :history2
       ["type=? and user_space_id= ?" "bulletin" 2839686]
       {:history
        (-> rs
            first
            :history
            read-json
            (assoc-in [:ol 0 :state :super-new-field] "message from the past, i hope you lived your life")
            json-str)}))))

(defn crazy-2 []
  (sql/with-connection (:remote-db (configure))
    (sql/with-query-results rs ["select history from history2 where type=? and user_space_id=?"
                                "bulletin" 2839686]
      (-> rs first :history read-json :ol (nth 0) println))))

(defn crazy-3 [amount]
  (sql/with-connection (:remote-db (configure))
    (sql/with-query-results rs ["select user_space_id, type from history limit ?" amount]
      (-> rs count println))))


;;read with batch
;;use pmap
;;recalculate 36000 - 1 hour
;;local performance?
;;continue profiling