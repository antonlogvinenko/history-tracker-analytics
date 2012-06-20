(ns history-tracker-analytics.collect
  (:require [clojure.contrib.sql :as sql]
            [clojure.contrib.seq :as seq]
            [clj-time.local :as time]
            [clojure.contrib.json :as json])
  (:use [clojureql.core :only (table select where)])
  (:import [java.io StringReader]
           [javax.xml.parsers DocumentBuilderFactory]
           [org.xml.sax InputSource]
           [java.text.SimpleDateFormat]
           [javax.xml.transform.dom DOMResult]))

;;Reading MySQL settings
(def mysql-settings {:classname "com.mysql.jdbc.Driver" :subprotocol "mysql"})

(defn- add-row [parameters row]
  (let [pair (.split row "=")]
    (->> pair
         second
         (assoc parameters (-> pair first keyword)))))

(defn- init-db [file]
  (reduce add-row mysql-settings (.split file "\n")))

;;Counting size
(def known-entry-size-bytes 17)
(def distribution-file-name "distribution")
(def print-freq 100000)

(defn- count-entry-size [{context :context state :state}]
  (+ (* 2 (count context))
     (* 2 (count state))
     known-entry-size-bytes))

(defn- conjoin [coll entry]
  (let [id (entry :id)
        key (.hashCode (select-keys entry [:type :user_space_id]))
        size (count-entry-size entry)
        update #(+ size (if (nil? %) 0 %))]
    (do (if (-> id (mod print-freq) zero?) (println id (count coll) (time/local-now)))
        (update-in coll [key] update))))



;;Database functions
(def sql-query "SELECT id, type, user_space_id, state, context from history")

;;buggy mysql jdbc driver here, rewrote standard clojure wrapper library
(defn- iterate-history-entries-with [db]
  (do (Class/forName (db :classname))
      (let [conn (java.sql.DriverManager/getConnection
                                 (str "jdbc:" (db :subprotocol) ":" (db :subname))
                                 (db :user) (db :password))
            stmt (.createStatement conn)]
        (do (.setFetchDirection stmt java.sql.ResultSet/FETCH_FORWARD)
            (.setFetchSize stmt Integer/MIN_VALUE)
            (let [result-set (.executeQuery stmt sql-query)]
              (loop [result result-set coll (hash-map)]
                (if (.next result)
                  (let [result-hash {:id (.getInt result "id")
                                     :type (.getString result "type")
                                     :user_space_id (.getInt result "user_space_id")
                                     :context (.getString result "context")
                                     :state (.getString result "state")}]
                    (recur result (conjoin coll result-hash)))
                  coll)))))))

(defn collect-stats [statistics-file-name [file & other]]
  (let [db (init-db (slurp file))
        start-time (time/local-now)
        big-data (iterate-history-entries-with db)
        end-time (time/local-now)]
    (do (println start-time)
        (println "MySQL connection parameters: " db)
        (spit statistics-file-name (vals big-data))
        (println end-time))))



(def remote-mysql-config "remote.ini")
(def local-mysql-config "local.ini")

(def max-sql "select max(id) as extremeid from history")
(def min-sql "select min(id) as extremeid from history")  
(defn- get-id-extreme [sql]
  (sql/with-query-results rs [sql] (:extremeid (nth rs 0))))
(defn- get-object-null [next-id]
  (sql/with-query-results rs
    ["select user_space_id, type from history where id = ?" (next-id)] rs))
(defn- fetch-object [next-id]
  (loop [next-id next-id]
    (let [object (get-object-null next-id)]
      (if (nil? object) (recur next-id)
          (let [object (nth object 0)]
            {:user-space-id (object :user_space_id)
             :type (object :type)})))))
          

;;load entries
;;save entries
;;routine

(defn- fetch-random-objects [db amount]
  (sql/with-connection db
    (let [min-id (get-id-extreme min-sql)
          max-id (get-id-extreme max-sql)
          next-id #(+ (rand-int (+ max-id (- min-id) 1)) min-id)]
      (loop [result #{}]
        (if (= (count result) amount) result
            (recur (->> next-id fetch-object (conj result))))))))

(defn- fetch-history [db {id :user-space-id type :type}]
  (sql/with-connection db
    (sql/with-query-results entries
      ["select *  from history where user_space_id = ? and type = ?" id type]
      (println "history size " (count entries))
      (loop [entries entries result []]
        (if (first entries)
          (recur (rest entries) (-> entries first (cons result)))
          result)))))

(defn- insert-history [db entry]
  (sql/with-connection db
    (sql/insert-records :history entry)))

(defn- configure []
  {:remote-db (init-db (slurp remote-mysql-config))
   :local-db (init-db (slurp local-mysql-config))})

(defn load-sample [amount]
  (let [{remote-db :remote-db local-db :local-db} (configure)
        objects (fetch-random-objects remote-db amount)]
    (doseq [object objects]
      (println "processing" object)
      (try (doseq [entry (fetch-history remote-db object)]
             (insert-history local-db entry))
           (catch Exception e (println e))))))




(defn- get-objects [db]
  (sql/with-connection db
    (sql/with-query-results rs ["select distinct user_space_id, type from history"]
      (vec rs))))

(defn- join-xml [history
                 {context :context state :state
                  local-revision :local_revision
                  user-space-revision :user_space_revision}]
  (let [state (. state substring 54)
        context (. context substring 54)]
    (str history
         "<object-state local_revision='" local-revision "' "
         "user_space_revision='" user-space-revision "'>"
         "<context>" context "</context>"
         "<state>" state "</state>"
         "</object-state>")))

(defn- create-xml-object-from [rs]
  (assoc (select-keys (first rs) [:type :user_space_id])
    :history (str "<history>" (reduce join-xml "" rs) "</history>")))

(defn- dump-object [db object]
  (sql/with-connection db
    (sql/insert-records :history2 object)))

(defn- create-object [db create-object-from object]
  (sql/with-connection db
    (sql/with-query-results rs
      [(str "select * from history "
            "where user_space_id = ? and type = ? "
            "order by user_space_revision asc, local_revision asc")
       (object :user_space_id) (object :type)]
      (println " [" (count rs) "]")
      (create-object-from rs))))

(defn convert [create-object-from]
  (let [{db :local-db} (configure)
        objects (get-objects db)]
    (println "objects total" (count objects))
    (doseq [index (-> objects count range)]
      (print index)
      (->> index
           (nth objects)
           (create-object db create-object-from)
           (dump-object db)))))








(defn measure-times [n f object]
  (loop [n n acc 0]
    (if (zero? n) acc
        (let [start (. System currentTimeMillis)
              something (f object)
              end (. System currentTimeMillis)]
          (recur (dec n) (-> end (- start) (+ acc)))))))

(defn measure [n f object]
  (-> n
      (measure-times f object)
      double
      (/ n)))

(defn get-root [text]
  (let [source (InputSource. (StringReader. text))]
    (.. DocumentBuilderFactory
        newInstance newDocumentBuilder
        (parse source) getDocumentElement)))

(defn get-attributes [object name]
  (.. object (getElementsByTagName name) (item 0)
      (getElementsByTagName "attributes") (item 0)
      (getElementsByTagName "attribute")))

(defn- get-int [value]
  (try (Integer/parseInt value) (catch Exception e)))

(defn- get-double [value]
  (try (Double/parseDouble value) (catch Exception e)))

(defn- get-date [value]
  (try (let [f (java.text.SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:SS")]
         (. f parse value))
       (catch Exception e)))

(defn- convert-attr [value]
  (if-let [parsed (get-int value)] parsed
          (if-let [parsed (get-double value)] parsed
                  (if-let [parsed (get-date value)] parsed
                          value))))

(defn parse-attribute [list attribute]
  (cons
   (reduce
    #(assoc %1 %2 (. attribute getAttribute %2)) {} ["value" "name"])
   list))

(defn parse-attributes [attributes]
  (let [attributes (map #(.item attributes %) (range (.getLength attributes)))]
    (reduce parse-attribute [] attributes)))

(defn parse-object-state [object-state]
  (reduce
   #(assoc %1 %2 (-> object-state (get-attributes %2) parse-attributes))
   {} ["state" "context"]))

(defn merge-history [history state]
  (merge history state))

(defn build-all [history object-state]
  (->> object-state parse-object-state (merge-history history)))

(defn routine-xml [object]
  (let [history (get-root object)
        states (. history getElementsByTagName "object-state")
        states (map #(.item states %) (range (.getLength states)))]
    (spit (str "./out/very-temp-" (rand)) (reduce build-all {} states))))
  







        
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

(defn parse-json [object]
  (json/read-json object))

(defn update-values [m f]
  (reduce (fn [r [k v]] (->> v f (assoc r k))) {} m))

(defn- convert-and-merge [json1 json2]
  (merge json1 json2))
;;  (merge (update-values json1 convert-attr)
;;         (update-values json2 convert-attr)))

(defn routine-json [object]
  (->> object
       parse-json
       (map #(select-keys % [:state :context]))
       (reduce (partial merge-with merge))
       (spit (str "./out/temp-" (rand)))))

(defn analyse-converted-history [f]
  (let [{db :local-db} (configure)]
    (sql/with-connection db
      (sql/with-query-results rs ["select history from history2"]
        (loop [result rs coll []]
          (if (seq result)
            (recur (rest result) (cons (->> result first :history (measure 10 f)) coll))
            coll))))))

(defn a [c] (println c c))

;;watch -n 10 "mysql test -e 'select count(distinct user_space_id, type) from history'"
;;watch -n 10 "mysql test -e 'select count(*) from history'"
;;watch -n 10 "mysql test -e 'select count(*) from history2'"