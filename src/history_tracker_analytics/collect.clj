(ns history-tracker-analytics.collect
    (:require [clojure.contrib.sql :as sql]
              [clj-time.local :as time])
    (:use [clojureql.core :only (table select where)]))

;;Reading MySQL settings
(def mysql-settings {:classname "com.mysql.jdbc.Driver" :subprotocol "mysql"})

(defn- add-row [parameters row]
  (let [pair (.split row "=")]
    (assoc parameters (keyword (first pair)) (second pair))))

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
    (do (if (= (mod id print-freq) 0) (println id (count coll) (time/local-now)))
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
(defn- get-object [next-id]
  (loop [next-id next-id]
    (let [object (get-object-null next-id)]
      (if (nil? object) (recur next-id)
          (let [object (nth object 0)]
            {:user-space-id (object :user_space_id)
             :type (object :type)})))))

;;load entries
;;save entries
;;routine

(defn- get-random-objects [amount]
  (let [min-id (get-id-extreme min-sql)
        max-id (get-id-extreme max-sql)
        next-id #(+ (rand-int (+ max-id (- min-id) 1)) min-id)]
    (loop [result #{}]
      (if (= (count result) amount) result
        (recur (conj result (get-object next-id)))))))
        
        
(defn load-sample [amount]
  (let [local-db (init-db (slurp local-mysql-config))
        remote-db (init-db (slurp remote-mysql-config))]
    (sql/with-connection remote-db
      (time (get-random-objects amount)))))

