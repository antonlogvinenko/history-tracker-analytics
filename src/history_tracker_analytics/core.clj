(ns history-tracker-analytics.core
  (:require [clojure.contrib.sql :as sql]
            [clojure.java.io :as io]
            [clj-time.local :as time]))

;;Reading MySQL settings
(def mysql-settings {:classname "com.mysql.jdbc.Driver" :subprotocol "mysql"})

(defn add-row [parameters row]
  (let [pair (.split row "=")]
    (assoc parameters (keyword (first pair)) (second pair))))

(defn init-db [file]
  (reduce add-row mysql-settings (.split file "\n")))

;;Counting size
(def known-entry-size-bytes 10)

(defn count-entry-size [{context :context state :state}]
  (+ (* 2 (count context))
     (* 2 (count state))
     known-entry-size-bytes))

(defn conjoin [coll entry]
  (let [key (select-keys entry [:type :user_space_id])
        size (count-entry-size entry)
        update #(+ size (if (nil? %) 0 %))]
    (update-in coll [key] update)))

(defn reduce-sql-results [results] (reduce conjoin (hash-map) results))

;;Database functions
(def sql-query "SELECT id, type, user_space_id, state, context from history limit 1000")

(defn iterate-history-entries-with [db]
  (sql/with-connection db
    (sql/with-query-results results [sql-query]
      {:fetch-size 1000}
      (reduce-sql-results results))))

;;Printing
(defn dump-to-file [big-data]
  (println big-data))

(defn -main [[file & other]]
  (let [db (init-db (slurp file))
        big-data (iterate-history-entries-with db)]
    (do (println (time/local-now))
        (println "MySQL connection parameters: " db)
        (dump-to-file big-data)
        (println (time/local-now)))))