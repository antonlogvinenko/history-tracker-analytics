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
;;omfg constant entry size turned to be 42!!!
(def known-entry-size-bytes 42)

(defn count-entry-size [{context :context state :state}]
  (+ (* 2 (count context))
     (* 2 (count state))
     known-entry-size-bytes))

(defn conjoin [coll entry]
  (let [id (entry :id)
        key (select-keys entry [:type :user_space_id])
        size (count-entry-size entry)
        update #(+ size (if (nil? %) 0 %))]
    (do (if (= (mod id 10000) 0) (println id))
        (update-in coll [key] update))))

(defn reduce-sql-results [results] (reduce conjoin (hash-map) results))

;;Database functions
(def sql-query "SELECT id, type, user_space_id, state, context from history limit 500000")

(defn iterate-history-entries-with [db]
  (sql/with-connection db
    (sql/with-query-results results [sql-query]
      {:fetch-size 1000}
      (reduce-sql-results results))))

;;Printing - not that i'll use these dummy wrappy functions, just not to foget how to print/read
(defn read-from-file [file] (load-file file))

;(use '(incanter core charts stats))
;(defn main [& args]
;  (view (histogram (sample-normal 1000))))

(defn -main [file & other]
  (let [d (println file)
        db (init-db (slurp file))
        big-data (iterate-history-entries-with db)]
    (do (println (time/local-now))
        (println "MySQL connection parameters: " db)
        (spit "stats.raw" big-data)
        (println (time/local-now)))))