(ns history-tracker-analytics.core
  (:require [clojure.contrib.sql :as sql]
            [clojureql.core :as ql]
            [clojure.java.io :as io]
            [clj-time.local :as time]))

;;Reading MySQL settings
(def mysql-settings {:classname "com.mysql.jdbc.Driver" :subprotocol "mysql"
                     :fetch-size 1000 :auto-commit true :use-unicode true
                     :character-encoding "UTF-8"})

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

(def print-freq 10000)
(defn conjoin [coll entry]
  (let [id (entry :id)
        key (select-keys entry [:type :user_space_id])
        size (count-entry-size entry)
        update #(+ size (if (nil? %) 0 %))]
    (do (if (= (mod id print-freq) 0) (println id))
        (update-in coll [key] update))))

;;Database functions
(def sql-query "SELECT id, type, user_space_id, state, context from history limit")


(defn iterate-history-entries-with [db]
  (do (Class/forName (db :classname))
      (let [conn (java.sql.DriverManager/getConnection
                                 (str "jdbc:" (db :subprotocol) ":" (db :subname))
                                 (db :user) (db :password))
            stmt (.createStatement conn)]
        (do (.setFetchDirection stmt java.sql.ResultSet/FETCH_FORWARD)
            (.setFetchSize stmt Integer/MIN_VALUE)
            (let [result-set (.executeQuery stmt sql-query)
                  empty-coll (hash-map)]
              (loop [result result-set coll empty-coll]
                (if (.next result)
                  (let [result-hash {:id (.getInt result "id")
                                     :type (.getString result "type")
                                     :user_space_id (.getInt result "user_space_id")
                                     :context (.getString result "context")
                                     :state (.getString result "state")}]
                    (recur result (conjoin coll result-hash)))
                  coll)))))))

;;Printing - not that i'll use these dummy wrappy functions, just not to foget how to print/read
(defn read-from-file [file] (load-file file))

;(use '(incanter core charts stats))
;(defn -main [& args]
;  (view (histogram (sample-normal 1000))))

(defn -main [file & other]
  (let [db (init-db (slurp file))
        start-time (time/local-now)
        big-data (iterate-history-entries-with db)
        end-time (time/local-now)]
    (do (println start-time)
        (println "MySQL connection parameters: " db)
        (spit "stats.raw" big-data)
        (println end-time))))