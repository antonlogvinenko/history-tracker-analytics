(ns history-tracker-analytics.core
  (:require [clojure.contrib.sql :as sql]
            [clojure.java.io :as io]
            [clj-time.local :as time]
            [clojure.contrib.generic.math-functions :as math]
            [clojure.contrib.string :as string]))

;;Reading MySQL settings
(def mysql-settings {:classname "com.mysql.jdbc.Driver" :subprotocol "mysql"})

(defn add-row [parameters row]
  (let [pair (.split row "=")]
    (assoc parameters (keyword (first pair)) (second pair))))

(defn init-db [file]
  (reduce add-row mysql-settings (.split file "\n")))



;;Counting size
(def known-entry-size-bytes 17)
(def statistics-file-name "statistics")
(def distribution-file-name "distribution")
(def print-freq 100000)

(defn count-entry-size [{context :context state :state}]
  (+ (* 2 (count context))
     (* 2 (count state))
     known-entry-size-bytes))

(defn conjoin [coll entry]
  (let [id (entry :id)
        key (.hashCode (select-keys entry [:type :user_space_id]))
        size (count-entry-size entry)
        update #(+ size (if (nil? %) 0 %))]
    (do (if (= (mod id print-freq) 0) (println id (count coll) (time/local-now)))
        (update-in coll [key] update))))



;;Database functions
(def sql-query "SELECT id, type, user_space_id, state, context from history")

;;buggy mysql jdbc driver here, rewrote standard clojure
(defn iterate-history-entries-with [db]
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

(defn collect-stats [[file & other]]
  (let [db (init-db (slurp file))
        start-time (time/local-now)
        big-data (iterate-history-entries-with db)
        end-time (time/local-now)]
    (do (println start-time)
        (println "MySQL connection parameters: " db)
        (spit statistics-file-name (vals big-data))
        (println end-time))))


(defn capacity-to-time [capacity] (* 21 capacity))
(defn time-to-capacity [time] (/ time 21))
(defn add-time [statistics] (map capacity-to-time statistics))

(defn by-percentile [statistics [percentile & other]]
  (let [length (count statistics)
        percent (/ percentile 100)
        maximum-capacity (math/floor (* length percent))
        maximum-time (add-time maximum-capacity)]
    {:maximum-capacity maximum-capacity
     :maximum-time maximum-time}))

(defn by-value [statistics value]
  (let [amount (count statistics)
        less-than-value (last (first (split-with #(<= % value))))]
    (/ less-than-value amount)))

(defn by-capacity [statistics [c & other]]
  {:related-time (capacity-to-time c)
   :percentile (by-value statistics c)})

(defn by-time [statistics [t & other]]
  {:related-capacity (time-to-capacity t)
   :percentile (by-value (add-time statistics))})

(use '(incanter core charts stats))

(defn draw [statistics]
  (let [statistics (take 9000000 statistics)]
    (do (view (histogram statistics
                         :theme :dark :x-label)))))
(defn info []
  (do
    (println "Usage:")
    (println "1. collect <ini-file> - creates 'statistics' file")
    (println "2. draw - draws statistics")
    (println "3. percentile <value>")
    (println "4. capacity <value>")
    (println "5. time <value>")))


(defn read-statistics [statistics-file-name]
  (let [text (string/chop (slurp statistics-file-name))
        strings (.split text " ")]
    (map #(Integer/parseInt %) strings)))


(defn -main [& other]
  (if (= 0 (count other))
    (info)
    (let [mode (first other)
          args (rest other)]
      (cond (= mode "collect") (collect-stats args)
            :else (let [statistics (read-statistics statistics-file-name)]
                    (cond (= mode "draw") (draw statistics)
                          (= mode "percentile") (println (by-percentile statistics other))
                          (= mode "capacity") (println (by-capacity statistics other))
                          (= mode "time") (println (by-time statistics other))
                          :else (info)))))))