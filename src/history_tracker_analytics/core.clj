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

;;buggy mysql jdbc driver here, rewrote standard clojure wrapper library
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

(defn by-percentile [statistics percentile]
  (let [length (count statistics)
        percent (/ percentile 100)
        index (int (* length percent))
        maximum-capacity (nth statistics index)
        maximum-time (capacity-to-time maximum-capacity)]
    {:maximum-capacity maximum-capacity
     :maximum-time maximum-time}))

(defn by-value [statistics value]
  (let [amount (count statistics)
        less-than-or-equal-list (first (split-with #(<= % value) statistics))
        less-than-count (count less-than-or-equal-list)]
    (double (/ less-than-count amount))))

(defn by-capacity [statistics c]
  {:related-time (capacity-to-time c)
   :percentile (double (by-value statistics c))})

(defn by-time [statistics t]
  {:percentile (double (by-value (add-time statistics)))})

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

;;http://clj-me.blogspot.com/2009/06/linear-interpolation-and-sorted-map.html
;;thanks to Christofer Grand
(defn create-linear-interpolation
  "Takes a coll of 2D points (vectors) and returns
  their linear interpolation function."
  [points]
  (let [m (into (sorted-map) points)]
    (fn [x]
      (let [[[x1 y1]] (rsubseq m <= x)
            [[x2 y2]] (subseq m > x)]
        (cond
         (not x2) y1
         (not x1) y2
         :else (+ y1 (* (- x x1) (/ (- y2 y1) (- x2 x1))))))))

(defn -main [& other]
  (if (= 0 (count other))
    (info)
    (let [mode (first other)
          args (rest other)]
      (cond (= mode "collect") (collect-stats args)
            :else (let [statistics (read-statistics statistics-file-name)
                        argument (Double/parseDouble (first args))]
                    (cond (= mode "draw") (draw statistics)
                          (= mode "percentile") (println (by-percentile statistics argument))
                          (= mode "capacity") (println (by-capacity statistics argument))
                          (= mode "time") (println (by-time statistics argument))
                          :else (info)))))))