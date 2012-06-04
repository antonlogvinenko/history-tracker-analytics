(ns history-tracker-analytics.core
  (:require [clojure.contrib.sql :as sql]
            [clojureql.core :as ql]
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

(def print-freq 100000)
(defn conjoin [coll entry]
  (let [id (entry :id)
        key (.hashCode (select-keys entry [:type :user_space_id]))
        size (count-entry-size entry)
        update #(+ size (if (nil? %) 0 %))]
    (do (if (= (mod id print-freq) 0) (println id (count coll) (time/local-now)))
        (update-in coll [key] update))))

;;Database functions
(def sql-query "SELECT id, type, user_space_id, state, context from history")


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

(def statistics-file-name "statistics")
(def distribution-file-name "distribution")

(defn collect-stats [[file & other]]
  (let [db (init-db (slurp file))
        start-time (time/local-now)
        big-data (iterate-history-entries-with db)
        end-time (time/local-now)]
    (do (println start-time)
        (println "MySQL connection parameters: " db)
        (spit statistics-file-name (vals big-data))
        (println end-time))))

(defn get-distribution-reduce [step]
  (fn [distribution item]
    (let [group (quot item step)
          group-amount (get distribution group 0)]
      (assoc distribution group (inc group-amount)))))

(defn get-distribution [statistics step]
  (let [reduce-f (get-distribution-reduce step)]
        (reduce reduce-f {} statistics)))

(defn build [[step & other]]
  (let [statistics (load-file statistics-file-name)
        distribution (get-distribution statistics step)]
    (spit (str distribution-file-name "-" step ) distribution)))

(defn add-time [x] x)
(use '(incanter core charts stats))
(defn draw [[file & other]]
  (let [distribution (load-file file)
        step (Integer/parseInt (second (.split file "-")))
        distribution-time (add-time distribution)]
    (do (view (histogram (sample-normal 1000)))
        (view (histogram (sample-normal 1000))))))

(defn info []
  (do
    (println "Usage:")
    (println "1. collect <ini-file> - creates 'statistics' file")
    (println "2. build <distr> - creates distribution files")
    (println "3. draw <distr> - draws time and capacity distribution files")))

(defn -main [& other]
      (do (view (histogram (sample-normal 1000)))
        (view (histogram (sample-normal 1000)))))

(defn main [& other]
  (if (= 0 (count other))
    (info)
    (let [mode (first other)
          args (rest other)]
      (cond (= mode "collect") (collect-stats args)
            (= mode "build") (build args)
            (= mode "draw") (draw args)
            :else (info)))))