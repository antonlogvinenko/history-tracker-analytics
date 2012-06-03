(ns history-tracker-analytics.core
  (:require [clojure.contrib.sql :as sql]
            [clojure.java.io :as io]
            [clj-time.local :as time]))

(defn add-row-to-pair [parameters row]
  (let [pair (.split row "=")
        key (keyword (first pair))
        value (second pair)]
    (assoc parameters key value)))

(defn init-parameters-from [settings]
  (let [mysql-file (slurp (nth settings 0))
        mysql-strings (.split mysql-file "\n")]
    {:mysql-settings (reduce add-row-to-pair
                             {:classname "com.mysql.jdbc.Driver" :subprotocol "mysql"}
                             mysql-strings)}))

(def sql-query "SELECT id, type, user_space_id, state, context from history limit 1000")

(defn conjoin [coll {context :context state :state :as entry}]
  (let [key (select-keys entry [:type :user_space_id])
        size (+ (count context) (count state))]
    (if (contains? coll key)
      (update-in coll [key] + size)
      (assoc coll key size))))

(defn dump-to-file [big-data]
  (println big-data))

(defn iterate-history-entries-with [db]
  (sql/with-connection db
    (sql/with-query-results results [sql-query]
      {:fetch-size 1000}
      (reduce conjoin (hash-map) results))))

(defn -main [& args]
  (let [parameters (init-parameters-from args)
        db (parameters :mysql-settings)
        big-data (iterate-history-entries-with db)]
    (do (println (time/local-now))
        (println "MySQL connection parameters: " db)
        (dump-to-file big-data)
        (println (time/local-now)))))

                                        ;3 what to measure?
                                        ;4 write to file
                                        ;build distributions

