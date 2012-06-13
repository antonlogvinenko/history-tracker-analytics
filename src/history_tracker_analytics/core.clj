(ns history-tracker-analytics.core
  (:use [history-tracker-analytics.collect :only (collect-stats)]
        [history-tracker-analytics.generate-table-function :only (generate-table-function)]
        [history-tracker-analytics.interpret :only
         (by-percentile by-capacity by-time draw read-statistics)])
  (:require [clojure.contrib.sql :as sql]
            [clojure.java.io :as io]
            [clj-time.local :as time]
            [clojure.contrib.generic.math-functions :as math]
            [clojure.contrib.string :as string]))

(def statistics-file-name "statistics")
(def table-function-file-name "table.f")

(defn info []
  (do
    (println "Usage:")
    (println "1. collect <ini-file> - creates 'statistics' file")
    (println "2. draw - draws statistics")
    (println "3. percentile <value>")
    (println "4. capacity <value>")
    (println "5. time <value>")))

(defn -main [& other]
  (if (= 0 (count other))
    (info)
    (let [mode (first other)
          args (rest other)]
      (cond (= mode "collect")
            (collect-stats statistics-file-name args)
            (= mode "generate-table-function")
            (generate-table-function table-function-file-name 10000 200)
            (= mode "draw")
            (draw (read-statistics statistics-file-name))
            :else (let [statistics (read-statistics statistics-file-name)
                        argument (Double/parseDouble (first args))]
                    (cond (= mode "percentile")
                          (println (by-percentile statistics argument table-function-file-name))
                          (= mode "capacity")
                          (println (by-capacity statistics argument table-function-file-name))
                          (= mode "time")
                          (println (by-time statistics argument table-function-file-name))
                          :else (info)))))))