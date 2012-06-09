(ns history-tracker-analytics.interpret
  (:use [clojure.contrib.string :as string :only (chop)]))

(defn add-time [statistics capacity-to-time] (map capacity-to-time statistics))

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
         :else (+ y1 (* (- x x1) (/ (- y2 y1) (- x2 x1)))))))))

(defn load-approximations [table-function-file-name]
  (let [data (load-file table-function-file-name)
        reversed-data (map #(vec (reverse %)) data)]
    {:capacity-to-time (create-linear-interpolation data)
     :time-to-capacity (create-linear-interpolation reversed-data)}))

(defn by-percentile [statistics percentile table-function-file-name]
  (let [{capacity-to-time :capacity-to-time} (load-approximations table-function-file-name)
        length (count statistics)
        percent (/ percentile 100)
        index (int (* length percent))
        maximum-capacity (nth statistics index)
        maximum-time (capacity-to-time maximum-capacity)]
    {:maximum-capacity maximum-capacity
     :maximum-time (double maximum-time)}))

(defn by-value [statistics value]
  (let [amount (count statistics)
        less-than-or-equal-list (first (split-with #(<= % value) statistics))
        less-than-count (count less-than-or-equal-list)]
    (double (/ less-than-count amount))))

(defn by-capacity [statistics c table-function-file-name]
  (let [{capacity-to-time :capacity-to-time} (load-approximations table-function-file-name)]
    {:related-time (capacity-to-time c)
     :percentile (double (by-value statistics c))}))

(defn by-time [statistics t table-function-file-name]
  (let [{time-to-capacity :time-to-capacity
         capacity-to-time :capacity-to-time} (load-approximations table-function-file-name)]
        {:percentile (double (by-value (add-time statistics)))}))

(use '(incanter core charts stats))

(defn draw [statistics]
  (let [statistics (take 9000000 statistics)]
    (do (view (histogram statistics
                         :theme :dark :x-label)))))


(defn read-statistics [statistics-file-name]
  (let [text (string/chop (slurp statistics-file-name))
        strings (.split text " ")]
    (map #(Integer/parseInt %) strings)))


