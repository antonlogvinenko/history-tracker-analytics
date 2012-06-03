(ns history-tracker-analytics.test.core
  (:use [history-tracker-analytics.core])
  (:use [clojure.test]))

;;clojure test-is: is, are, testing

;;Reading settings
(defn add-row-to-pair-test)

(defn init-parameters-from-test)


;;Counting size tests
(deftest count-entry-size-test
  (is (=
       (count-entry-size {:context "cake" :state "is a lie"})
       (+ known-entry-size-bytes (* 2 12)))))

(deftest count-bytes-test
  (is (=
       (count-bytes "cake")
       (* 2 4))))

(deftest conjoin-test)

;;Database functions
(defn iterate-history-entries-with-test)

;;Printing test
(defn dump-to-file-test)