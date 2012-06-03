(ns history-tracker-analytics.test.core
  (:use [history-tracker-analytics.core])
  (:use [clojure.test]))

;;clojure test-is: is, are, testing, stubbing

;;Reading settings
(deftest add-row-to-pair-test
  (are [old-pair row new-pair] (= new-pair (add-row-to-pair old-pair row))
       {} "ab" {:ab nil}
       {} "b=a" {:b "a"}
       {:c "d"} "a=b" {:c "d" :a "b"}
       {:c "d"} "asdas" {:c "d" :asdas nil}
       {:r "7"} "r=9" {:r "9"}))

(defn init-parameters-from-test)


;;Counting size tests
(deftest count-entry-size-test
  (is (=
       (count-entry-size {:context "cake" :state "is a lie"})
       (+ known-entry-size-bytes (* 2 12)))))

(deftest conjoin-test)

;;Database functions
(defn iterate-history-entries-with-test)

;;Printing test
(defn dump-to-file-test)