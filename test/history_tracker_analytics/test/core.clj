(ns history-tracker-analytics.test.core
  (:use [history-tracker-analytics.core])
  (:use [clojure.test]))

;;clojure test-is: is, are, testing, stubbing

;;Reading settings
(deftest add-row-test
  (are [old-pair row new-pair] (= new-pair (add-row old-pair row))
       {} "ab" {:ab nil}
       {} "b=a" {:b "a"}
       {:c "d"} "a=b" {:c "d" :a "b"}
       {:c "d"} "asdas" {:c "d" :asdas nil}
       {:r "7"} "r=9" {:r "9"}))

(deftest init-db-test
  (are [file values] (= (merge mysql-settings values) (init-db file))
       "user=bul_history" {:user "bul_history"}
       "user=bul_history\npassword=yo" {:user "bul_history" :password "yo"}))

;;Counting size tests
(deftest count-entry-size-test
  (is (=
       (count-entry-size {:context "cake" :state "is a lie"})
       (+ known-entry-size-bytes (* 2 12)))))

(deftest conjoin-test
  (testing "Conjoin adds new bulletin entry size correctly for both existing and new bulletins"
    (let [entry {:type "bulletin" :user_space_id 11 :context "cake" :state "is a lie"}]
      (are [coll entry new-coll] (= new-coll (conjoin coll entry))
           {} entry
           {{:type "bulletin" :user_space_id 11} 34}

           {{:type "bulletin" :user_space_id 11} 7} entry
           {{:type "bulletin" :user_space_id 11} 41}
           ))))

(deftest reduce-sql-results-test
  (let [sql-row-1 {:type "bulletin" :user_space_id 1 :context "abc" :state "def"}
        sql-row-2 {:type "bulletin" :user_space_id 1 :context "abc" :state "defgh"}
        sql-row-3 {:type "auction" :user_space_id 1 :context "abcdefgh" :state "ijklmno"}
        results [sql-row-1 sql-row-2 sql-row-3]
        reduced-value {{:type "bulletin" :user_space_id 1} 48
                       {:type "auction" :user_space_id 1} 40}]
       (is (= reduced-value (reduce-sql-results results)))))

;;Database functions - moved out everything possible, still dunno how to test it
;;will require mocking and stubbing
(deftest iterate-history-entries-with-test)

;;Printing test
(deftest dump-to-file-test)