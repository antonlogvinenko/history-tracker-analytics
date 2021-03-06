(ns history-tracker-analytics.test.core
  (:use [history-tracker-analytics.core])
  (:use [clojure.test]))

;;clojure test-is: is, are, testing, stubbing

(def test run-tests)

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
    (let [entry {:id 8 :type "bulletin" :user_space_id 11 :context "cake" :state "is a lie"}]
      (are [coll entry new-coll] (= new-coll (conjoin coll entry))
           {} entry
           {(.hashCode {:type "bulletin" :user_space_id 11}) (+ 24 known-entry-size-bytes)}

           {(.hashCode {:type "bulletin" :user_space_id 11}) 7} entry
           {(.hashCode {:type "bulletin" :user_space_id 11}) (+ 31 known-entry-size-bytes)}
           ))))

(deftest by-percentile-test
  (are [stats p resulting] (= resulting (by-percentile stats p))
       (take 100 (iterate #(+ % 2) 1))
       99
       {:maximum-capacity 199
        :maximum-time (capacity-to-time 199)}))

(deftest by-value-test
  (are [stats v resulting] (= resulting (by-value stats v))
       (take 100 (iterate #(+ % 2) 1))
       78
       39/100))


;;Database functions - moved out everything possible, still dunno how to test it
;;will require mocking and stubbing
(deftest iterate-history-entries-with-test)

;;Printing test
(deftest dump-to-file-test)