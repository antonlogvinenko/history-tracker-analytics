(ns history-tracker-analytics.generate-table-function
  (:require [clojure.contrib.string :as string])
  (:import [java.io StringReader]
           [javax.xml.parsers DocumentBuilderFactory]
           [org.xml.sax InputSource]
           [javax.xml.transform.dom DOMResult]))

(defn get-root [text]
  (let [source (InputSource. (StringReader. text))]
    (.. DocumentBuilderFactory
        newInstance newDocumentBuilder
        (parse source) getDocumentElement)))
(defn get-object [element]
  (let [object (.. element (getElementsByTagName "object") (item 0))
        id (. object getAttribute "id")
        type (. object getAttribute "type")]
    {:id id :type type}))
(defn get-revision [element]
  (. element getAttribute "revision"))
(defn get-state-time [element]
  (. element getAttribute "time"))
(defn get-id [element]
  (. element getAttribute "id"))
(defn get-attribute [element]
  (let [name (. element getAttribute "name")
        type (. element getAttribute "type")
        value (. element getAttribute "value")]
    {:name name :type type :value value}))
(defn get-attributes [element name]
  (let [attributes (.. element (getElementsByTagName name) (item 0)
                       (getElementsByTagName "attribute"))
        length (. attributes getLength)]
    (map #(get-attribute (.. attributes (item %))) (range 0 length))))
(defn get-object-states [root]
  (.. root (getElementsByTagName "objectState")))

(defn get-state [element]
  (get-attributes element "state"))
(defn get-context [element]
  (get-attributes element "context"))

(defn get-object-state [object-state]
  (let [object (get-object object-state)
        revision (get-revision object-state)
        state-time (get-state-time object-state)
        id (get-id object-state)
        state (get-state object-state)
        context (get-context object-state)]
    {:object object
     :revision revision
     :state-time state-time
     :id id
     :state state
     :context context}))

(defn parse-xml [xml]
  (let [root (get-root xml)
        object-states (get-object-states root)
        length (. object-states getLength)]
    (do (map
     #(get-object-state (. object-states (item %)))
     (range 0 length)))))

(defn generate-xml-part [n text]
  (.replaceAll text "XYZ" (str n)))

(defn generate-xml-string [size]
  (let [text (slurp "xml-sample.xml")
        parts (map #(generate-xml-part % text) (range 0 size))
        content (string/join "\n" parts)]
    (str "<bulletins>" content "</bulletins>")))

(defn measure [sample]
  (let [time-0 (. System currentTimeMillis)]
    (do (->> sample parse-xml vec (spit "very-temp.txt"))
        (- (. System currentTimeMillis) time-0))))

(defn capacity-to-time-avg [length]
  (let [sample (generate-xml-string length)
        repeat-count 5
        generating-time (loop [round repeat-count time 0]
                          (if (zero? round) time
                              (-> sample measure (+ time) (recur (dec round)))))
        bytes (-> sample count (* 2))]
    (do (println "generated length" length)
        [bytes (->> repeat-count (/ generating-time) double)])))

(defn generate-table-function [file-name max-size step]
  (let [versions (range 1 max-size step)
        capacity-to-time (map capacity-to-time-avg versions)]
    (spit file-name (vec capacity-to-time))))
