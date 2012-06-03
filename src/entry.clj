(ns history-tracker-analytics.entry
  (:require clj-record.boot))

(defn cake [] (println "cake"))

(def db
  {:classname "com.mysql.jdbc.Driver"
   :subprotocol "mysql"
   :user "root"
   :password "password"
   :subname "//127.0.0.1/bul_history"
   })

(clj-record.core/init-model
 :table-name "history")

