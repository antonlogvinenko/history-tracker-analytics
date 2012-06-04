(defproject history-tracker-analytics "1.0.0-SNAPSHOT"
  :description "FIXME: write description"
  :dependencies [
                 [org.clojure/clojure "1.2.1"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [mysql/mysql-connector-java "5.1.6"]
                 [clj-time "0.4.2"]
                 [incanter "1.3.0"]
                 [clojureql "1.0.3"]
                 ]
  :source-paths ["src/"]
  :jvm-opts ["-Xms1024M" "-Xmx2048M" "-Xmn1536M" "-server"]
  :main history-tracker-analytics.core)