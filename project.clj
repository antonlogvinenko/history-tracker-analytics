(defproject history-tracker-analytics "1.0.0-SNAPSHOT"
  :description "FIXME: write description"
  :dependencies [
                 [lein-swank "1.4.4"]
                 [org.clojure/clojure "1.2.1"]
                 [org.clojure/data.json "0.1.2"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [mysql/mysql-connector-java "5.1.6"]
                 [clj-time "0.4.2"]
                 [incanter "1.3.0"]
                 [clojureql "1.0.3"]
                 [clj-stacktrace "0.2.4"]
                 ]
  :source-paths ["src/"]
  :jvm-opts ["-Xms1024M" "-Xmx2048M" "-Xmn512M" "-server"]
  :main history-tracker-analytics.convert)