(defproject cascalog-logs-analysis-example "0.1.0-SNAPSHOT"
  :description "Simple example of using Cascalog to parse logs"
  :repositories {"conjars" "http://conjars.org/repo"}
  :profiles { :provided {:dependencies [[org.apache.hadoop/hadoop-core "1.2.1"]]}}
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[cascalog "2.1.0"]
                 [org.clojure/clojure "1.5.1"]]
  :jvm-opts ["-Xms768m" "-Xmx768m"]
  :main cascalog-logs-analysis-example.core)
