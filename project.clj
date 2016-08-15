(defproject storm-mongo "0.0.2-SNAPSHOT"
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :test-paths ["test/jvm"]

  :aot :all

  :dependencies [[org.mongodb/mongo-java-driver "2.11.2"]]

  :profiles {:dev
              {:dependencies [
                              [org.apache.storm/storm-core "1.0.0"]
                              [org.clojure/clojure "1.4.0"]
                              [junit "4.10"]]}}

  :min-lein-version "2.0.0"
  )
