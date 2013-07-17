(defproject storm-mongo "0.0.1-SNAPSHOT"
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :test-paths ["test/jvm"]
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib"]
  :aot :all

  :dependencies [[org.mongodb/mongo-java-driver "2.11.2"]]

  :profiles {:dev
              {:dependencies [[storm "0.8.2"]
                              [org.clojure/clojure "1.4.0"]
                              [junit "4.10"]]}}

  :min-lein-version "2.0.0"
  )
