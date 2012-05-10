(defproject storm-mongo "0.0.1-SNAPSHOT"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :jvm-opts ["-Djava.library.p  ath=/usr/local/lib:/opt/local/lib"]
  :aot :all
  :test-selectors {:integration :integration
                   :default (complement :integration)
                   :random (fn [_] (> (rand) ~(float 1/2)))
                   :all (constantly true)}
  :dependencies [[redis.clients/jedis "2.0.0"]]
  :dev-dependencies [[storm "0.7.2-rc1"] [clojure "1.4.0"] [clojure-contrib "1.2.0"] [junit "4.10"]])

