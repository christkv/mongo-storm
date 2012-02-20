(defproject storm-redis-pubsub "0.0.1-SNAPSHOT"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib"]
  :aot :all
  :dependencies [[redis.clients/jedis "2.0.0"] [org.mongodb/mongo-java-driver "2.7.3"]]
  :dev-dependencies [[storm "0.6.2"]])

