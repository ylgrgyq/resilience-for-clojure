(defproject resilience-for-clojure "0.2.8-SNAPSHOT"
  :description "A clojure wrapper over Resilience4j" 
  :url "https://github.com/ylgrgyq/resilience-for-clojure"
  :license {:name "The MIT License (MIT) "
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [io.github.resilience4j/resilience4j-circuitbreaker "1.2.0"]
                 [io.github.resilience4j/resilience4j-ratelimiter "1.2.0"]
                 [io.github.resilience4j/resilience4j-retry "1.2.0"]
                 [io.github.resilience4j/resilience4j-bulkhead "1.2.0"]
                 [io.github.resilience4j/resilience4j-timelimiter "1.2.0"]
                 [io.github.resilience4j/resilience4j-circularbuffer "1.2.0"]]
  :plugins [[lein-codox "0.10.7"]
            [lein-cloverage "1.1.2"]]
  :codox {:output-path "target/codox"
          :source-uri "https://github.com/ylgrgyq/resilience-for-clojure/blob/master/{filepath}#L{line}"
          :metadata {:doc/format :markdown}}
  :deploy-repositories {"releases" :clojars}
  :global-vars {*warn-on-reflection* true
                *assert* false})
