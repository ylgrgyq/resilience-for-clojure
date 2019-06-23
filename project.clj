(defproject resilience-for-clojure "0.2.6"
  :description "A clojure wrapper over Resilience4j" 
  :url "https://github.com/ylgrgyq/resilience-for-clojure"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [io.github.resilience4j/resilience4j-circuitbreaker "0.16.0"]
                 [io.github.resilience4j/resilience4j-ratelimiter "0.16.0"]
                 [io.github.resilience4j/resilience4j-retry "0.16.0"]
                 [io.github.resilience4j/resilience4j-bulkhead "0.16.0"]
                 [io.github.resilience4j/resilience4j-timelimiter "0.16.0"]
                 [io.github.resilience4j/resilience4j-circularbuffer "0.16.0"]]
  :plugins [[lein-codox "0.9.5"]]
  :codox {:output-path "target/codox"
          :source-uri "https://github.com/ylgrgyq/resilience-for-clojure/blob/master/{filepath}#L{line}"
          :metadata {:doc/format :markdown}}
  :deploy-repositories {"releases" :clojars}
  :global-vars {*warn-on-reflection* true
                *assert* false})
