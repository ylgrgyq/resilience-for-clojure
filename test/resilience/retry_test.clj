(ns resilience.retry-test
  (:refer-clojure :exclude [name])
  (:require [clojure.test :refer :all]
            [resilience.retry :refer :all]
            [resilience.core :as resilience])
  (:import (clojure.lang ExceptionInfo)
           (java.util.concurrent TimeUnit)))

(defn- fail [& _]
  (throw (ex-info "expected exception" {:expected true})))

(deftest test-retry
  (let [retry-config {:max-attempts 5
                      :wait-millis  200}
        testing-retry (retry "testing-retry" retry-config)]
    (testing "retry"
      (let [retry-times (volatile! 0)
            start (System/nanoTime)]
        (try
          (resilience/execute-with-retry testing-retry
                                         (let [times (vswap! retry-times inc)
                                               diff (.toMillis TimeUnit/NANOSECONDS (- (System/nanoTime) start))]
                                           (is (>= diff (* (dec times) (:wait-millis retry-config))))
                                           (fail)))
          (catch ExceptionInfo ex
            (is (:expected (ex-data ex)))
            (is (= @retry-times (:max-attempts retry-config)))))))))