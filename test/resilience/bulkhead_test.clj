(ns resilience.bulkhead-test
  (:refer-clojure :exclude [name])
  (:require [clojure.test :refer :all]
            [resilience.bulkhead :refer :all]
            [resilience.core :as resilience])
  (:import (java.util.concurrent CountDownLatch)
           (io.github.resilience4j.bulkhead BulkheadFullException)))

(deftest test-bulkhead
  (let [bulkhead-config {:max-concurrent-calls 5
                         :wait-millis          200}
        testing-bulkhead (bulkhead "testing-bulkhead" bulkhead-config)]
    (testing "bulkhead wait timeout"
      (let [latch (CountDownLatch. (:max-concurrent-calls bulkhead-config))
            done (CountDownLatch. 1)]
        (doseq [_ (range (:max-concurrent-calls bulkhead-config))]
          (future
            (try
              (resilience/execute-with-bulkhead testing-bulkhead
                                                (.countDown latch)
                                                (Thread/sleep (* 2 (:wait-millis bulkhead-config)))
                                                (.await done))
              (catch Exception ex
                (is false)))))
        (.await latch)
        (let [start (System/nanoTime)]
          (is (thrown? BulkheadFullException
                       (resilience/execute-with-bulkhead testing-bulkhead
                                                         (throw (IllegalStateException. "Can't be here")))))
          (is (>= (- (System/nanoTime) start) (:wait-millis bulkhead-config))))
        (.countDown done)))))
