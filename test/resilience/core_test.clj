(ns resilience.core-test
  (:require [clojure.test :refer :all]
            [resilience.breaker :as breaker]
            [resilience.retry :as retry]
            [resilience.bulkhead :as bulkhead]
            [resilience.core :refer :all])
  (:import (io.github.resilience4j.circuitbreaker CircuitBreakerOpenException)
           (clojure.lang ExceptionInfo)))

(defn- fail [& _]
  (throw (ex-info "expected exception" {:expected true})))

(defn- success [& _] true)

(defn- max-failed-times [buffer-size failure-threshold]
  (int (* (/ failure-threshold 100) buffer-size)))

(defn- fill-ring-buffer [breaker buffer-size expect-failure-times]
  (let [fail-times (volatile! 0)]
    (doseq [_ (range buffer-size)]
      (let [execute-fn (if (< @fail-times expect-failure-times) fail success)]
        (execute-with-breaker breaker (execute-fn))))))

(deftest test-execute
  (let [breaker-basic-config {:failure-rate-threshold                     50
                              :ring-buffer-size-in-closed-state           30
                              :ring-buffer-size-in-half-open-state        20
                              :wait-millis-in-open-state                  1000
                              :automatic-transfer-from-open-to-half-open? true}
        testing-breaker (breaker/circuit-breaker "testing-breaker" breaker-basic-config)
        retry-config {:max-attempts 5
                      :wait-millis  200}
        testing-retry (retry/retry "testing-retry" retry-config)]
    (testing "retry with breaker"
      (let [retry-times (volatile! 0)
            max-failed-allowed (max-failed-times (:ring-buffer-size-in-closed-state breaker-basic-config)
                                                 (:failure-rate-threshold breaker-basic-config))
            testing-fn (to-fn (do (vswap! retry-times inc) (fail)))]
        (fill-ring-buffer testing-breaker (:ring-buffer-size-in-closed-state breaker-basic-config) 0)
        (doseq [_ (range max-failed-allowed)]
          (is (= (execute
                   (testing-fn)
                   (with-retry testing-retry)
                   (with-breaker testing-breaker)
                   (recover-from ExceptionInfo (fn [_] :expected-exception)))
                 :expected-exception)))
        (is (= (execute
                 (testing-fn)
                 (with-retry testing-retry)
                 (with-breaker testing-breaker)
                 (recover-from CircuitBreakerOpenException
                               (fn [_] :breaker-open))
                 (recover-from ExceptionInfo (fn [_] :expected-exception)))
               :breaker-open))

        (is (= (* (:max-attempts retry-config)
                  max-failed-allowed)
               @retry-times))))))

(deftest test-with-resilience-family
  (let [breaker-basic-config {:failure-rate-threshold                     50
                              :ring-buffer-size-in-closed-state           30
                              :ring-buffer-size-in-half-open-state        20
                              :wait-millis-in-open-state                  1000
                              :automatic-transfer-from-open-to-half-open? true}
        testing-breaker (breaker/circuit-breaker "testing-breaker" breaker-basic-config)
        bulkhead-config {:max-concurrent-calls 5
                         :wait-millis          200}
        testing-bulkhead (bulkhead/bulkhead "testing-bulkhead" bulkhead-config)
        retry-config {:max-attempts 5
                      :wait-millis  200}
        testing-retry (retry/retry "testing-retry" retry-config)]
    (testing "retry with breaker"
      (let [retry-times (volatile! 0)
            max-failed-allowed (max-failed-times (:ring-buffer-size-in-closed-state breaker-basic-config)
                                                 (:failure-rate-threshold breaker-basic-config))
            testing-fn (to-fn (do (vswap! retry-times inc) (fail)))]
        (fill-ring-buffer testing-breaker (:ring-buffer-size-in-closed-state breaker-basic-config) 0)

        (doseq [_ (range max-failed-allowed)]
          (is (= (try
                   (with-resilience-family
                     [:retry testing-retry :breaker testing-breaker :bulkhead testing-bulkhead]
                     (testing-fn))
                   (catch ExceptionInfo _
                     :expected-exception))
                 :expected-exception)))
        (is (= (try
                 (with-resilience-family
                   [:retry testing-retry :breaker testing-breaker :bulkhead testing-bulkhead]
                   (testing-fn))
                 (catch CircuitBreakerOpenException _
                   :breaker-open))
               :breaker-open))

        (is (= (* (:max-attempts retry-config)
                  max-failed-allowed)
               @retry-times))))))
