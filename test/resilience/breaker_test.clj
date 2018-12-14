(ns resilience.breaker-test
  (:refer-clojure :exclude [name reset!])
  (:require [clojure.test :refer :all]
            [resilience.breaker :refer :all]
            [resilience.core :as resilience])
  (:import (io.github.resilience4j.circuitbreaker CircuitBreakerOpenException)
           (clojure.lang ExceptionInfo)))

(defn- fail [& _]
  (throw (ex-info "expected exception" {:expected true})))

(defn- success [& _] true)

(defn- fill-ring-buffer [breaker buffer-size expect-failure-times]
  (let [fail-times (volatile! 0)]
    (doseq [_ (range buffer-size)]
      (let [execute-fn (if (< @fail-times expect-failure-times) fail success)]
        (resilience/execute-with-breaker breaker (execute-fn))))))

(defn- max-failed-times [buffer-size failure-threshold]
  (int (* (/ failure-threshold 100) buffer-size)))

(defn- let-breaker-open [breaker fail-times]
  ;; let circuit open
  (doseq [_ (range fail-times)]
    (try
      (resilience/execute-with-breaker breaker (fail))
      (catch ExceptionInfo ex
        (is (:expected (ex-data ex))))))
  (is (= :OPEN (state breaker)))
  (is (thrown? CircuitBreakerOpenException (resilience/execute-with-breaker breaker (fail)))))

(deftest test-breaker
  (let [breaker-basic-config {:failure-rate-threshold                     50.0
                              :ring-buffer-size-in-closed-state           30
                              :ring-buffer-size-in-half-open-state        20
                              :wait-millis-in-open-state                  1000
                              :automatic-transfer-from-open-to-half-open? true}]
    (defbreaker testing-breaker breaker-basic-config)
    (testing "breaker from CLOSED to OPEN"
      ;; fill the ring buffer for closed state
      (is (= :CLOSED (state testing-breaker)))
      (fill-ring-buffer testing-breaker (:ring-buffer-size-in-closed-state breaker-basic-config) 0)

      ;; let circuit open
      (let-breaker-open testing-breaker
                        (max-failed-times (:ring-buffer-size-in-closed-state breaker-basic-config)
                                          (:failure-rate-threshold breaker-basic-config))))

    (testing "breaker from HALF_OPEN to OPEN"
      ;; wait circuit transfer to half open
      (Thread/sleep (:wait-millis-in-open-state breaker-basic-config))
      ;; will keep in OPEN state if we set :automatic-transfer-from-open-to-half-open? to false
      (is (= :HALF_OPEN (state testing-breaker)))

      ;; open circuit again all request will be allowed during half open state until ring buffer is full
      (let-breaker-open testing-breaker (:ring-buffer-size-in-half-open-state breaker-basic-config)))

    (testing "breaker from HALF_OPEN to CLOSED"
      ;; wait circuit transfer to half open
      (Thread/sleep (:wait-millis-in-open-state breaker-basic-config))
      ;; will keep in OPEN state if we set :automatic-transfer-from-open-to-half-open? to false
      (is (= :HALF_OPEN (state testing-breaker)))

      ;; open circuit again all request will be allowed during half open state until ring buffer is full
      (doseq [_ (range (:ring-buffer-size-in-half-open-state breaker-basic-config))]
        (resilience/execute-with-breaker testing-breaker (success)))
      (is (= :CLOSED (state testing-breaker)))
      (reset! testing-breaker))

    (testing "force transition state"
      (is (= :CLOSED (state testing-breaker)))
      (transition-to-disabled-state! testing-breaker)
      (is (= :DISABLED (state testing-breaker)))
      (transition-to-forced-open-state! testing-breaker)
      (is (= :FORCED_OPEN (state testing-breaker)))
      (transition-to-half-open! testing-breaker)
      (is (= :HALF_OPEN (state testing-breaker)))
      (transition-to-closed-state! testing-breaker)
      (is (= :CLOSED (state testing-breaker)))
      (transition-to-open-state! testing-breaker)
      (is (= :OPEN (state testing-breaker))))))

