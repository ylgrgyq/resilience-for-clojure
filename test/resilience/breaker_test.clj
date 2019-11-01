(ns resilience.breaker-test
  (:refer-clojure :exclude [name reset!])
  (:require [clojure.test :refer :all]
            [resilience.breaker :refer :all]
            [resilience.core :as resilience])
  (:import (io.github.resilience4j.circuitbreaker CallNotPermittedException)
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
  (is (thrown? CallNotPermittedException (resilience/execute-with-breaker breaker (fail)))))

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
                                          (:failure-rate-threshold breaker-basic-config)))

      (is (= {:failure-rate                    (:failure-rate-threshold breaker-basic-config)
              :number-of-buffered-calls        (:ring-buffer-size-in-closed-state breaker-basic-config)
              :number-of-failed-calls          (/ (:ring-buffer-size-in-closed-state breaker-basic-config) 2)
              :number-of-not-permitted-calls   1
              :number-of-slow-calls            0
              :number-of-slow-failed-calls     0
              :number-of-slow-successful-calls 0
              :number-of-successful-calls      (/ (:ring-buffer-size-in-closed-state breaker-basic-config) 2)
              :slow-call-rate                  0.0}
             (metrics testing-breaker))))

    (testing "breaker from HALF_OPEN to OPEN"
      ;; wait circuit transfer to half open
      (Thread/sleep (+ 500 (:wait-millis-in-open-state breaker-basic-config)))
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
      (is (= :OPEN (state testing-breaker))))

    (testing "consumer"
      (let [on-success-times (atom 0)
            on-success-fn (fn [_] (swap! on-success-times inc))
            on-error-times (atom 0)
            on-error-fn (fn [throwable _]
                          (is (:expected (ex-data throwable)))
                          (swap! on-error-times inc))
            on-state-transition-times (atom 0)
            on-state-transition-fn (fn [from to]
                                     (swap! on-state-transition-times inc)
                                     (is (= :CLOSED from))
                                     (is (= :OPEN to)))
            on-reset-times (atom 0)
            on-reset-fn (fn [] (swap! on-reset-times inc))
            on-ignored-error-times (atom 0)
            on-ignored-error-fn (fn [_ _]
                                  (swap! on-ignored-error-times inc))
            on-call-not-permitted-times (atom 0)
            on-call-not-permitted-fn (fn []
                                       (swap! on-call-not-permitted-times inc))]
        (defbreaker testing-breaker (assoc breaker-basic-config
                                      :ignore-exceptions [IllegalStateException]
                                      :automatic-transfer-from-open-to-half-open? false))

        (set-on-success-event-consumer! testing-breaker on-success-fn)
        (set-on-error-event-consumer! testing-breaker on-error-fn)
        (set-on-state-transition-event-consumer! testing-breaker on-state-transition-fn)
        (set-on-reset-event-consumer! testing-breaker on-reset-fn)
        (set-on-ignored-error-event! testing-breaker on-ignored-error-fn)
        (set-on-call-not-permitted-consumer! testing-breaker on-call-not-permitted-fn)

        (set-on-all-event-consumer! testing-breaker
                                    {:on-success on-success-fn
                                     :on-error on-error-fn
                                     :on-state-transition on-state-transition-fn
                                     :on-reset on-reset-fn
                                     :on-ignored-error on-ignored-error-fn
                                     :on-call-not-permitted on-call-not-permitted-fn})

        (reset! testing-breaker)
        ;; fill the ring buffer for closed state
        (is (= :CLOSED (state testing-breaker)))
        (fill-ring-buffer testing-breaker (:ring-buffer-size-in-closed-state breaker-basic-config) 0)

        (try (resilience/execute-with-breaker testing-breaker (throw (IllegalStateException. "ignored exception")))
             (catch IllegalStateException ex
               (is (some? ex))))

        ;; let circuit open
        (let-breaker-open testing-breaker
                          (max-failed-times (:ring-buffer-size-in-closed-state breaker-basic-config)
                                            (:failure-rate-threshold breaker-basic-config)))

        (is (= @on-success-times 60))
        (is (= @on-error-times 30))
        (is (= @on-state-transition-times 2))
        (is (= @on-reset-times 2))
        (is (= @on-ignored-error-times 2))
        (is (= @on-call-not-permitted-times 2))))))

(deftest test-registry
  (let [breaker-basic-config {:failure-rate-threshold                     50.0
                              :ring-buffer-size-in-closed-state           30
                              :ring-buffer-size-in-half-open-state        20
                              :wait-millis-in-open-state                  1000}]
    (defregistry testing-registry breaker-basic-config)
    (defbreaker testing-breaker {:registry testing-registry})

    (testing "breaker from CLOSED to OPEN and get breaker from registry"
      ;; fill the ring buffer for closed state
      (is (= :CLOSED (state testing-breaker)))
      (fill-ring-buffer testing-breaker (:ring-buffer-size-in-closed-state breaker-basic-config) 0)

      ;; let circuit open
      (let-breaker-open testing-breaker
                        (max-failed-times (:ring-buffer-size-in-closed-state breaker-basic-config)
                                          (:failure-rate-threshold breaker-basic-config)))
      (is (= [testing-breaker] (get-all-breakers testing-registry))))))

