(ns resilience.retry-test
  (:refer-clojure :exclude [name])
  (:require [clojure.test :refer :all]
            [resilience.retry :refer :all]
            [resilience.core :as resilience])
  (:import (clojure.lang ExceptionInfo)
           (java.util.concurrent TimeUnit)))

(defn- fail [& _]
  (throw (ex-info "expected exception" {:expected true})))

(defn- retry-and-straight-fail [testing-retry retry-config]
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
        (is (= @retry-times (:max-attempts retry-config)))))))

(defn- success-directly [testing-retry]
  (resilience/execute-with-retry testing-retry
                                 (println "pass")))

(defn- success-after-retry [testing-retry retry-config]
  (let [retry-times (volatile! 0)
        start (System/nanoTime)]
    (resilience/execute-with-retry testing-retry
                                   (let [times (vswap! retry-times inc)
                                         diff (.toMillis TimeUnit/NANOSECONDS (- (System/nanoTime) start))]
                                     (is (>= diff (* (dec times) (:wait-millis retry-config))))
                                     (when (<= times (/ (:max-attempts retry-config) 2))
                                       (fail))))))

(defn- fail-without-retry [testing-retry]
  (let [retry-times (volatile! 0)]
    (try
      (resilience/execute-with-retry testing-retry
                                     (vswap! retry-times inc)
                                     (throw (IllegalStateException. "ignored exception")))
      (catch IllegalStateException _
        (is (= @retry-times 1))))))

(deftest test-retry
  (let [retry-config {:max-attempts 5
                      :wait-millis  200
                      :ignore-exceptions [IllegalStateException]}
        testing-retry (retry "testing-retry" retry-config)]
    (testing "retry"
      (let [on-retry-times (atom 0)
            on-retry-fn (fn [_ last-throwable]
                          (is (:expected (ex-data last-throwable)))
                          (swap! on-retry-times inc))
            on-success-times (atom 0)
            on-success-fn (fn [_ last-throwable]
                            (is (:expected (ex-data last-throwable)))
                            (swap! on-success-times inc))
            on-error-times (atom 0)
            on-error-fn (fn [_ last-throwable]
                          (is (:expected (ex-data last-throwable)))
                          (swap! on-error-times inc))
            on-ignored-error-times (atom 0)
            on-ignored-error-fn (fn [_ last-throwable]
                                  (isa? last-throwable IllegalStateException)
                                  (swap! on-ignored-error-times inc))]
        (set-on-retry-event-consumer! testing-retry on-retry-fn)
        (set-on-success-event-consumer! testing-retry on-success-fn)
        (set-on-error-event-consumer! testing-retry on-error-fn)
        (set-on-ignored-error-event-consumer! testing-retry on-ignored-error-fn)

        (retry-and-straight-fail testing-retry retry-config)
        (success-directly testing-retry)
        (success-after-retry testing-retry retry-config)
        (fail-without-retry testing-retry)

        (is (= {:number-of-successful-calls-without-retry-attempt 1
                :number-of-failed-calls-without-retry-attempt 1
                :number-of-successful-calls-with-retry-attempt 1
                :number-of-failed-calls-with-retry-attempt 1}
               (metrics testing-retry)))
        ;; 4 retries in retry-and-straight-fail
        ;; 2 retries in success-after-retry
        (is (= @on-retry-times 6))
        (is (= @on-success-times 1))
        (is (= @on-error-times 1))
        (is (= @on-ignored-error-times 1))))))

(deftest test-registry
  (let [retry-config {:max-attempts 5
                      :wait-millis  200}]
    (defregistry testing-registry retry-config)
    (defretry testing-retry {:registry testing-registry})

    (testing "retry and get retry from registry"
      (retry-and-straight-fail testing-retry retry-config)
      (is (= [testing-retry] (get-all-retries testing-registry))))))