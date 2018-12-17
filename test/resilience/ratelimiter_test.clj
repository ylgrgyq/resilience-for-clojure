(ns resilience.ratelimiter-test
  (:refer-clojure :exclude [name reset!])
  (:require [clojure.test :refer :all]
            [resilience.ratelimiter :refer :all]
            [resilience.core :as resilience])
  (:import (java.util.concurrent TimeUnit)
           (io.github.resilience4j.ratelimiter RequestNotPermitted)))

(defn- duration-nanos [start-nanos]
  (- (System/nanoTime) start-nanos))

(defn- drain-permissions [testing-rate-limiter limiter-config]
  (let [c (volatile! 0)
        permissions-limit (:limit-for-period limiter-config)]
    (loop []
      (when (< @c permissions-limit)
        (resilience/execute-with-rate-limiter testing-rate-limiter
          (vswap! c inc))
        (recur)))
    @c))

(deftest test-rate-limiter
  (testing "do not need to wait when consume at most limit-for-period permissions continuously"
    (let [limiter-config {:timeout-millis              0
                          :limit-for-period            100
                          :limit-refresh-period-nanos (.toNanos TimeUnit/SECONDS 1)}]
      (defratelimiter testing-rate-limiter limiter-config)
      (is (= (drain-permissions testing-rate-limiter limiter-config)
             (:limit-for-period limiter-config)))))

  (testing "when no permissions left next request must wait at least limit-refresh-period-millis for more permissions"
    (let [limiter-config {:timeout-millis              200
                          :limit-for-period            1
                          :limit-refresh-period-nanos (.toNanos TimeUnit/MILLISECONDS 200)}
          c (volatile! 0)
          on-successfule-acquire-times (atom 0)
          on-successfule-acquire-fn (fn [] (swap! on-successfule-acquire-times inc))]
      (defratelimiter testing-rate-limiter limiter-config)
      (set-on-successful-acquire-event-consumer! testing-rate-limiter on-successfule-acquire-fn)

      (set-on-all-event-consumer! testing-rate-limiter
                                  {:on-successful-acquire on-successfule-acquire-fn})

      (is (= (drain-permissions testing-rate-limiter limiter-config)
             (:limit-for-period limiter-config)))

      ;; due to implementation to acquire the next permission after all permissions
      ;; were acquired is not need to wait whole period configured by :limit-for-period
      (let [start (System/nanoTime)]
        (resilience/execute-with-rate-limiter testing-rate-limiter
          (vswap! c inc))
        (let [d (duration-nanos start)]
          (is (<= d (:limit-refresh-period-nanos limiter-config)))))

      (doseq [_ (range 10)]
        (let [start (System/nanoTime)]
          (resilience/execute-with-rate-limiter testing-rate-limiter
            (vswap! c inc))
          (is (< (/ (Math/abs (- (duration-nanos start) (:limit-refresh-period-nanos limiter-config)))
                    (:limit-refresh-period-nanos limiter-config))
                 0.05))))

      (Thread/sleep 300)
      (is (= {:number-of-waiting-threads 0, :available-permissions 1}
             (metrics testing-rate-limiter)))
      (is (= @on-successfule-acquire-times (* 2 (inc @c))))))

  (testing "failed to acquire permission"
    (let [limiter-config {:timeout-millis              50
                          :limit-for-period            1
                          :limit-refresh-period-nanos (.toNanos TimeUnit/SECONDS 1)}
          c (volatile! 0)
          on-failed-acquire-times (atom 0)
          on-failed-acquire-fn (fn [] (swap! on-failed-acquire-times inc))]
      (defratelimiter testing-rate-limiter limiter-config)
      (set-on-failed-acquire-event-consumer! testing-rate-limiter on-failed-acquire-fn)

      (set-on-all-event-consumer! testing-rate-limiter
                                  {:on-failed-acquire on-failed-acquire-fn})

      (is (= (drain-permissions testing-rate-limiter limiter-config)
             (:limit-for-period limiter-config)))

      (is (thrown? RequestNotPermitted
                   (resilience/execute-with-rate-limiter testing-rate-limiter
                     (vswap! c inc))))

      (is (= @on-failed-acquire-times (* 2 (inc @c)))))))

(deftest test-registry
  (testing "do not need to wait when consume at most limit-for-period permissions continuously"
    (let [limiter-config {:timeout-millis              0
                          :limit-for-period            100
                          :limit-refresh-period-nanos (.toNanos TimeUnit/SECONDS 1)}]
      (defregistry testing-registry limiter-config)
      (defratelimiter testing-rate-limiter {:registry testing-registry})

      (is (= (drain-permissions testing-rate-limiter limiter-config)
             (:limit-for-period limiter-config)))
      (is (= [testing-rate-limiter] (get-all-rate-limiters testing-registry))))))
