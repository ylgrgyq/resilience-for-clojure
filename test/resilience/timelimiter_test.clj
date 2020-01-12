(ns resilience.timelimiter-test
  (:require [clojure.test :refer :all]
            [resilience.timelimiter :refer :all]
            [resilience.retry :as retry]
            [resilience.core :as resilience])
  (:import (java.util.concurrent CompletableFuture TimeoutException)))

(deftest test-time-limiter
  (let [time-limiter-config {:timeout-millis         100
                             :cancel-running-future? false}]

    (deftimelimiter testing-time-limiter time-limiter-config)
    (testing "timeout without cancel"
      (let [timeout (atom false)
            task-finish-future (CompletableFuture.)]
        (try
          (execute-future-with-time-limiter testing-time-limiter
                                            (do (future (Thread/sleep 200)
                                                        (.complete task-finish-future true))
                                                task-finish-future))
          (catch TimeoutException _
            (reset! timeout true)))
        (is @timeout)
        (is (.get task-finish-future))))
    (testing "timeout with cancel"
      (let [limiter (time-limiter {:timeout-millis         100
                                   :cancel-running-future? true})
            task-finish-future (CompletableFuture.)
            timeout (atom false)]
        (try
          (execute-future-with-time-limiter limiter
                                            (do (future (Thread/sleep 200)
                                                        (.complete task-finish-future true))
                                                task-finish-future))
          (catch TimeoutException _
            (reset! timeout true)))
        (is @timeout)
        (is (.isCancelled task-finish-future))))))

(deftest test-execute-with-time-limiter
  (testing "timeout with cancel"
    (let [limiter       (time-limiter {:timeout-millis         100
                                       :cancel-running-future? true})
          testing-retry (retry/retry "testing-retry" {:max-attempts 3})
          times         (atom 0)
          timeout       (atom false)]
      (try
        (resilience/execute-with-time-limiter
          (future (do (swap! times inc) (Thread/sleep 200)))
          limiter
          (resilience/with-retry testing-retry))
        (catch TimeoutException _
          (reset! timeout true)))
      (is @timeout)
      (is (= 3 @times)))))
