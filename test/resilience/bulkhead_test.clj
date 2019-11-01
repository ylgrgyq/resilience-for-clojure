(ns resilience.bulkhead-test
  (:refer-clojure :exclude [name])
  (:require [clojure.test :refer :all]
            [resilience.bulkhead :refer :all]
            [resilience.core :as resilience])
  (:import (java.util.concurrent CountDownLatch CyclicBarrier)
           (io.github.resilience4j.bulkhead BulkheadFullException)))

(deftest test-bulkhead
  (let [bulkhead-config {:max-concurrent-calls 5
                         :max-wait-millis      200
                         :writable-stack-trace-enabled false}]
    (defbulkhead testing-bulkhead bulkhead-config)
    (testing "bulkhead wait timeout"
      (let [barrier (CyclicBarrier. (inc (:max-concurrent-calls bulkhead-config)))
            done (CountDownLatch. 1)
            on-call-finished-times (atom 0)
            on-call-finished-fn (fn [] (swap! on-call-finished-times inc))
            on-call-permitted-times (atom 0)
            on-call-permitted-fn (fn [] (swap! on-call-permitted-times inc))
            on-call-rejected-times (atom 0)
            on-call-rejected-fn (fn [] (swap! on-call-rejected-times inc))]

        (set-on-call-finished-event-consumer! testing-bulkhead on-call-finished-fn)
        (set-on-call-permitted-event-consumer! testing-bulkhead on-call-permitted-fn)
        (set-on-call-rejected-event-consumer! testing-bulkhead on-call-rejected-fn)

        (set-on-all-event-consumer! testing-bulkhead
                                    {:on-call-finished on-call-finished-fn
                                     :on-call-permitted on-call-permitted-fn
                                     :on-call-rejected on-call-rejected-fn})

        (doseq [_ (range (:max-concurrent-calls bulkhead-config))]
          (future
            (try
              (resilience/execute-with-bulkhead testing-bulkhead
                                                (.await barrier)
                                                (Thread/sleep (* 2 (:max-wait-millis bulkhead-config)))
                                                (.await done))
              (.await barrier)
              (catch Exception _
                (is false)))))
        (.await barrier)
        (let [start (System/nanoTime)]
          (is (thrown? BulkheadFullException
                       (resilience/execute-with-bulkhead testing-bulkhead
                                                         (throw (IllegalStateException. "Can't be here")))))
          (is (>= (- (System/nanoTime) start) (:max-wait-millis bulkhead-config))))
        (.countDown done)
        (.await barrier)

        (is (= {:available-concurrent-calls (:max-concurrent-calls bulkhead-config)
                :max-allowed-concurrent-calls (:max-concurrent-calls bulkhead-config)}
               (metrics testing-bulkhead)))
        ;; we registered every kind of events twice, so statistic should be double
        (is (= @on-call-permitted-times (* 2 (:max-concurrent-calls bulkhead-config))))
        (is (= @on-call-finished-times (* 2 (:max-concurrent-calls bulkhead-config))))
        (is (= @on-call-rejected-times 2))))
    (testing "bulkhead acquire and release permission"
      (let [on-call-finished-times (atom 0)
            on-call-finished-fn (fn [] (swap! on-call-finished-times inc))
            on-call-permitted-times (atom 0)
            on-call-permitted-fn (fn [] (swap! on-call-permitted-times inc))
            on-call-rejected-times (atom 0)
            on-call-rejected-fn (fn [] (swap! on-call-rejected-times inc))]

        (set-on-call-finished-event-consumer! testing-bulkhead on-call-finished-fn)
        (set-on-call-permitted-event-consumer! testing-bulkhead on-call-permitted-fn)
        (set-on-call-rejected-event-consumer! testing-bulkhead on-call-rejected-fn)

        (set-on-all-event-consumer! testing-bulkhead
                                    {:on-call-finished on-call-finished-fn
                                     :on-call-permitted on-call-permitted-fn
                                     :on-call-rejected on-call-rejected-fn})

        (doseq [_ (range (:max-concurrent-calls bulkhead-config))]
          (acquire-permission testing-bulkhead))
        (is (= {:available-concurrent-calls 0
                :max-allowed-concurrent-calls (:max-concurrent-calls bulkhead-config)}
               (metrics testing-bulkhead)))
        (is (false? (try-acquire-permission testing-bulkhead)))
        (doseq [_ (range (:max-concurrent-calls bulkhead-config))]
          (release-permission testing-bulkhead))
        (is (= {:available-concurrent-calls (:max-concurrent-calls bulkhead-config)
                :max-allowed-concurrent-calls (:max-concurrent-calls bulkhead-config)}
               (metrics testing-bulkhead)))
        ;; we registered every kind of events twice, so statistic should be double
        (is (= @on-call-permitted-times (* 2 (:max-concurrent-calls bulkhead-config))))
        (is (= @on-call-finished-times 0))
        (is (= @on-call-rejected-times 2))))))

(deftest test-registry
  (let [bulkhead-config {:max-concurrent-calls 5
                         :max-wait-millis      200}]
    (defregistry testing-registry bulkhead-config)
    (defbulkhead testing-bulkhead {:registry testing-registry})

    (testing "bulkhead wait timeout and get bulkhead from registry"
      (let [latch (CountDownLatch. (:max-concurrent-calls bulkhead-config))
            done (CountDownLatch. 1)]
        (doseq [_ (range (:max-concurrent-calls bulkhead-config))]
          (future
            (try
              (resilience/execute-with-bulkhead testing-bulkhead
                                                (.countDown latch)
                                                (Thread/sleep (* 2 (:max-wait-millis bulkhead-config)))
                                                (.await done))
              (catch Exception _
                (is false)))))
        (.await latch)
        (let [start (System/nanoTime)]
          (is (thrown? BulkheadFullException
                       (resilience/execute-with-bulkhead testing-bulkhead
                                                         (throw (IllegalStateException. "Can't be here")))))
          (is (>= (- (System/nanoTime) start) (:max-wait-millis bulkhead-config))))
        (.countDown done)

        (is (= [testing-bulkhead] (get-all-bulkheads testing-registry)))))))
