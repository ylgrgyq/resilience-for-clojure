(ns resilience.thread-pool-bulkhead-test
  (:refer-clojure :exclude [name])
  (:require [clojure.test :refer :all]
            [resilience.thread-pool-bulkhead :refer :all]
            [resilience.core :as resilience])
  (:import (java.util.concurrent CountDownLatch)
           (io.github.resilience4j.bulkhead BulkheadFullException)))

(deftest test-bulkhead
  (let [bulkhead-config {:max-thread-pool-size  5
                         :core-thread-pool-size 2
                         :queue-capacity        10
                         :keep-alive-millis     1000}]
    (defbulkhead testing-bulkhead bulkhead-config)
    (testing "bulkhead wait timeout"
      (let [exceeding-pool-size        (- (:max-thread-pool-size bulkhead-config)
                                          (:core-thread-pool-size bulkhead-config))
            core-run-barrier           (CountDownLatch. (:core-thread-pool-size bulkhead-config))
            wait-in-queue-job-latch    (CountDownLatch. (:queue-capacity bulkhead-config))
            exceeding-pool-run-barrier (CountDownLatch. exceeding-pool-size)
            done                       (CountDownLatch. 1)
            call-finish-times-latch    (CountDownLatch. (* 2 (+ (:queue-capacity bulkhead-config)
                                                               (:max-thread-pool-size bulkhead-config))))
            on-call-finished-fn        (fn [] (.countDown call-finish-times-latch))
            on-call-permitted-times    (atom 0)
            on-call-permitted-fn       (fn [] (swap! on-call-permitted-times inc))
            on-call-rejected-times     (atom 0)
            on-call-rejected-fn        (fn [] (swap! on-call-rejected-times inc))]

        (set-on-call-finished-event-consumer! testing-bulkhead on-call-finished-fn)
        (set-on-call-permitted-event-consumer! testing-bulkhead on-call-permitted-fn)
        (set-on-call-rejected-event-consumer! testing-bulkhead on-call-rejected-fn)

        (set-on-all-event-consumer! testing-bulkhead
                                    {:on-call-finished on-call-finished-fn
                                     :on-call-permitted on-call-permitted-fn
                                     :on-call-rejected on-call-rejected-fn})
        ;; stuff up core pool
        (doseq [_ (range (:core-thread-pool-size bulkhead-config))]
          (try
            (resilience/execute-with-thread-pool-bulkhead testing-bulkhead
                                                          (.await done)
                                                          (.countDown core-run-barrier))
            (catch Exception _
              (is false))))

        ;; stuff up the waiting queue
        (doseq [_ (range (:queue-capacity bulkhead-config))]
          (try
            (resilience/execute-with-thread-pool-bulkhead testing-bulkhead
                                                          (.countDown wait-in-queue-job-latch))

            (catch Exception _
              (is false))))

        ;; make thread pool enlarge to max size and stuff it up
        (doseq [_ (range exceeding-pool-size)]
          (try
            (resilience/execute-with-thread-pool-bulkhead testing-bulkhead
                                                          (.await done)
                                                          (.countDown exceeding-pool-run-barrier))
            (catch Exception _
              (is false))))

        (is (thrown? BulkheadFullException
                     (resilience/execute-with-thread-pool-bulkhead testing-bulkhead
                                                       (throw (IllegalStateException. "Can't be here")))))

        ;; let stuffed job run
        (.countDown done)
        ;; core and exceeding pool will run first, then the job in the queue run
        (.await core-run-barrier)
        (.await exceeding-pool-run-barrier)
        (.await wait-in-queue-job-latch)

        (is (= {:core-thread-pool-size    (:core-thread-pool-size bulkhead-config)
                :max-thread-pool-size     (:max-thread-pool-size bulkhead-config)
                :queue-capacity           (:queue-capacity bulkhead-config)
                :queue-depth              0
                :remaining-queue-capacity (:queue-capacity bulkhead-config)
                :thread-pool-size         (:max-thread-pool-size bulkhead-config)}
               (metrics testing-bulkhead)))
        ;; we registered every kind of events twice, so statistic should be double
        (.await call-finish-times-latch)
        (is (= @on-call-permitted-times (* 2 (+ (:queue-capacity bulkhead-config)
                                                (:max-thread-pool-size bulkhead-config)))))
        (is (= @on-call-rejected-times 2))))))

(deftest test-registry
  (let [bulkhead-config {:core-thread-pool-size 5
                         :max-thread-pool-size  5
                         :queue-capacity        1}]
    (defregistry testing-registry bulkhead-config)
    (defbulkhead testing-bulkhead {:registry testing-registry})

    (testing "bulkhead wait timeout and get bulkhead from registry"
      (let [latch (CountDownLatch. (inc (:core-thread-pool-size bulkhead-config)))
            done (CountDownLatch. 1)]
        (doseq [_ (range (:core-thread-pool-size bulkhead-config))]
          (try
            (resilience/execute-with-thread-pool-bulkhead testing-bulkhead
                                              (.await done)
                                              (.countDown latch))
            (catch Exception _
              (is false))))

        (resilience/execute-with-thread-pool-bulkhead testing-bulkhead
                                          (.countDown latch))
        (is (thrown? BulkheadFullException
                     (resilience/execute-with-thread-pool-bulkhead testing-bulkhead
                                                       (throw (IllegalStateException. "Can't be here")))))
        (.countDown done)
        (.await latch)

        (is (= [testing-bulkhead] (get-all-thread-pool-bulkheads testing-registry)))))))