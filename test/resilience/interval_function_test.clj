(ns resilience.interval-function-test
  (:require [clojure.test :refer :all]
            [resilience.interval-function :refer :all])
  (:import (java.util.concurrent ThreadLocalRandom)
           (io.github.resilience4j.core IntervalFunction)))

(deftest test-default-interval-function
  (is (= 500 (.apply (default-interval-function)
                     (.nextInt (ThreadLocalRandom/current) 1 Integer/MAX_VALUE)))))

(deftest test-fixed-interval-function
  (testing "fixed interval"
    (is (= 101 (.apply (create-interval-function 101)
                       (.nextInt (ThreadLocalRandom/current) 1 Integer/MAX_VALUE)))))
  (testing "fixed interval with backoff function"
    (let [interval-stream (.apply (create-interval-function 101 inc) (int 2))]
      (is (= 102 interval-stream)))))

(deftest test-create-randomized-interval-function
  (letfn [(calculate-bound [init factor]
            (let [delta (* init factor)]
              [(+ init delta) (- init delta)]))]
    (testing "default interval function"
      (let [attempt  (.nextInt (ThreadLocalRandom/current) 1 Integer/MAX_VALUE)
            [max-interval min-interval] (calculate-bound IntervalFunction/DEFAULT_INITIAL_INTERVAL
                                                         IntervalFunction/DEFAULT_RANDOMIZATION_FACTOR)
            interval (.apply (create-randomized-interval-function) attempt)]
        (is (and (>= interval min-interval) (<= interval max-interval)))))
    (testing "with initial interval millis"
      (let [attempt  (.nextInt (ThreadLocalRandom/current) 1 Integer/MAX_VALUE)
            init-interval 1000
            [max-interval min-interval] (calculate-bound init-interval
                                                         IntervalFunction/DEFAULT_RANDOMIZATION_FACTOR)
            interval (.apply (create-randomized-interval-function init-interval) attempt)]
        (is (and (>= interval min-interval) (<= interval max-interval)))))
    (testing "with initial interval and randomization factor"
      (let [attempt  (.nextInt (ThreadLocalRandom/current) 1 Integer/MAX_VALUE)
            init-interval 1000
            factor 0.5
            [max-interval min-interval] (calculate-bound init-interval factor)
            interval (.apply (create-randomized-interval-function init-interval factor) attempt)]
        (is (and (>= interval min-interval) (<= interval max-interval)))))))

(deftest test-create-exponential-backoff
  (testing "default function"
    (let [interval (.apply (create-exponential-backoff) (int 2))]
      (is (= 750 interval))))
  (testing "with initial interval"
    (let [interval (.apply (create-exponential-backoff 100) (int 2))]
      (is (= 150 interval))))
  (testing "with initial interval and multiplier"
    (let [interval (.apply (create-exponential-backoff 100 2.5) (int 2))]
      (is (= 250 interval)))))

(deftest test-create-exponential-randomized-interval-function
  (letfn [(calculate-bound [init factor]
            (let [delta (* init factor)]
              [(+ init delta) (- init delta)]))]
    (testing "default interval function"
      (let [attempt  (int 2)
            interval (.apply (create-exponential-backoff IntervalFunction/DEFAULT_INITIAL_INTERVAL)
                             attempt)
            [max-interval min-interval] (calculate-bound interval
                                                         IntervalFunction/DEFAULT_RANDOMIZATION_FACTOR)
            interval (.apply (create-exponential-randomized-interval-function) attempt)]
        (is (and (>= interval min-interval) (<= interval max-interval)))))
    (testing "with initial interval millis"
      (let [attempt  (int 2)
            init-interval 1000
            interval (.apply (create-exponential-backoff init-interval) attempt)
            [max-interval min-interval] (calculate-bound interval
                                                         IntervalFunction/DEFAULT_RANDOMIZATION_FACTOR)
            interval (.apply (create-exponential-randomized-interval-function init-interval) attempt)]
        (is (and (>= interval min-interval) (<= interval max-interval)))))
    (testing "with initial interval and multiplier factor"
      (let [attempt  (int 2)
            init-interval 1000
            multiplier 3.5
            interval (.apply (create-exponential-backoff init-interval multiplier) attempt)
            [max-interval min-interval] (calculate-bound interval
                                                         IntervalFunction/DEFAULT_RANDOMIZATION_FACTOR)
            interval (.apply (create-exponential-randomized-interval-function init-interval multiplier) attempt)]
        (is (and (>= interval min-interval) (<= interval max-interval)))))
    (testing "with initial interval and multiplier factor and randomization factor"
      (let [attempt  (int 2)
            init-interval 1000
            multiplier 3.5
            randomization-factor 0.05
            interval (.apply (create-exponential-backoff init-interval multiplier) attempt)
            [max-interval min-interval] (calculate-bound interval
                                                         randomization-factor)
            interval (.apply (create-exponential-randomized-interval-function init-interval multiplier randomization-factor)
                             attempt)]
        (is (and (>= interval min-interval) (<= interval max-interval)))))))


