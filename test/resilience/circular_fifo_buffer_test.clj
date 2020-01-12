(ns resilience.circular-fifo-buffer-test
  (:require [clojure.test :refer :all]
            [resilience.circular-fifo-buffer :refer :all])
  (:import (io.vavr.control Option)
           (java.util.concurrent ThreadLocalRandom)))

(deftest test-take
  (testing "take from empty buffer"
    (is (.isEmpty ^Option (take (circular-fifo-buffer 100)))))
  (testing "take"
    (let [val 10101
          buffer (circular-fifo-buffer 100)]
      (add buffer val)
      (is (= val (.get (take buffer)))))))

(deftest test-empty
  (testing "empty"
    (is (empty? (circular-fifo-buffer 100))))
  (testing "non-empty"
    (let [val 10101
          buffer (circular-fifo-buffer 100)]
      (add buffer val)
      (is (not (empty? buffer))))))

(deftest test-size
  (testing "empty"
    (is (zero? (size (circular-fifo-buffer 100)))))
  (testing "non-empty"
    (let [val 10101
          buffer (circular-fifo-buffer 100)]
      (add buffer val)
      (is (= 1 (size buffer)))))
  (testing "non-empty2"
    (let [val 10101
          buffer (circular-fifo-buffer 100)
          expect-size (.nextInt (ThreadLocalRandom/current) 1 100)]
      (doseq [_ (range expect-size)]
        (add buffer val))
      (is (= expect-size (size buffer))))))

(deftest test-full
  (testing "empty"
    (is (not (full? (circular-fifo-buffer 100)))))
  (testing "non-empty"
    (let [val 10101
          buffer (circular-fifo-buffer 100)]
      (add buffer val)
      (is (not (full? buffer)))))
  (testing "full"
    (let [val 10101
          buffer (circular-fifo-buffer 100)]
      (doseq [_ (range 100)]
        (is (not (full? buffer)))
        (add buffer val))
      (is (full? buffer)))))
