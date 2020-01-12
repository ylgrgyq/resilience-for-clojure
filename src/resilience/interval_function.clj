(ns resilience.interval-function
  (:import (io.github.resilience4j.core IntervalFunction)
           (java.util.function Function)))

(defn ^IntervalFunction default-interval-function
  "Creates an IntervalFunction which returns a fixed default interval of 500 [ms]."
  []
  (IntervalFunction/ofDefaults))

(defn ^IntervalFunction create-interval-function
  ([interval-millis]
   (IntervalFunction/of ^long interval-millis))
  ([interval-millis backoff-function]
   (IntervalFunction/of ^long interval-millis (reify Function
                                                (apply [_ previous-interval]
                                                  (backoff-function previous-interval))))))

(defn ^IntervalFunction create-randomized-interval-function
  ([]
   (IntervalFunction/ofRandomized))
  ([interval-millis]
   (IntervalFunction/ofRandomized ^long interval-millis))
  ([interval-millis randomization-factor]
   (IntervalFunction/ofRandomized ^long interval-millis ^double randomization-factor)))

(defn ^IntervalFunction create-exponential-backoff
  ([]
   (IntervalFunction/ofExponentialBackoff))
  ([initial-interval-millis]
   (IntervalFunction/ofExponentialBackoff ^long initial-interval-millis))
  ([initial-interval-millis multiplier]
   (IntervalFunction/ofExponentialBackoff ^long initial-interval-millis ^double multiplier)))

(defn ^IntervalFunction create-exponential-randomized-interval-function
  ([]
   (IntervalFunction/ofExponentialRandomBackoff))
  ([initial-interval-millis]
   (IntervalFunction/ofExponentialRandomBackoff ^long initial-interval-millis))
  ([initial-interval-millis multiplier]
   (IntervalFunction/ofExponentialRandomBackoff ^long initial-interval-millis ^double multiplier))
  ([initial-interval-millis multiplier randomization-factor]
   (IntervalFunction/ofExponentialRandomBackoff ^long initial-interval-millis ^double multiplier ^double randomization-factor)))
