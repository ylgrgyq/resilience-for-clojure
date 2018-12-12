(ns resilience.breaker
  (:refer-clojure :exclude [name])
  (:require [resilience.util :as u])
  (:import (io.github.resilience4j.circuitbreaker CircuitBreakerConfig CircuitBreakerConfig$Builder
                                                  CircuitBreakerRegistry CircuitBreaker)
           (java.time Duration)
           (java.util.function Predicate)))

(defn ^CircuitBreakerConfig circuit-breaker-config [opts]
  (if (empty? opts)
    (throw (IllegalArgumentException. "please provide not empty configuration for circuit breaker."))
    (let [^CircuitBreakerConfig$Builder config (CircuitBreakerConfig/custom)]
      (when-let [failure-threshold (:failure-rate-threshold opts)]
        (.failureRateThreshold config (float failure-threshold)))
      (when-let [duration (:wait-millis-in-open-state opts)]
        (.waitDurationInOpenState config (Duration/ofMillis duration)))
      (when-let [size (:ring-buffer-size-in-half-open-state opts)]
        (.ringBufferSizeInHalfOpenState config size))
      (when-let [size (:ring-buffer-size-in-closed-state opts)]
        (.ringBufferSizeInClosedState config size))

      (when-let [record-failure (:record-failure opts)]
        (.recordFailure config (reify Predicate
                                 (test [_ v] (record-failure v)))))

      (when-let [exceptions (:record-exceptions opts)]
        (.recordExceptions config (into-array Class exceptions)))

      (when-let [exceptions (:ignore-exceptions opts)]
        (.ignoreExceptions config (into-array Class exceptions)))

      (when (:automatic-transfer-from-open-to-half-open? opts)
        (.enableAutomaticTransitionFromOpenToHalfOpen config))

      (.build config))))

(defn ^CircuitBreakerRegistry registry-with-config [^CircuitBreakerConfig config]
  (CircuitBreakerRegistry/of config))

(defmacro defregistry [name config]
  (let [sym (with-meta (symbol name) {:tag `CircuitBreakerRegistry})]
    `(def ~sym
       (let [config# (circuit-breaker-config ~config)]
         (registry-with-config config#)))))

(defn get-all-breakers [^CircuitBreakerRegistry registry]
  (let [breakers (.getAllCircuitBreakers registry)
        iter (.iterator breakers)]
    (u/lazy-seq-from-iterator iter)))

(defn circuit-breaker [^String name config]
  (let [^CircuitBreakerRegistry registry (:registry config)
        config (dissoc config :registry)]
    (cond
      (and registry (not-empty config))
      (let [config (circuit-breaker-config config)]
        (.circuitBreaker registry name ^CircuitBreakerConfig config))

      registry
      (.circuitBreaker registry name)

      :else
      (let [config (circuit-breaker-config config)]
        (CircuitBreaker/of name ^CircuitBreakerConfig config)))))

;; name configs
;; name registry
;; name registry configs
(defmacro defbreaker [name config]
  (let [sym (with-meta (symbol name) {:tag `CircuitBreaker})
        ^String name-in-string (str *ns* "/" name)]
    `(def ~sym (circuit-breaker ~name-in-string ~config))))

(defn name
  "Get the name of this CircuitBreaker"
  [^CircuitBreaker breaker]
  (.getName breaker))

(defn state
  "Get the state of the circuit breaker in keyword format.
   Currently, state can be one of :DISABLED, :CLOSED, :OPEN, :FORCED_OPEN, :HALF_OPEN"
  [^CircuitBreaker breaker]
  (keyword (.name (.getState breaker))))

(defn config
  "Returns the configurations of this CircuitBreaker"
  [^CircuitBreaker breaker]
  (.getCircuitBreakerConfig breaker))

(defn reset!
  "Get the circuit breaker to its original closed state, losing statistics.
   Should only be used, when you want to want to fully reset the circuit breaker without creating a new one."
  [^CircuitBreaker breaker]
  (.reset breaker))

(defn transition-to-closed-state! [^CircuitBreaker breaker]
  (.transitionToClosedState breaker))

(defn transition-to-open-state! [^CircuitBreaker breaker]
  (.transitionToOpenState breaker))

(defn transition-to-half-open! [^CircuitBreaker breaker]
  (.transitionToHalfOpenState breaker))

(defn transition-to-disabled-state! [^CircuitBreaker breaker]
  (.transitionToDisabledState breaker))

(defn transition-to-forced-open-state! [^CircuitBreaker breaker]
  (.transitionToForcedOpenState breaker))

(defn event-pulisher [^CircuitBreaker breaker]
  (.getEventPublisher breaker))

(defn metrics
  "Get the Metrics of this CircuitBreaker"
  [^CircuitBreaker breaker]
  (let [metric (.getMetrics breaker)]
    {:failure-rate (.getFailureRate metric)
     :number-of-buffered-calls (.getNumberOfBufferedCalls metric)
     :number-of-failed-calls (.getNumberOfFailedCalls metric)
     :number-of-not-permitted-calls (.getNumberOfNotPermittedCalls metric)
     :max-number-of-buffered-calls (.getMaxNumberOfBufferedCalls metric)
     :number-of-successful-calls (.getNumberOfSuccessfulCalls metric)}))



