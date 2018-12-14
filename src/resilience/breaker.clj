(ns resilience.breaker
  (:refer-clojure :exclude [name reset!])
  (:require [resilience.util :as u]
            [resilience.spec :as s])
  (:import (io.github.resilience4j.circuitbreaker CircuitBreakerConfig CircuitBreakerConfig$Builder
                                                  CircuitBreakerRegistry CircuitBreaker CircuitBreaker$StateTransition)
           (java.time Duration)
           (java.util.function Predicate)
           (io.github.resilience4j.core EventConsumer)
           (io.github.resilience4j.circuitbreaker.event CircuitBreakerOnSuccessEvent CircuitBreakerOnErrorEvent
                                                        CircuitBreakerOnIgnoredErrorEvent
                                                        CircuitBreakerOnStateTransitionEvent
                                                        CircuitBreakerOnResetEvent CircuitBreakerOnCallNotPermittedEvent)))

(defn ^CircuitBreakerConfig circuit-breaker-config [opts]
  (s/verify-opt-map-keys-with-spec :breaker/breaker-config opts)

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
  (u/enum->keyword (.getState breaker)))

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

(defprotocol CircuitBreakerEventListener
  (on-success [this breaker-name elapsed-millis])
  (on-error [this breaker-name throwable elapsed-millis])
  (on-state-transition [this breaker-name from-state to-state])
  (on-reset [this breaker-name])
  (on-ignored-error [this breaker-name throwable elapsed-millis])
  (on-call-not-permitted [this breaker-name]))

(defmulti ^:private relay-event (fn [_ event] (class event)))

(defmethod relay-event CircuitBreakerOnSuccessEvent
  [event-listener ^CircuitBreakerOnSuccessEvent event]
  (let [elapsed-millis (.toMillis (.getElapsedDuration event))]
    (on-success event-listener (.getCircuitBreakerName event) elapsed-millis)))

(defmethod relay-event CircuitBreakerOnErrorEvent
  [event-listener ^CircuitBreakerOnErrorEvent event]
  (let [elapsed-millis (.toMillis (.getElapsedDuration event))]
    (on-error event-listener (.getCircuitBreakerName event) (.getThrowable event) elapsed-millis)))

(defmethod relay-event CircuitBreakerOnStateTransitionEvent
  [event-listener ^CircuitBreakerOnStateTransitionEvent event]
  (let [^CircuitBreaker$StateTransition state-trans (.getStateTransition event)]
    (on-state-transition event-listener (.getCircuitBreakerName event)
                         (u/enum->keyword (.getFromState state-trans))
                         (u/enum->keyword (.getToState state-trans)))))

(defmethod relay-event CircuitBreakerOnResetEvent
  [event-listener event]
  (on-reset event-listener (.getCircuitBreakerName ^CircuitBreakerOnResetEvent event)))

(defmethod relay-event CircuitBreakerOnIgnoredErrorEvent
  [event-listener ^CircuitBreakerOnIgnoredErrorEvent event]
  (let [elapsed-millis (.toMillis (.getElapsedDuration event))]
    (on-ignored-error event-listener (.getCircuitBreakerName event) (.getThrowable event) elapsed-millis)))


(defmethod relay-event CircuitBreakerOnCallNotPermittedEvent
  [event-listener event]
  (on-call-not-permitted event-listener (.getCircuitBreakerName ^CircuitBreakerOnCallNotPermittedEvent event)))

(defmacro ^:private generate-consumer [event-listener]
  `(reify EventConsumer
     (consumeEvent [_ event#]
       (relay-event ~event-listener event#))))

(defn listen-on-success [^CircuitBreaker breaker event-listener]
  (let [pub (.getEventPublisher breaker)]
    (.onSuccess pub (generate-consumer event-listener))))

(defn listen-on-error [^CircuitBreaker breaker event-listener]
  (let [pub (.getEventPublisher breaker)]
    (.onError pub (generate-consumer event-listener))))

(defn listen-on-state-transition [^CircuitBreaker breaker event-listener]
  (let [pub (.getEventPublisher breaker)]
    (.onStateTransition pub (generate-consumer event-listener))))

(defn listen-on-reset [^CircuitBreaker breaker event-listener]
  (let [pub (.getEventPublisher breaker)]
    (.onReset pub (generate-consumer event-listener))))

(defn listen-on-ignored-error [^CircuitBreaker breaker event-listener]
  (let [pub (.getEventPublisher breaker)]
    (.onIgnoredError pub (generate-consumer event-listener))))

(defn listen-on-call-not-permitted [^CircuitBreaker breaker event-listener]
  (let [pub (.getEventPublisher breaker)]
    (.onCallNotPermitted pub (generate-consumer event-listener))))

(defn listen-on-all-event [^CircuitBreaker breaker event-listener]
  (let [pub (.getEventPublisher breaker)]
    (.onEvent pub (generate-consumer event-listener))))



