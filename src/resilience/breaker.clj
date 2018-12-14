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
                                                        CircuitBreakerEvent CircuitBreakerEvent$Type)))

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

(def ^{:dynamic true
       :doc     "Contextual value represents circuit breaker name"}
*breaker-name*)

(def ^{:dynamic true
       :doc "Contextual value represents event create time"}
*creation-time*)

(defmacro ^:no-doc with-context [abstract-event & body]
  (let [abstract-event (vary-meta abstract-event assoc :tag `CircuitBreakerEvent)]
    `(binding [*breaker-name* (.getCircuitBreakerName ~abstract-event)
               *creation-time* (.getCreationTime ~abstract-event)]
       ~@body)))

(defmulti ^:private on-event (fn [_ event] (.getEventType ^CircuitBreakerEvent event)))

(defmethod on-event CircuitBreakerEvent$Type/SUCCESS
  [consumer-fn-map ^CircuitBreakerOnSuccessEvent event]
  (with-context event
    (when-let [consumer-fn (get consumer-fn-map :on-success)]
      (consumer-fn (.toMillis (.getElapsedDuration event))))))

(defmethod on-event CircuitBreakerEvent$Type/ERROR
  [consumer-fn-map ^CircuitBreakerOnErrorEvent event]
  (with-context event
    (when-let [consumer-fn (get consumer-fn-map :on-error)]
      (consumer-fn (.getThrowable event) (.toMillis (.getElapsedDuration event))))))

(defmethod on-event CircuitBreakerEvent$Type/STATE_TRANSITION
  [consumer-fn-map ^CircuitBreakerOnStateTransitionEvent event]
  (with-context event
    (when-let [consumer-fn (get consumer-fn-map :on-state-transition)]
      (let [^CircuitBreaker$StateTransition state-trans (.getStateTransition event)]
        (consumer-fn (u/enum->keyword (.getFromState state-trans))
                     (u/enum->keyword (.getToState state-trans)))))))

(defmethod on-event CircuitBreakerEvent$Type/RESET
  [consumer-fn-map event]
  (with-context event
    (when-let [consumer-fn (get consumer-fn-map :on-reset)]
      (consumer-fn))))

(defmethod on-event CircuitBreakerEvent$Type/IGNORED_ERROR
  [consumer-fn-map ^CircuitBreakerOnIgnoredErrorEvent event]
  (with-context event
    (when-let [consumer-fn (get consumer-fn-map :on-ignored-error)]
      (consumer-fn (.getThrowable event) (.toMillis (.getElapsedDuration event))))))

(defmethod on-event CircuitBreakerEvent$Type/NOT_PERMITTED
  [consumer-fn-map event]
  (with-context event
    (when-let [consumer-fn (get consumer-fn-map :on-call-not-permitted)]
      (consumer-fn))))

(defn- create-consumer
  ([consumer-fn-map]
   (reify EventConsumer
     (consumeEvent [_ event]
       (on-event consumer-fn-map event))))
  ([k consumer-fn]
   (reify EventConsumer
     (consumeEvent [_ event]
       (on-event {k consumer-fn} event)))))

(defn set-on-success-event-consumer!
  [^CircuitBreaker breaker consumer-fn]
  "set a consumer to consume `on-success` event which emitted when request success from circuit breaker.
   `consumer-fn` accepts a function which takes `elapsed-millis` as arguments,
   which stands for the duration in milliseconds of the successful request"
  (let [pub (.getEventPublisher breaker)]
    (.onSuccess pub (create-consumer :on-success consumer-fn))))

(defn set-on-error-event-consumer!
  [^CircuitBreaker breaker consumer-fn]
  "set a consumer to consume `on-error` event which emitted when request failed from circuit breaker.
   `consumer-fn` accepts a function which takes `throwable`, `elapsed-millis` as arguments"
  (let [pub (.getEventPublisher breaker)]
    (.onError pub (create-consumer :on-error consumer-fn))))

(defn set-on-state-transition-event-consumer!
  [^CircuitBreaker breaker consumer-fn]
  "set a consumer to consume `on-state-transition` event which emitted when the state of the circuit breaker changed
   `consumer-fn` accepts a function which takes `from-state`, `to-state` as arguments"
  (let [pub (.getEventPublisher breaker)]
    (.onStateTransition pub (create-consumer :on-state-transition consumer-fn))))

(defn set-on-reset-event-consumer!
  [^CircuitBreaker breaker consumer-fn]
  "set a consumer to consume `on-reset` event which emitted when the state of the circuit breaker reset to CLOSED
   `consumer-fn` accepts a function which takes no arguments"
  (let [pub (.getEventPublisher breaker)]
    (.onReset pub (create-consumer :on-reset consumer-fn))))

(defn set-on-ignored-error-event!
  [^CircuitBreaker breaker consumer-fn]
  "set a consumer to consume `on-ignored-error` event which emitted when the request failed due to an error
   which we determine to ignore
   `consumer-fn` accepts a function which takes `throwable`, `elapsed-millis` as arguments"
  (let [pub (.getEventPublisher breaker)]
    (.onIgnoredError pub (create-consumer :on-ignored-error consumer-fn))))

(defn set-on-call-not-permitted-consumer!
  [^CircuitBreaker breaker consumer-fn]
  "set a consumer to consume on call not permitted event which emitted when a request is
   refused due to circuit breaker open.
   `consumer-fn` accepts a function which takes no arguments"
  (let [pub (.getEventPublisher breaker)]
    (.onCallNotPermitted pub (create-consumer :on-call-not-permitted consumer-fn))))

(defn set-on-all-event-consumer!
  [^CircuitBreaker breaker consumer-fn-map]
  "set a consumer to consume all available events emitted from circuit breaker.
   `consumer-fn-map` accepts a map which contains following key and function pairs:

   * `on-success` accepts a function which takes `elapsed-millis` as arguments
   * `on-error` accepts a function which takes `throwable`, `elapsed-millis` as arguments
   * `on-state-transition` accepts a function which takes `from-state`, `to-state` as arguments
   * `on-reset` accepts a function which takes no arguments
   * `on-ignored-error` accepts a function which takes `throwable`, `elapsed-millis` as arguments
   * `on-call-not-permitted` accepts a function which takes no arguments"
  (let [pub (.getEventPublisher breaker)]
    (.onEvent pub (create-consumer consumer-fn-map))))



