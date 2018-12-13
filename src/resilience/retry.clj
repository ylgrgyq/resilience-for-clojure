(ns resilience.retry
  (:refer-clojure :exclude [name])
  (:require [resilience.util :as u])
  (:import (io.github.resilience4j.retry RetryConfig RetryConfig$Builder RetryRegistry Retry)
           (java.time Duration)
           (java.util.function Predicate)
           (io.github.resilience4j.retry.event RetryOnSuccessEvent RetryOnRetryEvent
                                               RetryOnErrorEvent RetryOnIgnoredErrorEvent)
           (io.github.resilience4j.core EventConsumer)))

(defn ^RetryConfig retry-config [opts]
  (if (empty? opts)
    (throw (IllegalArgumentException. "please provide not empty configuration for retry."))
    (let [^RetryConfig$Builder config (RetryConfig/custom)]
      (when-let [attemp (:max-attempts opts)]
        (.maxAttempts config attemp))
      (when-let [wait-millis (:wait-millis opts)]
        (.waitDuration config (Duration/ofMillis wait-millis)))
      (when-let [f (:retry-on-result opts)]
        (.retryOnResult config (reify Predicate
                                 (test [_ t] (f t)))))
      (when-let [f (:retry-on-exception opts)]
        (.retryOnException config (reify Predicate
                                    (test [_ t] (f t)))))

      (when-let [exceptions (:retry-exceptions opts)]
        (.retryExceptions config (into-array Class exceptions)))

      (when-let [exceptions (:ignore-exceptions opts)]
        (.ignoreExceptions config (into-array Class exceptions)))
      (.build config))))

(defn ^RetryConfig registry-with-config [^RetryConfig config]
  (RetryRegistry/of config))

(defmacro defregistry [name configs]
  (let [sym (with-meta (symbol name) {:tag `RetryRegistry})]
    `(def ~sym
       (let [configs# (retry-config ~configs)]
         (registry-with-config configs#)))))

(defn get-all-retries [^RetryRegistry registry]
  (let [breakers (.getAllRetries registry)
        iter (.iterator breakers)]
    (u/lazy-seq-from-iterator iter)))

(defn retry [^String name config]
  (let [^RetryRegistry registry (:registry config)
        config (dissoc config :registry)]
    (cond
      (and registry (not-empty config))
      (let [breaker-config (retry-config config)]
        (.retry registry name ^RetryConfig breaker-config))

      registry
      (.retry registry name)

      :else
      (let [breaker-config (retry-config config)]
        (Retry/of name ^RetryConfig breaker-config)))))

;; name configs
;; name registry
;; name registry configs
(defmacro defretry [name config]
  (let [sym (with-meta (symbol name) {:tag `Retry})
        ^String name-in-string (str *ns* "/" name)]
    `(def ~sym (retry ~name-in-string ~config))))

(defn name
  "Get the name of this Retry."
  [^Retry retry]
  (.getName retry))

(defn config [^Retry retry]
  (.getRetryConfig retry))

(defn metrics
  "Get the Metrics of this Retry"
  [^Retry retry]
  (let [metric (.getMetrics retry)]
    {:number-of-successful-calls-without-retry-attempt (.getNumberOfSuccessfulCallsWithoutRetryAttempt metric)
     :number-of-failed-calls-without-retry-attempt (.getNumberOfFailedCallsWithoutRetryAttempt metric)
     :number-of-successful-calls-with-retry-attempt (.getNumberOfSuccessfulCallsWithRetryAttempt metric)
     :number-of-failed-calls-with-retry-attempt (.getNumberOfFailedCallsWithRetryAttempt metric)}))

(defprotocol RetryEventListener
  (on-retry [this retry-name number-of-retry-attempts last-throwable])
  (on-success [this retry-name number-of-retry-attempts last-throwable])
  (on-error [this retry-name number-of-retry-attempts last-throwable])
  (on-ignored-error [this retry-name number-of-retry-attempts last-throwable]))

(defmulti ^:private relay-event (fn [_ event] (class event)))

(defmethod relay-event RetryOnRetryEvent
  [event-listener ^RetryOnRetryEvent event]
  (on-retry event-listener
            (.getRateLimiterName event)
            (.getNumberOfRetryAttempts event)
            (.getLastThrowable event)))

(defmethod relay-event RetryOnSuccessEvent
  [event-listener ^RetryOnSuccessEvent event]
  (on-success event-listener
              (.getRateLimiterName event)
              (.getNumberOfRetryAttempts event)
              (.getLastThrowable event)))

(defmethod relay-event RetryOnErrorEvent
  [event-listener ^RetryOnErrorEvent event]
  (on-error event-listener
            (.getRateLimiterName event)
            (.getNumberOfRetryAttempts event)
            (.getLastThrowable event)))

(defmethod relay-event RetryOnIgnoredErrorEvent
  [event-listener ^RetryOnIgnoredErrorEvent event]
  (on-ignored-error event-listener
                    (.getRateLimiterName event)
                    (.getNumberOfRetryAttempts event)
                    (.getLastThrowable event)))

(defmacro ^:private generate-consumer [event-listener]
  `(reify EventConsumer
     (consumeEvent [_ event#]
       (relay-event ~event-listener event#))))

(defn listen-on-retry [^Retry retry event-listener]
  (let [pub (.getEventPublisher retry)]
    (.onRetry pub (generate-consumer event-listener))))

(defn listen-on-success [^Retry retry event-listener]
  (let [pub (.getEventPublisher retry)]
    (.onSuccess pub (generate-consumer event-listener))))

(defn listen-on-error [^Retry retry event-listener]
  (let [pub (.getEventPublisher retry)]
    (.onError pub (generate-consumer event-listener))))

(defn listen-on-ignored-error [^Retry retry event-listener]
  (let [pub (.getEventPublisher retry)]
    (.onIgnoredError pub (generate-consumer event-listener))))

(defn listen-on-all-event [^Retry retry event-listener]
  (let [pub (.getEventPublisher retry)]
    (.onEvent pub (generate-consumer event-listener))))