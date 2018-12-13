(ns resilience.ratelimiter
  (:refer-clojure :exclude [name])
  (:require [resilience.util :as u])
  (:import (java.time Duration)
           (io.github.resilience4j.ratelimiter RateLimiterConfig RateLimiterConfig$Builder
                                               RateLimiterRegistry RateLimiter)
           (io.github.resilience4j.ratelimiter.event RateLimiterOnFailureEvent RateLimiterOnSuccessEvent)
           (io.github.resilience4j.core EventConsumer)))

(defn ^RateLimiterConfig rate-limiter-config [opts]
  (if (empty? opts)
    (throw (IllegalArgumentException. "please provide not empty configuration for rate limiter."))
    (let [^RateLimiterConfig$Builder config (RateLimiterConfig/custom)]
      (when-let [timeout (:timeout-millis opts)]
        (.timeoutDuration config (Duration/ofMillis timeout)))

      (when-let [limit (:limit-for-period opts)]
        (.limitForPeriod config (int limit)))

      (when-let [period-millis (:limit-refresh-period-millis opts)]
        (.limitRefreshPeriod config (Duration/ofMillis period-millis)))

      (.build config))))

(defn ^RateLimiterRegistry registry-with-config [^RateLimiterConfig config]
  (RateLimiterRegistry/of config))

(defmacro defregistry [name config]
  (let [sym (with-meta (symbol name) {:tag `RateLimiterRegistry})]
    `(def ~sym
       (let [config# (rate-limiter-config ~config)]
         (registry-with-config config#)))))

(defn get-all-rate-limiters [^RateLimiterRegistry registry]
  (let [heads (.getAllRateLimiters registry)
        iter (.iterator heads)]
    (u/lazy-seq-from-iterator iter)))

(defn rate-limiter [^String name config]
  (let [^RateLimiterRegistry registry (:registry config)
        config (dissoc config :registry)]
    (cond
      (and registry (not-empty config))
      (let [config (rate-limiter-config config)]
        (.rateLimiter registry name ^RateLimiterConfig config))

      registry
      (.rateLimiter registry name)

      :else
      (let [config (rate-limiter-config config)]
        (RateLimiter/of name ^RateLimiterConfig config)))))

;; name configs
;; name registry
;; name registry configs
(defmacro defratelimiter [name config]
  (let [sym (with-meta (symbol name) {:tag `RateLimiter})
        ^String name-in-string (str *ns* "/" name)]
    `(def ~sym (rate-limiter ~name-in-string ~config))))

(defn name
  "Get the name of this RateLimiter"
  [^RateLimiter limiter]
  (.getName limiter))

(defn config
  "Get the RateLimiterConfig of this RateLimiter"
  [^RateLimiter limiter]
  (.getRateLimiterConfig limiter))

(defn change-timeout-millis!
  "Dynamic rate limiter configuration change.
   This method allows to change timeout-millis of current limiter.
   NOTE! New timeout-millis won't affect threads that are currently waiting for permission."
  [^RateLimiter limiter timeout-millis]
  (.changeTimeoutDuration limiter (Duration/ofMillis timeout-millis)))

(defn change-limit-for-period!
  "Dynamic rate limiter configuration change.
   This method allows to change count of permissions available during refresh period.
   NOTE! New limit won't affect current period permissions and will apply only from next one."
  [^RateLimiter limiter limit]
  (.changeLimitForPeriod limiter (int limit)))

(defn acquire-permission!
  "Acquires a permission from this rate limiter, blocking until one is available."
  [^RateLimiter limiter timeout-millis]
  (.getPermission limiter (Duration/ofMillis timeout-millis)))

(defn reserve-permission! [^RateLimiter limiter timeout-millis]
  "Reserves a permission from this rate limiter and returns nanoseconds you should wait for it.
  If returned long is negative, it means that you failed to reserve permission,"
  (.reservePermission limiter (Duration/ofMillis timeout-millis)))

(defn metrics
  "Get the Metrics of this RateLimiter."
  [^RateLimiter limiter]
  (let [metric (.getMetrics limiter)]
    {:number-of-waiting-threads (.getNumberOfWaitingThreads metric)
     :available-permissions (.getAvailablePermissions metric)}))

(defprotocol RateLimiterEventListener
  (on-success [this rate-limiter-name])
  (on-failure [this rate-limiter-name]))

(defmulti ^:private relay-event (fn [_ event] (class event)))

(defmethod relay-event RateLimiterOnSuccessEvent
  [event-listener ^RateLimiterOnSuccessEvent event]
  (on-success event-listener (.getRateLimiterName event)))

(defmethod relay-event RateLimiterOnFailureEvent
  [event-listener ^RateLimiterOnFailureEvent event]
  (on-failure event-listener (.getRateLimiterName event)))

(defmacro ^:private generate-consumer [event-listener]
  `(reify EventConsumer
     (consumeEvent [_ event#]
       (relay-event ~event-listener event#))))

(defn listen-on-success [^RateLimiter limiter event-listener]
  (let [pub (.getEventPublisher limiter)]
    (.onSuccess pub (generate-consumer event-listener))))

(defn listen-on-failure [^RateLimiter limiter event-listener]
  (let [pub (.getEventPublisher limiter)]
    (.onFailure pub (generate-consumer event-listener))))

(defn listen-on-all-event [^RateLimiter limiter event-listener]
  (let [pub (.getEventPublisher limiter)]
    (.onEvent pub (generate-consumer event-listener))))
