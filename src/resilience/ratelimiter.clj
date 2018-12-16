(ns resilience.ratelimiter
  (:refer-clojure :exclude [name])
  (:require [resilience.util :as u]
            [resilience.spec :as s])
  (:import (java.time Duration)
           (io.github.resilience4j.ratelimiter RateLimiterConfig RateLimiterConfig$Builder
                                               RateLimiterRegistry RateLimiter)
           (io.github.resilience4j.ratelimiter.event RateLimiterEvent RateLimiterEvent$Type)
           (io.github.resilience4j.core EventConsumer)))

(defn ^RateLimiterConfig rate-limiter-config [opts]
  (s/verify-opt-map-keys-with-spec :ratelimiter/rate-limiter-config opts)

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

(defn get-all-rate-limiters
  "Get all rate limiters registered to this rate limiter registry instance"
  [^RateLimiterRegistry registry]
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

(def ^{:dynamic true
       :doc     "Contextual value represents rate limiter name"}
*rate-limiter-name*)

(def ^{:dynamic true
       :doc "Contextual value represents event create time"}
*creation-time*)

(defmacro ^{:private true :no-doc true} with-context [abstract-event & body]
  (let [abstract-event (vary-meta abstract-event assoc :tag `RateLimiterEvent)]
    `(binding [*rate-limiter-name* (.getRateLimiterName ~abstract-event)
               *creation-time* (.getCreationTime ~abstract-event)]
       ~@body)))

(defn- create-consumer [consumer-fn]
  (reify EventConsumer
    (consumeEvent [_ event]
      (with-context event
        (consumer-fn)))))

(defn set-on-failed-acquire-event-consumer!
  [^RateLimiter rate-limiter consumer-fn]
  "set a consumer to consume `on-failed-acquire` event which emitted when request is rejected by the rate limiter.
   `consumer-fn` accepts a function which takes no arguments.

   Please note that in `consumer-fn` you can get the rate limiter name and the creation time of the
   consumed event by accessing `*rate-limiter-name*` and `*creation-time*` under this namespace."
  (let [pub (.getEventPublisher rate-limiter)]
    (.onFailure pub (create-consumer consumer-fn))))

(defn set-on-successful-acquire-event-consumer!
  [^RateLimiter rate-limiter consumer-fn]
  "set a consumer to consume `on-successful-acquire` event which emitted when request is permitted by the rate limiter.
   `consumer-fn` accepts a function which takes no arguments.

   Please note that in `consumer-fn` you can get the rate limiter name and the creation time of the
   consumed event by accessing `*rate-limiter-name*` and `*creation-time*` under this namespace."
  (let [pub (.getEventPublisher rate-limiter)]
    (.onSuccess pub (create-consumer consumer-fn))))

(defn set-on-all-event-consumer!
  [^RateLimiter rate-limiter consumer-fn-map]
  "set a consumer to consume all available events emitted from the rate limiter.
   `consumer-fn-map` accepts a map which contains following key and function pairs:

   * `on-failed-acquire` accepts a function which takes no arguments
   * `on-successful-acquire` accepts a function which takes no arguments

   Please note that in `consumer-fn` you can get the rate limiter name and the creation time of the
   consumed event by accessing `*rate-limiter-name*` and `*creation-time*` under this namespace."
  (let [pub (.getEventPublisher rate-limiter)]
    (.onEvent pub (reify EventConsumer
                    (consumeEvent [_ event]
                      (with-context event
                                    (when-let [consumer-fn (->> (u/case-enum (.getEventType ^RateLimiterEvent event)
                                                                             RateLimiterEvent$Type/FAILED_ACQUIRE
                                                                             :on-failed-acquire

                                                                             RateLimiterEvent$Type/SUCCESSFUL_ACQUIRE
                                                                             :on-successful-acquire)
                                                                (get consumer-fn-map))]
                                      (consumer-fn))))))))