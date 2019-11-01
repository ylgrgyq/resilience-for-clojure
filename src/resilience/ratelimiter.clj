(ns resilience.ratelimiter
  (:refer-clojure :exclude [name])
  (:require [resilience.util :as u]
            [resilience.spec :as s])
  (:import (java.time Duration)
           (io.github.resilience4j.ratelimiter RateLimiterConfig RateLimiterConfig$Builder
                                               RateLimiterRegistry RateLimiter)
           (io.github.resilience4j.ratelimiter.event RateLimiterEvent RateLimiterEvent$Type)
           (io.github.resilience4j.core EventConsumer)))

(defn ^RateLimiterConfig rate-limiter-config
  "Create a RateLimiterConfig.

  Allowed options are:
  * :writable-stack-trace-enabled
    Enables writable stack traces. When set to false, Exception#getStackTrace()
    returns a zero length array. This may be used to reduce log spam when the circuit breaker
    is open as the cause of the exceptions is already known (the circuit breaker is
    short-circuiting calls).

  * :timeout-millis
    Configures the default wait for permission duration.
    Default value is 5000 milliseconds.

  * :limit-for-period
    Configures the permissions limit for refresh period.
    Count of permissions available during one rate limiter period specified by
    :limit-refresh-period-nanos value.
    Default value is 50.

  * :limit-refresh-period-nanos
    Configures the period of limit refresh.
    After each period rate limiter sets its permissions count to :limit-for-period value.
    Default value is 500 nanoseconds.
   "
  [opts]
  (s/verify-opt-map-keys-with-spec :ratelimiter/rate-limiter-config opts)

  (if (empty? opts)
    (RateLimiterConfig/ofDefaults)
    (let [^RateLimiterConfig$Builder config (RateLimiterConfig/custom)]
      (when-let [stack-trace-enabled (:writable-stack-trace-enabled opts)]
        (.writableStackTraceEnabled config stack-trace-enabled))

      (when-let [timeout (:timeout-millis opts)]
        (.timeoutDuration config (Duration/ofMillis timeout)))

      (when-let [limit (:limit-for-period opts)]
        (.limitForPeriod config (int limit)))

      (when-let [period-nanos (:limit-refresh-period-nanos opts)]
        (.limitRefreshPeriod config (Duration/ofNanos period-nanos)))

      (.build config))))

(defn ^RateLimiterRegistry registry-with-config
  "Create a RateLimiterRegistry with a rate limiter configurations map.

   Please refer to `rate-limiter-config` for allowed key value pairs
   within the rate limiter configuration map."
  [^RateLimiterConfig config]
  (let [c (if (instance? RateLimiterConfig config)
            config
            (rate-limiter-config config))]
    (RateLimiterRegistry/of c)))

(defmacro defregistry
  "Define a RateLimiterRegistry under `name` with a default or custom
   rate limiter configuration.

   Please refer to `rate-limiter-config` for allowed key value pairs
   within the rate limiter configuration map."
  ([name]
    (let [sym (with-meta (symbol name) {:tag `RateLimiterRegistry})]
      `(def ~sym (RateLimiterRegistry/ofDefaults))))
  ([name config]
    (let [sym (with-meta (symbol name) {:tag `RateLimiterRegistry})]
      `(def ~sym
         (let [config# (rate-limiter-config ~config)]
           (registry-with-config config#))))))

(defn get-all-rate-limiters
  "Get all rate limiters registered to this rate limiter registry instance"
  [^RateLimiterRegistry registry]
  (let [heads (.getAllRateLimiters registry)
        iter (.iterator heads)]
    (u/lazy-seq-from-iterator iter)))

(defn ^RateLimiter rate-limiter
  "Create a rate limiter with a `name` and a default or custom rate limiter configuration.

   The `name` argument is only used to register this newly created rate limiter
   to a RateLimiterRegistry. If you don't want to bind this rate limiter with
   a RateLimiterRegistry, the `name` argument is ignored.

   Please refer to `rate-limiter-config` for allowed key value pairs
   within the rate limiter configurations map.

   If you want to register this rate limiter to a RateLimiterRegistry,
   you need to put :registry key with a RateLimiterRegistry in the `config`
   argument. If you do not provide any other configurations, the newly created
   rate limiter will inherit rate limiter configurations from this
   provided RateLimiterRegistry
   Example:
   (rate-limiter my-rate-limiter {:registry my-registry})

   If you want to register this rate limiter to a RateLimiterRegistry
   and you want to use new rate limiter configurations to overwrite the configurations
   inherited from the registered RateLimiterRegistry,
   you need not only provide the :registry key with the RateLimiterRegistry in `config`
   argument but also provide other rate limiter configurations you'd like to overwrite.
   Example:
   (rate-limiter my-rate-limiter {:registry my-registry
                                  :timeout-millis 50})

   If you only want to create a rate limiter and not register it to any
   RateLimiterRegistry, you just need to provide rate limiter configurations in `config`
   argument. The `name` argument is ignored."
  ([^String name] (RateLimiter/ofDefaults name))
  ([^String name config]
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
         (RateLimiter/of name ^RateLimiterConfig config))))))

(defmacro defratelimiter
  "Define a rate limiter under `name` with a default or custom rate limiter configuration.

   Please refer to `rate-limiter-config` for allowed key value pairs
   within the rate limiter configuration.

   If you want to register this rate limiter to a RateLimiterRegistry,
   you need to put :registry key with a RateLimiterRegistry in the `config`
   argument. If you do not provide any other configurations, the newly created
   rate limiter will inherit rate limiter configurations from this
   provided RateLimiterRegistry
   Example:
   (defratelimiter my-rate-limiter {:registry my-registry})

   If you want to register this rate limiter to a RateLimiterRegistry
   and you want to use new rate limiter configurations to overwrite the configurations
   inherited from the registered RateLimiterRegistry,
   you need not only provide the :registry key with the RateLimiterRegistry in `config`
   argument but also provide other rate limiter configurations you'd like to overwrite.
   Example:
   (defratelimiter my-rate-limiter {:registry my-registry
                                    :timeout-millis 50})

   If you only want to create a rate limiter and not register it to any
   RateLimiterRegistry, you just need to provide rate limiter configuration in `config`
   argument without :registry keyword."
  ([name]
   (let [sym (with-meta (symbol name) {:tag `RateLimiter})
         ^String name-in-string (str *ns* "/" name)]
     `(def ~sym (rate-limiter ~name-in-string))))
  ([name config]
   (let [sym (with-meta (symbol name) {:tag `RateLimiter})
         ^String name-in-string (str *ns* "/" name)]
     `(def ~sym (rate-limiter ~name-in-string ~config)))))

(defn ^String name
  "Get the name of this RateLimiter"
  [^RateLimiter limiter]
  (.getName limiter))

(defn ^RateLimiterConfig config
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

(defn acquire-permission
  "Acquires one or the given number of permissions from this rate limiter, blocking until all the
   required permissions are available, or the thread is interrupted. Maximum wait time is set
   by `:timeout-millis` in `rate-limiter-config`.

   If the current thread is interrupted while waiting for a permit then it won't throw
   `InterruptedException`, but its interrupt status will be set.

   Returns true if a permit was acquired and false if waiting `:timeout-millis`
   elapsed before a permit was acquired"
  ([^RateLimiter limiter]
   (.acquirePermission limiter))
  ([^RateLimiter limiter permits]
   (.acquirePermission limiter permits)))

(defn reserve-permission
  "Reserves one or the given number permits from this rate limiter and returns nanoseconds you should
   wait for it. If returned long is negative, it means that you failed to reserve permission,
   possibly your `:timeout-millis` in `rate-limiter-config` is less then time to wait for
   permission."
  ([^RateLimiter limiter]
   (.reservePermission limiter))
  ([^RateLimiter limiter permits]
   (.reservePermission limiter permits)))

(defn wait-for-permission
  "Will wait for one or required number of permits within default timeout duration."
  ([^RateLimiter limiter]
   (RateLimiter/waitForPermission limiter))
  ([^RateLimiter limiter permits]
   (RateLimiter/waitForPermission limiter permits)))

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