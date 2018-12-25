(ns resilience.retry
  (:refer-clojure :exclude [name])
  (:require [resilience.util :as u]
            [resilience.spec :as s])
  (:import (io.github.resilience4j.retry RetryConfig RetryConfig$Builder RetryRegistry Retry IntervalFunction)
           (java.time Duration)
           (java.util.function Predicate)
           (io.github.resilience4j.retry.event RetryEvent RetryEvent$Type)
           (io.github.resilience4j.core EventConsumer)))

(defn ^RetryConfig retry-config
  "Create a RetryConfig.

  Allowed options are:
  * :max-attempts
    Configures max retry times.
    Default value is 3.

  * :wait-millis
    Configures the wait interval time in milliseconds. Between every retry must wait this
    much time then retry next time.
    Must be greater than 10. Default value is 500.

  * :retry-on-result
    Configures a function which evaluates if an result should be retried.
    The function must return true if the result should be retried, otherwise it must return false.

  * :interval-function
    Set a function to modify the waiting interval after a failure.
    By default the interval stays the same.

  * :retry-on-exception
    Configures a function which takes a throwable as argument and evaluates if an exception
    should be retried.
    The function must return true if the exception should count be retried, otherwise it must return false.

    :retry-exceptions
    Configures a list of error classes that are recorded as a failure and thus increase
    the failure rate. Any exception matching or inheriting from one of the list will be retried,
    unless ignored via :ignore-exceptions. Ignoring an exception has priority over retrying an exception.
    Example:
    {:retry-exceptions [Throwable]
     :ignore-exceptions [RuntimeException]}
    would retry all Errors and checked Exceptions, and ignore unchecked.
    For a more sophisticated exception management use the :retry-on-exception configuration.

    :ignore-exceptions
    Configures a list of error classes that are ignored and thus are not retried.
    Any exception matching or inheriting from one of the list will not be retried, even if marked via :retry-exceptions.
    Ignoring an exception has priority over retrying an exception.
    Example:
    {:ignore-exceptions [Throwable]
     :retry-exceptions [Exception]}
    would capture nothing.
    Example:
    {:ignore-exceptions [Exception]
     :retry-exceptions [Throwable]}
    would capture Errors.
    For a more sophisticated exception management use the :retry-on-exception function
   "
  [opts]
  (s/verify-opt-map-keys-with-spec :retry/retry-config opts)
  (if (empty? opts)
    (RetryConfig/ofDefaults)
    (let [^RetryConfig$Builder config (RetryConfig/custom)]
      (when-let [attempts (:max-attempts opts)]
        (.maxAttempts config (int attempts)))
      (when-let [wait-millis (:wait-millis opts)]
        (.waitDuration config (Duration/ofMillis wait-millis)))
      (when-let [f (:retry-on-result opts)]
        (.retryOnResult config (reify Predicate
                                 (test [_ t] (f t)))))

      (when-let [f (:interval-function opts)]
        (.intervalFunction config ^IntervalFunction f))

      (when-let [f (:retry-on-exception opts)]
        (.retryOnException config (reify Predicate
                                    (test [_ t] (f t)))))

      (when-let [exceptions (:retry-exceptions opts)]
        (.retryExceptions config (into-array Class exceptions)))

      (when-let [exceptions (:ignore-exceptions opts)]
        (.ignoreExceptions config (into-array Class exceptions)))
      (.build config))))

(defn ^RetryConfig registry-with-config
  "Create a RetryRegistry with a retry configurations map.

   Please refer to `retry-config` for allowed key value pairs
   within the retry configuration map."
  [^RetryConfig config]
  (let [c (if (instance? RetryConfig config)
            config
            (retry-config config))]
    (RetryRegistry/of c)))

(defmacro defregistry
  "Define a RetryRegistry under `name` with a default or custom
   retry configuration.

   Please refer to `retry-config` for allowed key value pairs
   within the retry configuration map."
  ([name]
   (let [sym (with-meta (symbol name) {:tag `RetryRegistry})]
     `(def ~sym (RetryRegistry/ofDefaults))))
  ([name configs]
   (let [sym (with-meta (symbol name) {:tag `RetryRegistry})]
     `(def ~sym
        (let [configs# (retry-config ~configs)]
          (registry-with-config configs#))))))

(defn get-all-retries
  "Get all retries registered to this retry registry instance"
  [^RetryRegistry registry]
  (let [breakers (.getAllRetries registry)
        iter (.iterator breakers)]
    (u/lazy-seq-from-iterator iter)))

(defn ^Retry retry
  "Create a retry with a `name` and a default or custom retry configuration.

   The `name` argument is only used to register this newly created retry
   to a RetryRegistry. If you don't want to bind this retry with
   a RetryRegistry, the `name` argument is ignored.

   Please refer to `retry-config` for allowed key value pairs
   within the retry configurations map.

   If you want to register this retry to a RetryRegistry,
   you need to put :registry key with a RetryRegistry in the `config`
   argument. If you do not provide any other configurations, the newly created
   retry will inherit retry configurations from this
   provided RetryRegistry
   Example:
   (retry my-retry {:registry my-registry})

   If you want to register this retry to a RetryRegistry
   and you want to use new retry configurations to overwrite the configurations
   inherited from the registered RetryRegistry,
   you need not only provide the :registry key with the RetryRegistry in `config`
   argument but also provide other retry configurations you'd like to overwrite.
   Example:
   (retry my-retry {:registry my-registry
                    :max-attempts 5
                    :wait-millis 5000})

   If you only want to create a retry and not register it to any
   RetryRegistry, you just need to provide retry configurations in `config`
   argument. The `name` argument is ignored."
  ([^String name] (Retry/ofDefaults name))
  ([^String name config]
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
         (Retry/of name ^RetryConfig breaker-config))))))

(defmacro defretry
  "Define a retry under `name` with a default or custom retry configuration.

   Please refer to `retry-config` for allowed key value pairs
   within the retry configuration.

   If you want to register this retry to a RetryRegistry,
   you need to put :registry key with a RetryRegistry in the `config`
   argument. If you do not provide any other configurations, the newly created
   retry will inherit retry configurations from this
   provided RetryRegistry
   Example:
   (defretry my-retry {:registry my-registry})

   If you want to register this retry to a RetryRegistry
   and you want to use new retry configurations to overwrite the configurations
   inherited from the registered RetryRegistry,
   you need not only provide the :registry key with the RetryRegistry in `config`
   argument but also provide other retry configurations you'd like to overwrite.
   Example:
   (defretry my-retry {:registry my-registry
                       :max-attempts 5
                       :wait-millis 5000})

   If you only want to create a retry and not register it to any
   RetryRegistry, you just need to provide retry configuration in `config`
   argument without :registry keyword."
  ([name]
   (let [sym (with-meta (symbol name) {:tag `Retry})
         ^String name-in-string (str *ns* "/" name)]
     `(def ~sym (retry name-in-string))))
  ([name config]
   (let [sym (with-meta (symbol name) {:tag `Retry})
         ^String name-in-string (str *ns* "/" name)]
     `(def ~sym (retry ~name-in-string ~config)))))

(defn ^String name
  "Get the name of this Retry."
  [^Retry retry]
  (.getName retry))

(defn ^RetryConfig config
  "Get the RetryConfig of this Retry"
  [^Retry retry]
  (.getRetryConfig retry))

(defn metrics
  "Get the Metrics of this Retry"
  [^Retry retry]
  (let [metric (.getMetrics retry)]
    {:number-of-successful-calls-without-retry-attempt (.getNumberOfSuccessfulCallsWithoutRetryAttempt metric)
     :number-of-failed-calls-without-retry-attempt (.getNumberOfFailedCallsWithoutRetryAttempt metric)
     :number-of-successful-calls-with-retry-attempt (.getNumberOfSuccessfulCallsWithRetryAttempt metric)
     :number-of-failed-calls-with-retry-attempt (.getNumberOfFailedCallsWithRetryAttempt metric)}))

(def ^{:dynamic true
       :doc     "Contextual value represents retry name"}
*retry-name*)

(def ^{:dynamic true
       :doc "Contextual value represents event create time"}
*creation-time*)

(defmacro ^{:private true :no-doc true} with-context [abstract-event & body]
  (let [abstract-event (vary-meta abstract-event assoc :tag `RetryEvent)]
    `(binding [*retry-name* (.getName ~abstract-event)
               *creation-time* (.getCreationTime ~abstract-event)]
       ~@body)))

(defn- create-consumer [consumer-fn]
  (reify EventConsumer
    (consumeEvent [_ event]
      (with-context event
        (consumer-fn (.getNumberOfRetryAttempts ^RetryEvent event)
                     (.getLastThrowable ^RetryEvent event))))))

(defn set-on-retry-event-consumer!
  [^Retry retry consumer-fn]
  "set a consumer to consume `on-retry` event which emitted when the protected request is failed and retried.
   `consumer-fn` accepts a function which takes `number-of-retry-attemps` and `last-throwable` as arguments.

   Please note that in `consumer-fn` you can get the retry policy name and the creation time of the
   consumed event by accessing `*retry-name*` and `*creation-time*` under this namespace."
  (let [pub (.getEventPublisher retry)]
    (.onRetry pub (create-consumer consumer-fn))))

(defn set-on-success-event-consumer!
  [^Retry retry consumer-fn]
  "set a consumer to consume `on-success` event which emitted when the protected request is success.
   `consumer-fn` accepts a function which takes `number-of-retry-attemps` and `last-throwable` as arguments.

   Please note that in `consumer-fn` you can get the retry policy name and the creation time of the
   consumed event by accessing `*retry-name*` and `*creation-time*` under this namespace."
  (let [pub (.getEventPublisher retry)]
    (.onSuccess pub (create-consumer consumer-fn))))

(defn set-on-error-event-consumer!
  [^Retry retry consumer-fn]
  "set a consumer to consume `on-error` event which emitted when the protected request is failed at last.
   `consumer-fn` accepts a function which takes `number-of-retry-attemps` and `last-throwable` as arguments.

   Please note that in `consumer-fn` you can get the retry policy name and the creation time of the
   consumed event by accessing `*retry-name*` and `*creation-time*` under this namespace."
  (let [pub (.getEventPublisher retry)]
    (.onError pub (create-consumer consumer-fn))))

(defn set-on-ignored-error-event-consumer!
  [^Retry retry consumer-fn]
  "set a consumer to consume `on-ignored-error` event which emitted when the protected request is failed at last
   due to an error which we determine to ignore.
   `consumer-fn` accepts a function which takes `number-of-retry-attemps` and `last-throwable` as arguments.

   Please note that in `consumer-fn` you can get the retry policy name and the creation time of the
   consumed event by accessing `*retry-name*` and `*creation-time*` under this namespace."
  (let [pub (.getEventPublisher retry)]
    (.onIgnoredError pub (create-consumer consumer-fn))))

(defn set-on-all-event-consumer!
  [^Retry retry consumer-fn-map]
  "set a consumer to consume all available events emitted from the retry.
   `consumer-fn-map` accepts a map which contains following key and function pairs:

   * `on-retry` accepts a function which takes `number-of-retry-attemps` and `last-throwable` as arguments
   * `on-success` accepts a function which takes `number-of-retry-attemps` and `last-throwable` as arguments
   * `on-error` accepts a function which takes `number-of-retry-attemps` and `last-throwable` as arguments
   * `on-ignored-error` accepts a function which takes `number-of-retry-attemps` and `last-throwable` as arguments

   Please note that in `consumer-fn` you can get the retry policy name and the creation time of the
   consumed event by accessing `*retry-name*` and `*creation-time*` under this namespace."
  (let [pub (.getEventPublisher retry)]
    (.onEvent pub (reify EventConsumer
                    (consumeEvent [_ event]
                      (with-context event
                                    (when-let [consumer-fn (->> (u/case-enum (.getEventType ^RetryEvent event)
                                                                             RetryEvent$Type/RETRY
                                                                             :on-retry

                                                                             RetryEvent$Type/SUCCESS
                                                                             :on-success

                                                                             RetryEvent$Type/ERROR
                                                                             :on-error

                                                                             RetryEvent$Type/IGNORED_ERROR
                                                                             :on-ignored-error)
                                                                (get consumer-fn-map))]
                                      (consumer-fn))))))))