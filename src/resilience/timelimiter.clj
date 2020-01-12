(ns resilience.timelimiter
  (:refer-clojure :exclude [name])
  (:require [resilience.spec :as s]
            [resilience.util :as u])
  (:import (java.time Duration)
           (io.github.resilience4j.timelimiter TimeLimiterConfig TimeLimiterConfig$Builder TimeLimiter TimeLimiterRegistry)
           (io.github.resilience4j.timelimiter.event TimeLimiterEvent TimeLimiterEvent$Type TimeLimiterOnErrorEvent TimeLimiterOnSuccessEvent TimeLimiterOnTimeoutEvent)
           (io.github.resilience4j.core EventConsumer)))

(defn ^TimeLimiterConfig time-limiter-config
  "Create a TimeLimiterConfig.

  Allowed options are:
  * :timeout-millis
    Configures the thread execution timeout.
    Default value is 1 second.

  * :cancel-running-future?
    Configures whether cancel is called on the running future.
    Defaults to true.
   "
  [opts]
  (s/verify-opt-map-keys-with-spec :timelimiter/time-limiter-config opts)

  (if (empty? opts)
    (TimeLimiterConfig/ofDefaults)
    (let [^TimeLimiterConfig$Builder config (TimeLimiterConfig/custom)]
      (when-let [timeout (:timeout-millis opts)]
        (.timeoutDuration config (Duration/ofMillis timeout)))

      (when (:cancel-running-future? opts)
        (.cancelRunningFuture config true))

      (.build config))))

(defn ^TimeLimiterRegistry registry-with-config
  "Create a TimeLimiterRegistry with a time limiter
   configurations map.

   Please refer to `time-limiter-config` for allowed key value pairs
   within the time limiter configuration map."
  [config]
  (let [^TimeLimiterConfig c (if (instance? TimeLimiterConfig config)
                                  config
                                  (time-limiter-config config))]
    (TimeLimiterRegistry/of c)))

(defmacro defregistry
  "Define a TimeLimiterRegistry under `name` with a default or custom
   circuit breaker configuration.

   Please refer to `time-limiter-config` for allowed key value pairs
   within the time limiter configuration map."
  ([name]
   (let [sym (with-meta (symbol name) {:tag `TimeLimiterRegistry})]
     `(def ~sym (TimeLimiterRegistry/ofDefaults))))
  ([name config]
   (let [sym (with-meta (symbol name) {:tag `TimeLimiterRegistry})]
     `(def ~sym
        (let [config# (time-limiter-config ~config)]
          (registry-with-config config#))))))

(defn get-all-time-limiters
  "Get all time limiters registered to this time limiter registry instance"
  [^TimeLimiterRegistry registry]
  (let [heads (.getAllTimeLimiters registry)
        iter (.iterator heads)]
    (u/lazy-seq-from-iterator iter)))

(defn ^TimeLimiter time-limiter
  "Create a time limiter with a default or custom time limiter configuration.

   Please refer to `time-limiter-config` for allowed key value pairs
   within the time limiter configuration."
  ([] (TimeLimiter/ofDefaults))
  ([config]
   (let [config (time-limiter-config config)]
     (TimeLimiter/of ^TimeLimiterConfig config))))

(defmacro deftimelimiter
  "Define a time limiter under `name`.

   Please refer to `time-limiter-config` for allowed key value pairs
   within the time limiter configuration."
  ([name]
   (let [sym (with-meta (symbol name) {:tag `TimeLimiter})]
     `(def ~sym (time-limiter))))
  ([name config]
   (let [sym (with-meta (symbol name) {:tag `TimeLimiter})]
     `(def ~sym (time-limiter ~config)))))

(defn ^String name
  "Get the name of this TimeLimiter"
  [^TimeLimiter limiter]
  (.getName limiter))

(defn ^TimeLimiterConfig config
  "Get the Metrics of this TimeLimiter"
  [^TimeLimiter limiter]
  (.getTimeLimiterConfig limiter))

(defn on-success
  "Records a successful call.
   This method must be invoked when a call was successful."
  [^TimeLimiter limiter]
  (.onSuccess limiter))

(defn on-error
  "Records a failed call. This method must be invoked when a call failed."
  [^TimeLimiter limiter ^Throwable throwable]
  (.onError limiter throwable))

(def ^{:dynamic true
       :doc     "Contextual value represents timer limiter name"}
  *time-limiter-name*)

(def ^{:dynamic true
       :doc "Contextual value represents event create time"}
  *creation-time*)

(defmacro ^{:private true :no-doc true} with-context [abstract-event & body]
  (let [abstract-event (vary-meta abstract-event assoc :tag `TimeLimiterEvent)]
    `(binding [*time-limiter-name* (.getTimeLimiterName ~abstract-event)
               *creation-time*     (.getCreationTime ~abstract-event)]
       ~@body)))

(defmulti ^:private on-event (fn [_ event] (.getEventType ^TimeLimiterEvent event)))

(defmethod on-event TimeLimiterEvent$Type/SUCCESS
  [consumer-fn-map ^TimeLimiterOnSuccessEvent event]
  (with-context event
                (when-let [consumer-fn (get consumer-fn-map :on-success)]
                  (consumer-fn))))

(defmethod on-event TimeLimiterEvent$Type/ERROR
  [consumer-fn-map ^TimeLimiterOnErrorEvent event]
  (with-context event
                (when-let [consumer-fn (get consumer-fn-map :on-error)]
                  (consumer-fn (.getThrowable event)))))

(defmethod on-event TimeLimiterEvent$Type/TIMEOUT
  [consumer-fn-map ^TimeLimiterOnTimeoutEvent event]
  (with-context event
                (when-let [consumer-fn (get consumer-fn-map :on-timeout)]
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
  [^TimeLimiter limiter consumer-fn]
  "set a consumer to consume `on-success` event which emitted when execution success from time limiter.
   `consumer-fn` accepts a function which takes no arguments.

   Please note that in `consumer-fn` you can get the time limiter name and the creation time of the
   consumed event by accessing `*time-limiter-name*` and `*creation-time*` under this namespace."
  (let [pub (.getEventPublisher limiter)]
    (.onSuccess pub (create-consumer :on-success consumer-fn))))

(defn set-on-error-event-consumer!
  [^TimeLimiter limiter consumer-fn]
  "set a consumer to consume `on-error` event which emitted when execution failed from time limiter.
   `consumer-fn` accepts a function which takes a Throwable as argument.

   Please note that in `consumer-fn` you can get the time limiter name and the creation time of the
   consumed event by accessing `*time-limiter-name*` and `*creation-time*` under this namespace."
  (let [pub (.getEventPublisher limiter)]
    (.onError pub (create-consumer :on-error consumer-fn))))

(defn set-on-timeout-event-consumer!
  [^TimeLimiter limiter consumer-fn]
  "set a consumer to consume `on-timeout` event which emitted when execution timeout from time limiter.
   `consumer-fn` accepts a function which takes no arguments.

   Please note that in `consumer-fn` you can get the time limiter name and the creation time of the
   consumed event by accessing `*time-limiter-name*` and `*creation-time*` under this namespace."
  (let [pub (.getEventPublisher limiter)]
    (.onTimeout pub (create-consumer :on-timeout consumer-fn))))

(defn set-on-all-event-consumer!
  [^TimeLimiter limiter consumer-fn-map]
  "set a consumer to consume all available events emitted from time limiter.
   `consumer-fn-map` accepts a map which contains following key and function pairs:

   * `on-success` accepts a function which takes no arguments
   * `on-error` accepts a function which takes Throwable as a argument
   * `on-timeout` accepts a function which takes no arguments

   Please note that in `consumer-fn` you can get the time limiter name and the creation time of the
   consumed event by accessing `*time-limiter-name*` and `*creation-time*` under this namespace."
  (let [pub (.getEventPublisher limiter)]
    (.onEvent pub (create-consumer consumer-fn-map))))
