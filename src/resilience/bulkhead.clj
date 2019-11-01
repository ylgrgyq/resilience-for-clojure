(ns resilience.bulkhead
  (:refer-clojure :exclude [name])
  (:require [resilience.util :as u]
            [resilience.spec :as s])
  (:import (io.github.resilience4j.bulkhead BulkheadConfig BulkheadConfig$Builder BulkheadRegistry Bulkhead Bulkhead$EventPublisher Bulkhead$Metrics)
           (io.github.resilience4j.bulkhead.event BulkheadEvent BulkheadEvent$Type)
           (io.github.resilience4j.core EventConsumer)
           (java.time Duration)))

(defn ^BulkheadConfig bulkhead-config
  "Create a BulkheadConfig.

  Allowed options are:
  * :max-concurrent-calls
    Configures the max amount of concurrent calls the bulkhead will support.

  * :max-wait-millis
    Configures a maximum amount of time in ms the calling thread will wait
    to enter the bulkhead. If bulkhead has space available, entry is
    guaranteed and immediate. If bulkhead is full, calling threads will
    contest for space, if it becomes available. :max-wait-millis can be set to 0.

  * :writable-stack-trace-enabled
    Enables writable stack traces. When set to false, Exception#getStackTrace() returns a zero length array.
    This may be used to reduce log spam when the circuit breaker is open as the cause of the exceptions is already
    known (the circuit breaker is short-circuiting calls).

    Note: for threads running on an event-loop or equivalent (rx computation
    pool, etc), setting :max-wait-time to 0 is highly recommended. Blocking an
    event-loop thread will most likely have a negative effect on application
    throughput.
   "
  [opts]
  (s/verify-opt-map-keys-with-spec :bulkhead/bulkhead-config opts)

  (if (empty? opts)
    (BulkheadConfig/ofDefaults)
    (let [^BulkheadConfig$Builder config (BulkheadConfig/custom)]
      (when-let [max-calls (:max-concurrent-calls opts)]
        (.maxConcurrentCalls config (int max-calls)))

      (when-let [wait-millis (:max-wait-millis opts)]
        (.maxWaitDuration config (Duration/ofMillis wait-millis)))

      (when-let [stack-trace-enabled (:writable-stack-trace-enabled opts)]
        (.writableStackTraceEnabled config stack-trace-enabled))

      (.build config))))

(defn ^BulkheadRegistry registry-with-config
  "Create a BulkheadRegistry with a bulkhead configurations map.

   Please refer to `bulkhead-config` for allowed key value pairs
   within the bulkhead configuration map."
  [^BulkheadConfig config]
  (let [c (if (instance? BulkheadConfig config)
            config
            (bulkhead-config config))]
    (BulkheadRegistry/of c)))

(defmacro defregistry
  "Define a BulkheadRegistry under `name` with a default or custom
   bulkhead configuration.

   Please refer to `bulkhead-config` for allowed key value pairs
   within the bulkhead configuration map."
  ([name]
   (let [sym (with-meta (symbol name) {:tag `BulkheadRegistry})]
     `(def ~sym (BulkheadRegistry/ofDefaults))))
  ([name config]
   (let [sym (with-meta (symbol name) {:tag `BulkheadRegistry})]
     `(def ~sym
        (let [config# (bulkhead-config ~config)]
          (registry-with-config config#))))))

(defn get-all-bulkheads
  "Get all bulkhead registered to this bulkhead registry instance"
  [^BulkheadRegistry registry]
  (let [heads (.getAllBulkheads registry)
        iter (.iterator heads)]
    (u/lazy-seq-from-iterator iter)))

(defn ^Bulkhead bulkhead
  "Create a bulkhead with a `name` and a default or custom bulkhead configuration.

   The `name` argument is only used to register this newly created bulkhead
   to a BulkheadRegistry. If you don't want to bind this bulkhead with
   a BulkheadRegistry, the `name` argument is ignored.

   Please refer to `bulkhead-config` for allowed key value pairs
   within the bulkhead configurations map.

   If you want to register this bulkhead to a BulkheadRegistry,
   you need to put :registry key with a BulkheadRegistry in the `config`
   argument. If you do not provide any other configurations, the newly created
   bulkhead will inherit bulkhead configurations from this
   provided BulkheadRegistry
   Example:
   (bulkhead my-bulkhead {:registry my-registry})

   If you want to register this bulkhead to a BulkheadRegistry
   and you want to use new bulkhead configurations to overwrite the configurations
   inherited from the registered BulkheadRegistry,
   you need not only provide the :registry key with the BulkheadRegistry in `config`
   argument but also provide other bulkhead configurations you'd like to overwrite.
   Example:
   (bulkhead my-bulkhead {:registry my-registry
                          :max-wait-millis 50})

   If you only want to create a bulkhead and not register it to any
   BulkheadRegistry, you just need to provide bulkhead configurations in `config`
   argument. The `name` argument is ignored."
  ([^String name] (Bulkhead/ofDefaults name))
  ([^String name config]
   (let [^BulkheadRegistry registry (:registry config)
         config (dissoc config :registry)]
     (cond
       (and registry (not-empty config))
       (let [config (bulkhead-config config)]
         (.bulkhead registry name ^BulkheadConfig config))

       registry
       (.bulkhead registry name)

       :else
       (let [config (bulkhead-config config)]
         (Bulkhead/of name ^BulkheadConfig config))))))

(defmacro defbulkhead
  "Define a bulkhead under `name` with a default or custom configuration.

   Please refer to `bulkhead-config` for allowed key value pairs
   within the bulkhead configuration.

   If you want to register this bulkhead to a BulkheadRegistry,
   you need to put :registry key with a BulkheadRegistry in the `config`
   argument. If you do not provide any other configurations, the newly created
   bulkhead will inherit bulkhead configurations from this
   provided BulkheadRegistry
   Example:
   (defbulkhead my-bulkhead {:registry my-registry})

   If you want to register this bulkhead to a BulkheadRegistry
   and you want to use new bulkhead configurations to overwrite the configurations
   inherited from the registered BulkheadRegistry,
   you need not only provide the :registry key with the BulkheadRegistry in `config`
   argument but also provide other bulkhead configurations you'd like to overwrite.
   Example:
   (defbulkhead my-bulkhead {:registry my-registry
                             :max-wait-millis 50})

   If you only want to create a bulkhead and not register it to any
   BulkheadRegistry, you just need to provide bulkhead configuration in `config`
   argument without :registry keyword."
  ([name]
   (let [sym (with-meta (symbol name) {:tag `Bulkhead})
         ^String name-in-string (str *ns* "/" name)]
     `(def ~sym (bulkhead ~name-in-string))))
  ([name config]
   (let [sym (with-meta (symbol name) {:tag `Bulkhead})
         ^String name-in-string (str *ns* "/" name)]
     `(def ~sym (bulkhead ~name-in-string ~config)))))

(defn change-config!
  "Dynamic bulkhead configuration change. NOTE! New `:max-wait-millis` config won't affect threads
   that are currently waiting for permission."
  [^Bulkhead bulkhead ^BulkheadConfig config]
  (.changeConfig bulkhead config))

(defn try-acquire-permission
  "Acquires a permission to execute a call, only if one is available at the time of invocation.
   If the current thread is interrupted while waiting for a permit then it won't throw
   InterruptedException, but its interrupt status will be set."
  [^Bulkhead bulkhead]
  (.tryAcquirePermission bulkhead))

(defn acquire-permission
  "Acquires a permission to execute a call, only if one is available at the time of invocation
   If the current thread is interrupted while waiting for a permit
   then it won't throw InterruptedException, but its interrupt status will be set."
  [^Bulkhead bulkhead]
  (.acquirePermission bulkhead))

(defn release-permission
  "Releases a permission and increases the number of available permits by one.
   Should only be used when a permission was acquired but not used. Otherwise use
   on-complete(bulkhead) to signal a completed call and release a permission."
  [^Bulkhead bulkhead]
  (.releasePermission bulkhead))

(defn on-complete
  "Records a completed call."
  [^Bulkhead bulkhead]
  (.onComplete bulkhead))

(defn ^String name
  "Get the name of this Bulkhead"
  [^Bulkhead bulkhead]
  (.getName bulkhead))

(defn ^BulkheadConfig config
  "Get the Metrics of this Bulkhead"
  [^Bulkhead bulkhead]
  (.getBulkheadConfig bulkhead))

(defn metrics
  "Get the BulkheadConfig of this Bulkhead"
  [^Bulkhead bulkhead]
  (let [^Bulkhead$Metrics metric (.getMetrics bulkhead)]
    {:available-concurrent-calls (.getAvailableConcurrentCalls metric)
     :max-allowed-concurrent-calls (.getMaxAllowedConcurrentCalls metric)}))

(def ^{:dynamic true
       :doc     "Contextual value represents bulkhead name"}
*bulkhead-name*)

(def ^{:dynamic true
       :doc "Contextual value represents event create time"}
*creation-time*)

(defmacro ^{:private true :no-doc true} with-context [abstract-event & body]
  (let [abstract-event (vary-meta abstract-event assoc :tag `BulkheadEvent)]
    `(binding [*bulkhead-name* (.getBulkheadName ~abstract-event)
               *creation-time* (.getCreationTime ~abstract-event)]
       ~@body)))

(defn- create-consumer [consumer-fn]
  (reify EventConsumer
    (consumeEvent [_ event]
      (with-context event
        (consumer-fn)))))

(defn set-on-call-rejected-event-consumer!
  [^Bulkhead bulkhead consumer-fn]
  "set a consumer to consume `on-call-rejected` event which emitted when request rejected by bulkhead.
   `consumer-fn` accepts a function which takes no arguments.

   Please note that in `consumer-fn` you can get the bulkhead name and the creation time of the
   consumed event by accessing `*bulkhead-name*` and `*creation-time*` under this namespace."
  (let [^Bulkhead$EventPublisher pub (.getEventPublisher bulkhead)]
    (.onCallRejected pub (create-consumer consumer-fn))))

(defn set-on-call-permitted-event-consumer!
  "set a consumer to consume `on-call-permitted` event which emitted when request permitted by bulkhead.
   `consumer-fn` accepts a function which takes no arguments.

   Please note that in `consumer-fn` you can get the bulkhead name and the creation time of the
   consumed event by accessing `*bulkhead-name*` and `*creation-time*` under this namespace."
  [^Bulkhead bulkhead consumer-fn]
  (let [^Bulkhead$EventPublisher pub (.getEventPublisher bulkhead)]
    (.onCallPermitted pub (create-consumer consumer-fn))))

(defn set-on-call-finished-event-consumer!
  [^Bulkhead bulkhead consumer-fn]
  "set a consumer to consume `on-call-finished` event which emitted when a request finished and leave this bulkhead.
   `consumer-fn` accepts a function which takes no arguments.

   Please note that in `consumer-fn` you can get the bulkhead name and the creation time of the
   consumed event by accessing `*bulkhead-name*` and `*creation-time*` under this namespace."
  (let [^Bulkhead$EventPublisher pub (.getEventPublisher bulkhead)]
    (.onCallFinished pub (create-consumer consumer-fn))))

(defn set-on-all-event-consumer!
  [^Bulkhead bulkhead consumer-fn-map]
  "set a consumer to consume all available events emitted from the bulkhead.
   `consumer-fn-map` accepts a map which contains following key and function pairs:

   * `on-call-rejected` accepts a function which takes no arguments
   * `on-call-permitted` accepts a function which takes no arguments
   * `on-call-finished` accepts a function which takes no arguments

   Please note that in `consumer-fn` you can get the bulkhead name and the creation time of the
   consumed event by accessing `*bulkhead-name*` and `*creation-time*` under this namespace."
  (let [pub (.getEventPublisher bulkhead)]
    (.onEvent pub (reify EventConsumer
                    (consumeEvent [_ event]
                      (with-context event
                        (when-let [consumer-fn (->> (u/case-enum (.getEventType ^BulkheadEvent event)
                                                                 BulkheadEvent$Type/CALL_REJECTED
                                                                 :on-call-rejected

                                                                 BulkheadEvent$Type/CALL_PERMITTED
                                                                 :on-call-permitted

                                                                 BulkheadEvent$Type/CALL_FINISHED
                                                                 :on-call-finished)
                                                    (get consumer-fn-map))]
                          (consumer-fn))))))))
