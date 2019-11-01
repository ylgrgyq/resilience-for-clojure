(ns resilience.thread-pool-bulkhead
  (:refer-clojure :exclude [name])
  (:require [resilience.util :as u]
            [resilience.spec :as s])
  (:import (io.github.resilience4j.bulkhead ThreadPoolBulkheadConfig ThreadPoolBulkheadConfig$Builder
                                            ThreadPoolBulkheadRegistry ThreadPoolBulkhead)
           (io.github.resilience4j.bulkhead.event BulkheadEvent BulkheadEvent$Type)
           (io.github.resilience4j.core EventConsumer)
           (java.util.concurrent CompletionStage)
           (java.time Duration)))

(defn ^ThreadPoolBulkheadConfig thread-pool-bulkhead-config
  "Create a ThreadPoolBulkheadConfig.

  Allowed options are:
  * :max-thread-pool-size
    Configures the max thread pool size.

  * :core-thread-pool-size
    Configures the core thread pool size.

  * :queue-capacity
    Configures the capacity of the queue.

  * :keep-alive-millis
    When the number of threads is greater than
    the core, this is the maximum time in milliseconds that excess idle threads
    will wait for new tasks before terminating.

  * :writable-stack-trace-enabled
    Enables writable stack traces. When set to false, Exception#getStackTrace() returns a zero length array.
    This may be used to reduce log spam when the circuit breaker is open as the cause of the exceptions is already
    known (the circuit breaker is short-circuiting calls).
   "
  [opts]
  (s/verify-opt-map-keys-with-spec :thread-pool-bulkhead/bulkhead-config opts)

  (if (empty? opts)
    (ThreadPoolBulkheadConfig/ofDefaults)
    (let [^ThreadPoolBulkheadConfig$Builder config (ThreadPoolBulkheadConfig/custom)]
      (when-let [max-thread-pool-size (:max-thread-pool-size opts)]
        (.maxThreadPoolSize config (int max-thread-pool-size)))

      (when-let [core-size (:core-thread-pool-size opts)]
        (.coreThreadPoolSize config (int core-size)))

      (when-let [queue-capacity (:queue-capacity opts)]
        (.queueCapacity config (int queue-capacity)))

      (when-let [keep-alive (:keep-alive-millis opts)]
        (.keepAliveDuration config (Duration/ofMillis keep-alive)))

      (when-let [stack-trace-enabled (:writable-stack-trace-enabled opts)]
        (.writableStackTraceEnabled config stack-trace-enabled))

      (.build config))))

(defn ^ThreadPoolBulkheadRegistry registry-with-config
  "Create a ThreadPoolBulkheadRegistry with a configurations map.

   Please refer to `thread-pool-bulkhead-config` for allowed key value pairs
   within the configuration map."
  [^ThreadPoolBulkheadConfig config]
  (let [c (if (instance? ThreadPoolBulkheadConfig config)
            config
            (thread-pool-bulkhead-config config))]
    (ThreadPoolBulkheadRegistry/of c)))

(defmacro defregistry
  "Define a ThreadPoolBulkheadRegistry under `name` with a default or custom
   thread pool bulkhead configuration.

   Please refer to `thread-pool-bulkhead-config` for allowed key value pairs
   within the configuration map."
  ([name]
   (let [sym (with-meta (symbol name) {:tag `ThreadPoolBulkheadRegistry})]
     `(def ~sym (ThreadPoolBulkheadRegistry/ofDefaults))))
  ([name config]
   (let [sym (with-meta (symbol name) {:tag `ThreadPoolBulkheadRegistry})]
     `(def ~sym
        (let [config# (thread-pool-bulkhead-config ~config)]
          (registry-with-config config#))))))

(defn get-all-thread-pool-bulkheads
  "Get all thread pool bulkhead registered to this thread pool bulkhead registry instance"
  [^ThreadPoolBulkheadRegistry registry]
  (let [heads (.getAllBulkheads registry)
        iter (.iterator heads)]
    (u/lazy-seq-from-iterator iter)))

(defn ^ThreadPoolBulkhead bulkhead
  "Create a thread pool bulkhead with a `name` and a default or custom configurations.

   The `name` argument is only used to register this newly created thread pool bulkhead
   to a ThreadPoolBulkheadRegistry. If you don't want to bind this bulkhead with
   a ThreadPoolBulkheadRegistry, the `name` argument is ignored.

   Please refer to `thread-pool-bulkhead-config` for allowed key value pairs
   within the bulkhead configurations map.

   If you want to register this thread pool bulkhead to a ThreadPoolBulkheadRegistry,
   you need to put :registry key with a ThreadPoolBulkheadRegistry in the `config`
   argument. If you do not provide any other configurations, the newly created
   bulkhead will inherit bulkhead configurations from this
   provided ThreadPoolBulkheadRegistry
   Example:
   (bulkhead my-bulkhead {:registry my-registry})

   If you want to register this thread pool bulkhead to a ThreadPoolBulkheadRegistry
   and you want to use new bulkhead configurations to overwrite the configurations
   inherited from the registered ThreadPoolBulkheadRegistry,
   you need not only provide the :registry key with the ThreadPoolBulkheadRegistry in `config`
   argument but also provide other bulkhead configurations you'd like to overwrite.
   Example:
   (bulkhead my-bulkhead {:registry my-registry
                          :max-wait-millis 50})

   If you only want to create a bulkhead and not register it to any
   ThreadPoolBulkheadRegistry, you just need to provide bulkhead configurations in `config`
   argument. The `name` argument is ignored."
  ([^String name] (ThreadPoolBulkhead/ofDefaults name))
  ([^String name config]
   (let [^ThreadPoolBulkheadRegistry registry (:registry config)
         config (dissoc config :registry)]
     (cond
       (and registry (not-empty config))
       (let [config (thread-pool-bulkhead-config config)]
         (.bulkhead registry name ^ThreadPoolBulkheadConfig config))

       registry
       (.bulkhead registry name)

       :else
       (let [config (thread-pool-bulkhead-config config)]
         (ThreadPoolBulkhead/of name ^ThreadPoolBulkheadConfig config))))))

(defmacro defbulkhead
  "Define a thread pool bulkhead under `name` with a default or custom configurations.

   Please refer to `thread-pool-bulkhead-config` for allowed key value pairs
   within the bulkhead configuration.

   If you want to register this bulkhead to a ThreadPoolBulkheadRegistry,
   you need to put :registry key with a ThreadPoolBulkheadRegistry in the `config`
   argument. If you do not provide any other configurations, the newly created
   bulkhead will inherit bulkhead configurations from this
   provided ThreadPoolBulkheadRegistry
   Example:
   (defbulkhead my-bulkhead {:registry my-registry})

   If you want to register this bulkhead to a ThreadPoolBulkheadRegistry
   and you want to use new bulkhead configurations to overwrite the configurations
   inherited from the registered ThreadPoolBulkheadRegistry,
   you need not only provide the :registry key with the ThreadPoolBulkheadRegistry in `config`
   argument but also provide other bulkhead configurations you'd like to overwrite.
   Example:
   (defbulkhead my-bulkhead {:registry my-registry
                             :max-wait-millis 50})

   If you only want to create a bulkhead and not register it to any
   ThreadPoolBulkheadRegistry, you just need to provide bulkhead configuration in `config`
   argument without :registry keyword."
  ([name]
   (let [sym (with-meta (symbol name) {:tag `ThreadPoolBulkhead})
         ^String name-in-string (str *ns* "/" name)]
     `(def ~sym (bulkhead ~name-in-string))))
  ([name config]
   (let [sym (with-meta (symbol name) {:tag `ThreadPoolBulkhead})
         ^String name-in-string (str *ns* "/" name)]
     `(def ~sym (bulkhead ~name-in-string ~config)))))

(defn ^String name
  "Get the name of this Bulkhead"
  [^ThreadPoolBulkhead bulkhead]
  (.getName bulkhead))

(defn ^ThreadPoolBulkheadConfig config
  "Get the Metrics of this Bulkhead"
  [^ThreadPoolBulkhead bulkhead]
  (.getBulkheadConfig bulkhead))

(defn ^CompletionStage submit-callable
  "Submits a value-returning task for execution and returns a Future representing the pending
   results of the task."
  [^ThreadPoolBulkhead bulkhead ^Callable callable]
  (.submit bulkhead callable))

(defn submit-runnable
  "Submits a task for execution."
  [^ThreadPoolBulkhead bulkhead ^Runnable runnable]
  (.submit bulkhead runnable))

(defn metrics
  "Get the ThreadPoolBulkheadConfig of this Bulkhead"
  [^ThreadPoolBulkhead bulkhead]
  (let [metric (.getMetrics bulkhead)]
    {:core-thread-pool-size (.getCoreThreadPoolSize metric)
     :thread-pool-size (.getThreadPoolSize metric)
     :max-thread-pool-size (.getMaximumThreadPoolSize metric)
     :queue-depth (.getQueueDepth metric)
     :queue-capacity (.getQueueCapacity metric)
     :remaining-queue-capacity (.getRemainingQueueCapacity metric)}))

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
  [^ThreadPoolBulkhead bulkhead consumer-fn]
  "set a consumer to consume `on-call-rejected` event which emitted when request rejected by bulkhead.
   `consumer-fn` accepts a function which takes no arguments.

   Please note that in `consumer-fn` you can get the bulkhead name and the creation time of the
   consumed event by accessing `*bulkhead-name*` and `*creation-time*` under this namespace."
  (let [pub (.getEventPublisher bulkhead)]
    (.onCallRejected pub (create-consumer consumer-fn))))

(defn set-on-call-permitted-event-consumer!
  "set a consumer to consume `on-call-permitted` event which emitted when request permitted by bulkhead.
   `consumer-fn` accepts a function which takes no arguments.

   Please note that in `consumer-fn` you can get the bulkhead name and the creation time of the
   consumed event by accessing `*bulkhead-name*` and `*creation-time*` under this namespace."
  [^ThreadPoolBulkhead bulkhead consumer-fn]
  (let [pub (.getEventPublisher bulkhead)]
    (.onCallPermitted pub (create-consumer consumer-fn))))

(defn set-on-call-finished-event-consumer!
  [^ThreadPoolBulkhead bulkhead consumer-fn]
  "set a consumer to consume `on-call-finished` event which emitted when a request finished and leave this bulkhead.
   `consumer-fn` accepts a function which takes no arguments.

   Please note that in `consumer-fn` you can get the bulkhead name and the creation time of the
   consumed event by accessing `*bulkhead-name*` and `*creation-time*` under this namespace."
  (let [pub (.getEventPublisher bulkhead)]
    (.onCallFinished pub (create-consumer consumer-fn))))

(defn set-on-all-event-consumer!
  [^ThreadPoolBulkhead bulkhead consumer-fn-map]
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
