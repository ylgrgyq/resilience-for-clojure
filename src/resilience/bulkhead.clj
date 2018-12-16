(ns resilience.bulkhead
  (:refer-clojure :exclude [name])
  (:require [resilience.util :as u]
            [resilience.spec :as s])
  (:import (io.github.resilience4j.bulkhead BulkheadConfig BulkheadConfig$Builder BulkheadRegistry Bulkhead)
           (io.github.resilience4j.bulkhead.event BulkheadEvent BulkheadEvent$Type)
           (io.github.resilience4j.core EventConsumer)))

(defn ^BulkheadConfig bulkhead-config [opts]
  (s/verify-opt-map-keys-with-spec :bulkhead/bulkhead-config opts)

  (if (empty? opts)
    (throw (IllegalArgumentException. "please provide not empty configuration for bulkhead."))
    (let [^BulkheadConfig$Builder config (BulkheadConfig/custom)]
      (when-let [max-calls (:max-concurrent-calls opts)]
        (.maxConcurrentCalls config (int max-calls)))

      (when-let [wait-millis (:max-wait-millis opts)]
        (.maxWaitTime config wait-millis))

      (.build config))))

(defn ^BulkheadRegistry registry-with-config [^BulkheadConfig config]
  (BulkheadRegistry/of config))

(defmacro defregistry [name config]
  (let [sym (with-meta (symbol name) {:tag `BulkheadRegistry})]
    `(def ~sym
       (let [config# (bulkhead-config ~config)]
         (registry-with-config config#)))))

(defn get-all-bulkheads [^BulkheadRegistry registry]
  (let [heads (.getAllBulkheads registry)
        iter (.iterator heads)]
    (u/lazy-seq-from-iterator iter)))

(defn bulkhead [^String name config]
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
        (Bulkhead/of name ^BulkheadConfig config)))))

;; name configs
;; name registry
;; name registry configs
(defmacro defbulkhead [name config]
  (let [sym (with-meta (symbol name) {:tag `Bulkhead})
        ^String name-in-string (str *ns* "/" name)]
    `(def ~sym (bulkhead ~name-in-string ~config))))

(defn name
  "Get the name of this Bulkhead"
  [^Bulkhead breaker]
  (.getName breaker))

(defn config
  "Get the Metrics of this Bulkhead"
  [^Bulkhead breaker]
  (.getBulkheadConfig breaker))

(defn metrics
  "Get the BulkheadConfig of this Bulkhead"
  [^Bulkhead breaker]
  (let [metric (.getMetrics breaker)]
    {:available-concurrent-calls (.getAvailableConcurrentCalls metric)}))

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
  (let [pub (.getEventPublisher bulkhead)]
    (.onCallRejected pub (create-consumer consumer-fn))))

(defn set-on-call-permitted-event-consumer!
  "set a consumer to consume `on-call-permitted` event which emitted when request permitted by bulkhead.
   `consumer-fn` accepts a function which takes no arguments.

   Please note that in `consumer-fn` you can get the bulkhead name and the creation time of the
   consumed event by accessing `*bulkhead-name*` and `*creation-time*` under this namespace."
  [^Bulkhead bulkhead consumer-fn]
  (let [pub (.getEventPublisher bulkhead)]
    (.onCallPermitted pub (create-consumer consumer-fn))))

(defn set-on-call-finished-event-consumer!
  [^Bulkhead bulkhead consumer-fn]
  "set a consumer to consume `on-call-finished` event which emitted when a request finished and leave this bulkhead.
   `consumer-fn` accepts a function which takes no arguments.

   Please note that in `consumer-fn` you can get the bulkhead name and the creation time of the
   consumed event by accessing `*bulkhead-name*` and `*creation-time*` under this namespace."
  (let [pub (.getEventPublisher bulkhead)]
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
