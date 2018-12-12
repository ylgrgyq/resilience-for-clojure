(ns resilience.bulkhead
  (:refer-clojure :exclude [name])
  (:require [resilience.util :as u])
  (:import (io.github.resilience4j.bulkhead BulkheadConfig BulkheadConfig$Builder BulkheadRegistry Bulkhead)))

(defn ^BulkheadConfig bulkhead-config [opts]
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



