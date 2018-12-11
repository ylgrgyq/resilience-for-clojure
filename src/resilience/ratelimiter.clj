(ns game-lobby.resilience.ratelimiter
  (:refer-clojure :exclude [name])
  (:require [game-lobby.resilience.util :as u])
  (:import (java.time Duration)
           (io.github.resilience4j.ratelimiter RateLimiterConfig RateLimiterConfig$Builder RateLimiterRegistry RateLimiter)))

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

(defn name [^RateLimiter breaker]
  (.getName breaker))

(defn config [^RateLimiter breaker]
  (.getRateLimiterConfig breaker))

(defn metrics [^RateLimiter breaker]
  (let [metric (.getMetrics breaker)]
    {:number-of-waiting-threads (.getNumberOfWaitingThreads metric)
     :available-permissions (.getAvailablePermissions metric)}))



