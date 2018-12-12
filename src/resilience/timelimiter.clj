(ns resilience.timelimiter
  (:refer-clojure :exclude [name])
  (:import (java.time Duration)
           (io.github.resilience4j.timelimiter TimeLimiterConfig TimeLimiterConfig$Builder TimeLimiter)))

(defn ^TimeLimiterConfig time-limiter-config [opts]
  (if (empty? opts)
    (throw (IllegalArgumentException. "please provide not empty configuration for time limiter."))
    (let [^TimeLimiterConfig$Builder config (TimeLimiterConfig/custom)]
      (when-let [timeout (:timeout-millis opts)]
        (.timeoutDuration config (Duration/ofMillis timeout)))

      (when (:cancel-running-future opts)
        (.cancelRunningFuture config true))

      (.build config))))

(defn time-limiter [config]
  (let [config (time-limiter-config config)]
    (TimeLimiter/of ^TimeLimiterConfig config)))

;; name configs
;; name registry
;; name registry configs
(defmacro deftimelimiter [name config]
  (let [sym (with-meta (symbol name) {:tag `TimeLimiter})]
    `(def ~sym (time-limiter ~config))))

