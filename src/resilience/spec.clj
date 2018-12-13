(ns ^{:doc "Lots of things copied from https://github.com/sunng87/diehard under this namespace"}
  resilience.spec
  (:require [clojure.spec.alpha :as s])
  (:import (java.time Duration)
           (io.github.resilience4j.retry IntervalFunction)))

;; copied from https://github.com/sunng87/diehard and https://groups.google.com/forum/#!topic/clojure/fti0eJdPQJ8
(defmacro only-keys
  [& {:keys [req req-un opt opt-un] :as args}]
  `(s/merge (s/keys ~@(apply concat (vec args)))
            (s/map-of ~(set (concat req
                                    (map (comp keyword name) req-un)
                                    opt
                                    (map (comp keyword name) opt-un)))
                      any?)))

(def is-exception-class?
  #(isa? % Exception))

;; breaker

(s/def :breaker/failure-rate-threshold float?)
(s/def :breaker/wait-millis-in-open-state int?)
(s/def :breaker/ring-buffer-size-in-half-open-state int?)
(s/def :breaker/ring-buffer-size-in-closed-state int?)
(s/def :breaker/record-failure fn?)
(s/def :breaker/record-exceptions
  (s/or :single is-exception-class?
        :multi (s/coll-of is-exception-class?)))
(s/def :breaker/ignore-exceptions
  (s/or :single is-exception-class?
        :multi (s/coll-of is-exception-class?)))
(s/def :breaker/automatic-transfer-from-open-to-half-open? boolean?)

(s/def :breaker/breaker-config
  (only-keys :opt-un [:breaker/failure-rate-threshold :breaker/wait-millis-in-open-state
                      :breaker/ring-buffer-size-in-half-open-state :breaker/ring-buffer-size-in-closed-state
                      :breaker/record-failure :breaker/record-exceptions :breaker/ignore-exceptions
                      :breaker/automatic-transfer-from-open-to-half-open?]))

;; retry

(s/def :retry/max-attempts int?)
(s/def :retry/wait-millis int?)
(s/def :retry/retry-on-result fn?)
(s/def :retry/retry-on-exception fn?)

(s/def :retry/interval-function #(instance? IntervalFunction %))

(s/def :retry/retry-exceptions
  (s/or :single is-exception-class?
        :multi (s/coll-of is-exception-class?)))

(s/def :retry/ignore-exceptions
  (s/or :single is-exception-class?
        :multi (s/coll-of is-exception-class?)))

(s/def :retry/retry-config
  (only-keys :opt-un [:retry/max-attempts :retry/wait-millis
                      :retry/retry-on-result :retry/retry-on-exception :retry/interval-function
                      :retry/retry-exceptions :retry/ignore-exceptions]))

;; rate limiter

(s/def :ratelimiter/timeout-millis int?)
(s/def :ratelimiter/limit-for-period int?)
(s/def :ratelimiter/limit-refresh-period-millis int?)

(s/def :ratelimiter/rate-limiter-config
  (only-keys :opt-un [:ratelimiter/timeout-millis
                      :ratelimiter/limit-for-period
                      :ratelimiter/limit-refresh-period-millis]))

;; bulkhead

(s/def :bulkhead/max-concurrent-calls int?)
(s/def :bulkhead/max-wait-millis int?)

(s/def :bulkhead/bulkhead-config
  (only-keys :opt-un [:bulkhead/max-concurrent-calls
                      :bulkhead/max-wait-millis]))

;; time limiter

(s/def :timelimiter/timeout-millis int?)
(s/def :timelimiter/cancel-running-future boolean?)

(s/def :timelimiter/time-limiter-config
  (only-keys :opt-un [:timelimiter/timeout-millis
                      :timelimiter/cancel-running-future]))