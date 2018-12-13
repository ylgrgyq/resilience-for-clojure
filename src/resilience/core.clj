(ns resilience.core
  (:import (io.github.resilience4j.circuitbreaker CircuitBreaker)
           (io.github.resilience4j.retry Retry)
           (io.github.resilience4j.bulkhead Bulkhead)
           (io.github.resilience4j.ratelimiter RateLimiter)
           (io.github.resilience4j.timelimiter TimeLimiter)
           (java.util.function Supplier)))

(defmacro to-fn [& body]
  `(fn [] (do ~@body)))

;; breaker

(defmacro execute-with-breaker [breaker & body]
  (let [breaker (vary-meta breaker assoc :tag `CircuitBreaker)
        f (with-meta `(to-fn ~@body) {:tag `Callable})]
    `(.executeCallable ~breaker ~f)))

(defn with-breaker [^CircuitBreaker breaker f]
  (CircuitBreaker/decorateCallable breaker f))

;; retry

(defmacro execute-with-retry [^Retry retry & body]
  `(.executeCallable ~retry ^Callable (fn [] (do ~@body))))

(defn with-retry [^Retry r f]
  (Retry/decorateCallable r f))

;; bulkhead

(defmacro execute-with-bulkhead [^Bulkhead bulkhead & body]
  `(.executeCallable ~bulkhead ^Callable (fn [] (do ~@body))))

(defn with-bulkhead [^Bulkhead r f]
  (Bulkhead/decorateCallable r f))

;; rate limiter

(defmacro execute-with-rate-limiter [^RateLimiter ratelimiter & body]
  `(.executeCallable ~ratelimiter ^Callable (fn [] (do ~@body))))

(defn with-rate-limiter [^RateLimiter r f]
  (RateLimiter/decorateCallable r f))

;; time limiter

(defmacro execute-with-time-limiter [^TimeLimiter timelimiter & body]
  `(.executeFutureSupplier ~timelimiter (reify Supplier
                                          (get [_] (do ~@body)))))

(defn with-time-limiter [^TimeLimiter r f]
  (TimeLimiter/decorateFutureSupplier r
                                      (reify Supplier
                                        (get [_] (f)))))

;; gather together

(defmacro execute-callable* [f]
  (let [f (vary-meta f assoc :tag `Callable)]
    `(.call ^Callable ~f)))

(defmacro execute
  [execute-body & args]
  `(->> (to-fn ~execute-body)
        ~@args
        execute-callable*))

(defmacro with-resilience-family [family-members & body]
  (let [wrappers (map #(let [[k v] %]
                         (list (symbol (str "resilience.core/with-" (name k))) v))
                      (partition-all 2 family-members))]

    `(->> (to-fn ~@body)
          ~@wrappers
          execute-callable*)))

(defn- recover-from* [exception failover-fn wraped-fn]
  (let [wraped-fn (vary-meta wraped-fn assoc :tag `Callable)]
    `(fn []
       (try
         (.call ~wraped-fn)
         (catch ~exception ex#
           (~failover-fn ex#))))))

(defmacro recover-from [exception failover-fn wraped-fn]
  (recover-from* exception failover-fn wraped-fn))

(defmacro recover [failover-fn wraped-fn]
  (recover-from* 'Exception failover-fn wraped-fn))