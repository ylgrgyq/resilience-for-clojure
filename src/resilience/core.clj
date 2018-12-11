(ns resilience.core
  (:import (io.github.resilience4j.circuitbreaker CircuitBreaker)
           (io.github.resilience4j.retry Retry)
           (io.github.resilience4j.bulkhead Bulkhead)
           (io.github.resilience4j.ratelimiter RateLimiter)
           (io.github.resilience4j.timelimiter TimeLimiter)
           (java.util.function Supplier)))

;; breaker

(defmacro execute-with-circuit-breaker [^CircuitBreaker breaker & body]
  `(.executeCallable ~breaker ^Callable (fn [] (do ~@body))))

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

(defn execute [^Callable f]
  (.call f))

(defmacro to-fn [& body]
  `(fn [] (do ~@body)))

(defmacro with-resilience-family [family-members & body]
  (concat `(->> (to-fn ~@body))
          (map #(let [[k v] %]
                  (list (symbol (str "resilience.core/with-" (name k))) v))
               family-members)
          (list `(execute))))