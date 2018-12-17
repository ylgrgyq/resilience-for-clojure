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

(defmacro execute-with-breaker
  "Execute the following codes with the protection
   of a circuit breaker given by `breaker` argument.

   Please try to not put `clojure.core/recur` in `body`
   otherwise you may get an infinite loop easily."
  [breaker & body]
  (let [breaker (vary-meta breaker assoc :tag `CircuitBreaker)
        f (with-meta `(to-fn ~@body) {:tag `Callable})]
    `(.executeCallable ~breaker ~f)))

(defn with-breaker
  "Provide a function `f` which has no arguments and returns a function
   which is decorated by a circuit breaker given by `breaker` argument.
   Usually this is used within `resilience.core/execute` block."
  [^CircuitBreaker breaker f]
  (CircuitBreaker/decorateCallable breaker f))

;; retry

(defmacro execute-with-retry
  "Execute the following codes with the protection
   of a retry policy given by `retry` argument.

   Please try to not put `clojure.core/recur` in `body`
   otherwise you may get an infinite loop easily."
  [retry & body]
  (let [retry (vary-meta retry assoc :tag `Retry)
        f (with-meta `(to-fn ~@body) {:tag `Callable})]
    `(.executeCallable ~retry ~f)))

(defn with-retry
  "Provide a function `f` which has no arguments and returns a function
   which is decorated by a retry policy given by `retry` argument.
   Usually this is used within `resilience.core/execute` block."
  [^Retry retry f]
  (Retry/decorateCallable retry f))

;; bulkhead

(defmacro execute-with-bulkhead
  "Execute the following codes with the protection
   of a bulkhead given by `bulkhead` argument.

   Please try to not put `clojure.core/recur` in `body`
   otherwise you may get an infinite loop easily."
  [^Bulkhead bulkhead & body]
  (let [bulkhead (vary-meta bulkhead assoc :tag `Bulkhead)
        f (with-meta `(to-fn ~@body) {:tag `Callable})]
    `(.executeCallable ~bulkhead ~f)))

(defn with-bulkhead
  "Provide a function `f` which has no arguments and returns a function
   which is decorated by a bulkhead given by `bulkhead` argument.
   Usually this is used within `resilience.core/execute` block."
  [^Bulkhead bulkhead f]
  (Bulkhead/decorateCallable bulkhead f))

;; rate limiter

(defmacro execute-with-rate-limiter
  [rate-limiter & body]
  "Execute the following codes with the protection
   of a rate limiter given by `rate-limiter` argument.

   Please try to not put `clojure.core/recur` in `body`
   otherwise you may get an infinite loop easily."
  (let [ratelimiter (vary-meta rate-limiter assoc :tag `RateLimiter)
        f (with-meta `(to-fn ~@body) {:tag `Callable})]
    `(.executeCallable ~ratelimiter ~f)))

(defn with-rate-limiter
  "Provide a function `f` which has no arguments and returns a function
   which is decorated by a rate limiter given by `rate-limiter` argument.
   Usually this is used within `resilience.core/execute` block."
  [^RateLimiter rate-limiter f]
  (RateLimiter/decorateCallable rate-limiter f))

;; time limiter

(defmacro execute-with-time-limiter
  "Execute the following codes with the protection
   of a time limiter given by `time-limiter` argument.

   Please try to not put `clojure.core/recur` in `body`
   otherwise you may get an infinite loop easily."
  [^TimeLimiter timelimiter & body]
  (let [timelimiter (vary-meta timelimiter assoc :tag `TimeLimiter)]
    `(.executeFutureSupplier ~timelimiter (reify Supplier
                                            (get [_] (do ~@body))))))

(defn with-time-limiter
  "Provide a function `f` which has no arguments and returns a function
   which is decorated by a time limiter given by `time-limiter` argument.
   Usually this is used within `resilience.core/execute` block."
  [^TimeLimiter time-limiter f]
  (TimeLimiter/decorateFutureSupplier time-limiter
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

(defn- recover-from* [exceptions failover-fn wraped-fn]
  (let [wraped-fn (vary-meta wraped-fn assoc :tag `Callable)
        handler (gensym "handler-fn-")
        catch-blocks (if (sequential? exceptions)
                       (let [ex-name-list (repeatedly (count exceptions)
                                                      (partial gensym "ex-"))]
                         (mapv #(list 'catch % %2
                                      (list handler %2))
                               exceptions ex-name-list))
                       `((catch ~exceptions ex#
                           (~handler ex#))))]
    `(let [~handler ~failover-fn]
       (fn []
         (try
           (.call ~wraped-fn)
           ~@catch-blocks)))))

(defmacro recover-from [exceptions failover-fn wraped-fn]
  (recover-from* exceptions failover-fn wraped-fn))

(defmacro recover [failover-fn wraped-fn]
  (recover-from* 'Exception failover-fn wraped-fn))