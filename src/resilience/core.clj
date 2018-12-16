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

(defmacro execute-with-retry [retry & body]
  (let [retry (vary-meta retry assoc :tag `Retry)
        f (with-meta `(to-fn ~@body) {:tag `Callable})]
    `(.executeCallable ~retry ~f)))

(defn with-retry [^Retry r f]
  (Retry/decorateCallable r f))

;; bulkhead

(defmacro execute-with-bulkhead [^Bulkhead bulkhead & body]
  (let [bulkhead (vary-meta bulkhead assoc :tag `Bulkhead)
        f (with-meta `(to-fn ~@body) {:tag `Callable})]
    `(.executeCallable ~bulkhead ~f)))

(defn with-bulkhead [^Bulkhead h f]
  (Bulkhead/decorateCallable h f))

;; rate limiter

(defmacro execute-with-rate-limiter
  [ratelimiter & body]
  "Please take care that you can not put `(recur)` in `body` otherwise you'll got an infinite loop."
  (let [ratelimiter (vary-meta ratelimiter assoc :tag `RateLimiter)
        f (with-meta `(to-fn ~@body) {:tag `Callable})]
    `(.executeCallable ~ratelimiter ~f)))

(defn with-rate-limiter [^RateLimiter r f]
  (RateLimiter/decorateCallable r f))

;; time limiter

(defmacro execute-with-time-limiter [^TimeLimiter timelimiter & body]
  (let [timelimiter (vary-meta timelimiter assoc :tag `TimeLimiter)]
    `(.executeFutureSupplier ~timelimiter (reify Supplier
                                            (get [_] (do ~@body))))))

(defn with-time-limiter [^TimeLimiter t f]
  (TimeLimiter/decorateFutureSupplier t
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

(defmacro recover-from [exception failover-fn wraped-fn]
  (recover-from* exception failover-fn wraped-fn))

(defmacro recover [failover-fn wraped-fn]
  (recover-from* 'Exception failover-fn wraped-fn))