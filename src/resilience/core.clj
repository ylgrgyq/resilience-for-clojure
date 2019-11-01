(ns resilience.core
  (:import (io.github.resilience4j.circuitbreaker CircuitBreaker)
           (io.github.resilience4j.retry Retry)
           (io.github.resilience4j.bulkhead Bulkhead ThreadPoolBulkhead)
           (io.github.resilience4j.ratelimiter RateLimiter)
           (io.github.resilience4j.timelimiter TimeLimiter)
           (java.util.function Supplier)))

(defmacro to-fn
  "Wrap body to a function."
  [& body]
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

;; thread pool bulkhead

(defmacro execute-with-thread-pool-bulkhead
  "Execute the following codes with the protection
   of a thread pool bulkhead given by `bulkhead` argument.

   Please try to not put `clojure.core/recur` in `body`
   otherwise you may get an infinite loop easily."
  [^ThreadPoolBulkhead bulkhead & body]
  (let [bulkhead (vary-meta bulkhead assoc :tag `ThreadPoolBulkhead)
        f (with-meta `(to-fn ~@body) {:tag `Callable})]
    `(.executeCallable ~bulkhead ~f)))

(defn with-thread-pool-bulkhead
  "Provide a function `f` which has no arguments and returns a function
   which is decorated by a thread pool bulkhead given by `bulkhead` argument.
   Usually this is used within `resilience.core/execute` block."
  [^ThreadPoolBulkhead bulkhead f]
  (ThreadPoolBulkhead/decorateCallable bulkhead f))

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

;; gather together

(defmacro execute-callable*
  "Internal use only. Sorry for not founding a way to conceal it."
  [f]
  (let [f (vary-meta f assoc :tag `Callable)]
    `(.call ^Callable ~f)))

(defmacro execute
  "Create a function with the `execute-body`, applies any wrapper functions
   in the `args` and then executes it.
   ex: (execute
         (let [data (fetch-data-from db)]
           (save data))
         (with-breaker breaker)
         (with-retry retry-policy)
         (recover-from [CallNotPermittedException ExceptionInfo]
                       (fn [_] (log/error \"circuit breaker open\")))
         (recover (fn [ex] (log/error ex \"unexpected exception happened\"))))"
  [execute-body & args]
  `(->> (to-fn ~execute-body)
        ~@args
        execute-callable*))

(defmacro with-resilience-family
  "Protected forms in `body` with resilience family members.
   ex:  (with-resilience-family
          [:retry retry-policy :breaker breaker :bulkhead bulkhead]
          (let [data (fetch-data-from db)]
            (save data)))
   Please remember to catch Exceptions which may thrown from resilience family members.
   "
  [family-members & body]
  (let [wrappers (map #(let [[k v] %]
                         (list (symbol (str "resilience.core/with-" (name k))) v))
                      (partition-all 2 family-members))]

    `(->> (to-fn ~@body)
          ~@wrappers
          execute-callable*)))

(defn- recover-from* [exceptions failover-fn wraped-fn]
  (let [failover-fn (or failover-fn (fn [_] nil))
        wraped-fn (vary-meta wraped-fn assoc :tag `Callable)
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

(defmacro recover-from
  "Using with `resilience.core/execute` to recover from enumerated exceptions list.
   You can chose to provide a failover function which will be evaluated when one of
   enumerated exception occurred. If no failover function provided, then nil will
   be returned when exception happened."
  ([exceptions wraped-fn]
   (recover-from* exceptions nil wraped-fn))
  ([exceptions failover-fn wraped-fn]
   (recover-from* exceptions failover-fn wraped-fn)))

(defmacro recover
  "Like `resilience.core/recover-from` but will catch any exceptions inherited
   from java.lang.Exception.
   You can chose to provide a failover function which will be evaluated when any
   exceptions occurred. If no failover function provided, then nil will
   be returned any exceptions happened."
  ([wraped-fn]
   (recover-from* 'Exception nil wraped-fn))
  ([failover-fn wraped-fn]
   (recover-from* 'Exception failover-fn wraped-fn)))