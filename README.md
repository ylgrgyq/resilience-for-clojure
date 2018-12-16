# Resilience for Clojure

[![Build Status](https://travis-ci.com/ylgrgyq/resilience-for-clojure.svg?branch=master)](https://travis-ci.com/ylgrgyq/resilience-for-clojure)

A Clojure wrapper over the great library [_Resilience4j_](https://github.com/resilience4j/resilience4j) to provide fault tolerance to your service. Picked a lot of ideas from another great library [_diehard_](https://github.com/sunng87/diehard). Many thanks to both of them.

## Usage Examples

As you may expected, it has [Circuit Breaker](https://github.com/ylgrgyq/resilience-for-clojure#circuit-breaker), [Retry](https://github.com/ylgrgyq/resilience-for-clojure#retry), [Bulkhead](https://github.com/ylgrgyq/resilience-for-clojure#bulkhead), [Rate Limiter](https://github.com/ylgrgyq/resilience-for-clojure#rate-limiter).

And there's also an example to show how to use them all together [here](https://github.com/ylgrgyq/resilience-for-clojure#use-the-resilience-family-all-together) and how to handle exceptions [here](https://github.com/ylgrgyq/resilience-for-clojure#exception-handling).

Further more, it has [examples](https://github.com/ylgrgyq/resilience-for-clojure#registry) to create a `Registry` to collect `Circuit Breaker`, `Retry`, `Rate Limiter` and `Bulkhead` that scattered everywhere and manage them together. And [examples](https://github.com/ylgrgyq/resilience-for-clojure#consume-emitted-events) to set listeners to consume events from your `Circuit Breaker`, `Retry`, `Rate Litmiter` to monitor their state or metrics in real time.

### Circuit Breaker

```clojure
(require '[resilience.breaker :as breaker]
         '[resilience.core :as resilience])

;; define a breaker
;; when over 80% of the last 30 requests failed, the breaker will open for one second
(breaker/defbreaker my-breaker 
  {:failure-rate-threshold 80
   :ring-buffer-size-in-closed-state 30
   :wait-millis-in-open-state 1000})

;; do something with the protection of the breaker
(resilience/execute-with-breaker my-breaker
  (do-something)
  (do-another-thing))
```

### Retry

```clojure
(require '[resilience.retry :as retry]
         '[resilience.core :as resilience])

;; define a retry
;; retry the request at most 3 times with a one second interval when any exception occurs
(retry/defretry my-retry
  {:max-attempts 3
   :wait-millis 1000})

;; do something with the protection of the retry
(resilience/execute-with-retry my-retry
  (do-something)
  (do-another-thing))
```

### Bulkhead
```clojure
(require '[resilience.bulkhead :as bulkhead]
         '[resilience.core :as resilience])

;; define a bulkhead
;; allow at most 3 parallel executions 
(bulkhead/defbulkhead my-bulkhead
  {:max-concurrent-calls 3
   :max-wait-millis 1000})

;; do something with the protection of the bulkhead
(resilience/execute-with-bulkhead my-bulkhead
  (do-something)
  (do-another-thing))
```

### Rate Limiter
```clojure
(require '[resilience.ratelimiter :as ratelimiter]
         '[resilience.core :as resilience])

;; define a ratelimiter
;; allow at most 10 requests per second
(ratelimiter/defratelimiter my-ratelimiter
  {:limit-for-period 10
   :limit-refresh-period-millis 1000
   :timeout-millis 1000})

;; do something with the protection of the ratelimiter
(resilience/execute-with-ratelimiter my-ratelimiter
  (do-something)
  (do-another-thing))
```

### Use the resilience family all together

```clojure
(require '[resilience.breaker :as breaker]
         '[resilience.bulkhead :as bulkhead]
         '[resilience.retry :as retry]
         '[resilience.ratelimiter :as ratelimiter]
         '[resilience.core :as resilience])

;; define a breaker
(breaker/defbreaker my-breaker 
  {:failure-rate-threshold 80
   :ring-buffer-size-in-closed-state 30
   :wait-millis-in-open-state 1000})

;; define a retry
(retry/defretry my-retry
  {:max-attempts 3
   :wait-millis 1000})

;; define a bulkhead
(bulkhead/defbulkhead my-bulkhead
  {:max-concurrent-calls 3
   :max-wait-millis 1000})

;; define a ratelimiter
(ratelimiter/defratelimiter my-ratelimiter
  {:limit-for-period 10
   :limit-refresh-period-millis 1000
   :timeout-millis 1000})

;; use them all together
(resilience/with-resilience-family 
  [:retry my-retry :breaker my-breaker :bulkhead my-bulkhead :rate-limiter my-ratelimiter]
  (do-something)
  (do-another-thing))

;; an alternative way
(resilience/execute
  (do (do-something)
      (do-another-thing))
  (resilience/with-retry my-retry)
  (resilience/with-breaker my-breaker)
  (resilience/with-bulkhead my-bulkhead)
  (resilience/with-rate-limiter my-ratelimiter))
```

Please note that the second parameter passed to `with-resilience-family` is a list. It means the order of the family members is matters. Such as usually you need to put retry policy before circuit breaker to insure that when the circuit breaker is open you do not retry further in vain. And the same is for using `execute` in which you need to mind the order of those `with-*` forms.

### Exception Handling

What we missing until now is how to handle exceptions thrown by resilience family members. Most of resilience family members have their corresponding exceptions which will be thrown when certain conditions match. Such as when circuit breaker is open, the subsequent requests will trigger `CircuitBreakerOpenException` in circuit breaker. And for bulkhead, when bulkhead is full, the subsequent parallel requests will trigger `BulkheadFullException` in bulkhead. What is matters here is that sometimes you need to handle all these exceptions respectively which may force you to add `try ... catch` blocks to protect your codes then make your codes not as concise as above examples. After adding exception handling stuff, the example before looks like this:

```clojure
(try
  (resilience/with-resilience-family
    [:retry my-retry :breaker my-breaker :bulkhead my-bulkhead :rate-limiter my-ratelimiter]
    (do-something)
    (do-another-thing))
  (catch CircuitBreakerOpenException ex
    (log-circuit-breaker-open-and-return-a-fallback-value ex))
  (catch BulkheadFullException ex
    (log-bulkhead-full-and-return-a-fallback-value ex))
  (catch RequestNotPermitted ex
    (log-request-not-permitted-and-return-a-fallback-value ex))
  (catch Exception ex
    (log-unexpected-exception-and-return-a-fallback-value ex)))
```

If you don't like to use `try ... catch` block, you can choose to use `execute` with `recover` form like this:

```clojure
(resilience/execute
  (do (do-something)
      (do-another-thing))
  (resilience/with-retry my-retry)
  (resilience/with-breaker my-breaker)
  (resilience/with-bulkhead my-bulkhead)
  (resilience/with-rate-limiter my-ratelimiter)
  (resilience/recover-from CircuitBreakerOpenException log-circuit-breaker-open-and-return-a-fallback-value)
  (resilience/recover-from BulkheadFullException log-bulkhead-full-and-return-a-fallback-value)
  (resilience/recover-from RequestNotPermitted log-request-not-permitted-and-return-a-fallback-value)
  (resilience/recover log-unexpected-exception-and-return-a-fallback-value))
```

There's not much difference between them. Usually you can just choose any one style of them and stick on it.

### Registry

All of Circuit Breaker, Retry, Rate Limiter can register to their corresponding `Registry` and use `Registry` as a management tool. Take Circuit Breaker as an example. You can use `Circuit Breaker Registry` to create, retrieve and monitor all your `Circuit Breaker` instances registered to it. Like: 

```clojure
(require '[resilience.breaker :as breaker]
         '[resilience.core :as resilience])

;; define a Circuit Breaker Registry
(breaker/defregistry my-breaker-registry
  {:failure-rate-threshold           80
   :wait-millis-in-open-state        1000
   :ring-buffer-size-in-closed-state 30})

;; define a breaker and register it to the Circuit Breaker Registry 
;; the new breaker will inherit configuration from the Registry
(breaker/defbreaker my-registered-breaker {:registry my-breaker-registry})

;; then we can use the circuit breaker as usual
(resilience/execute-with-breaker my-breaker
  (do-something)
  (do-another-thing))

;; and from the Registry we can get all circuit breakers registered to it
(doseq [cb (breaker/get-all-breakers my-breaker-registry)]
  (let [state (breaker/state cb)
        metrics (breaker/metrics cb)]
    (do-some-monitoring-stuff cb state metrics)))
```

### Consume emitted events

`CircuitBreaker`, `RateLimiter`, and `Retry` components emit a stream of events which can be consumed.

Still take `CircuitBreaker` as an example. 

```clojure
(require '[resilience.breaker :as breaker]
         '[resilience.core :as resilience])
(import '(resilience.breaker CircuitBreakerEventListener))

;; define a breaker like before
(breaker/defbreaker my-breaker 
  {:failure-rate-threshold 80
   :ring-buffer-size-in-closed-state 30
   :wait-millis-in-open-state 1000})

;; then you can add listener to the circuit breaker
;; listen on success event
(breaker/listen-on-success my-breaker
  (reify CircuitBreakerEventListener
    (on-success [this name elapsed-millis]
      (log/info ...))))

;; listen on error event
(breaker/listen-on-error my-breaker
  (reify CircuitBreakerEventListener
    (on-error [this breaker-name throwable elapsed-millis]
      (log/info ...))))

;; listen on state transition event
(breaker/listen-on-state-transition my-breaker
  (reify CircuitBreakerEventListener
    (on-state-transition [this breaker-name from-state to-state]
      (log/info ...))))

;; I'm not going to list all the available events you can listen on
;; please check the doc or the codes to get more details
...

;; and you can also create a big listener to catch all these events
(let [listener (reify CircuitBreakerEventListener
                 (on-success [this breaker-name elapsed-millis]
                   (log/info ...))
                 (on-error [this breaker-name throwable elapsed-millis]
                   (log/info ...))
                 (on-state-transition [this breaker-name from-state to-state]
                   (log/info ...))
                 ...)]
  ;; you can listen on events separately
  (breaker/listen-on-success my-breaker listener)
  (breaker/listen-on-error my-breaker listener)
  (breaker/listen-on-state-transition my-breaker listener)
  ...

  ;; or listen all these events on one call
  (breaker/listen-on-all-events my-breaker listener))
```

# License

Copyright 2018 Rui Guo. Released under the [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0.html).
