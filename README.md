# resilience for clojure

A wrapper over the great library [_Resilience4j_](https://github.com/resilience4j/resilience4j) to provide fault tolerance to your service. Picked a lot of ideas from another great library [_diehard_](https://github.com/sunng87/diehard).

## Usage Examples

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

;; use them all together
(resilience/with-resilience-family 
  {:bulkhead my-bulkhead :retry my-retry :breaker my-breaker}
  (do-something)
  (do-another-thing))
```

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


