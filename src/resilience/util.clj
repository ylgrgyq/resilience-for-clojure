(ns resilience.util
  (:require [clojure.spec.alpha :as s])
  (:import (java.util Iterator)))

(defn lazy-seq-from-iterator [^Iterator iter]
  (if (.hasNext iter)
    (cons (.next iter)
          (lazy-seq (lazy-seq-from-iterator iter)))
    []))

(defn enum->keyword [^Enum e]
  (keyword (.name e)))

;; copied from https://github.com/sunng87/diehard
(defn verify-opt-map-keys-with-spec [spec opt-map]
  (let [parsed (s/conform spec opt-map)]
    (if (= parsed ::s/invalid)
      (throw (ex-info "Invalid input" (s/explain-data spec opt-map)))
      parsed)))