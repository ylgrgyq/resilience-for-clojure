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
