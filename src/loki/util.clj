(ns loki.util
  (:require
   [clojure.edn :as edn]
   [clj-time.core :as time]
   [clj-time.format :as tf]
   [clojure.string :as str]
   [clojure.java.io :as io]
   [clojure.core.match :refer [match]])
  (:import
   [java.io PushbackReader]
   [com.google.common.util.concurrent
    RateLimiter]))

(defn canonicalize-duration-units
  [units]
  (case units
    ("d" "days") :days
    ("m" "months") :months))

(defn parse-duration
  [duration-str]
  (match (re-find #"(\d+)(\w+)" duration-str)
         [_ duration-num duration-units]
         {:num   (Integer/parseInt duration-num)
          :units (canonicalize-duration-units duration-units)}

         _ (throw (ex-info "Could not parse duration"
                           {:duration duration-str}))))

(def units-fn
  {:days   time/days
   :months time/months})

(defn duration->from-and-to
  [{:keys [num units] :as duration}]
  (let [now  (time/plus (time/now) (time/days 1))
        to   (tf/unparse (tf/formatter :date) now)
        from (->> (time/minus now ((units-fn units) num))
                  (tf/unparse (tf/formatter :date)))]
    {:from from
     :to   to}))

 (defn time-up? [tag start duration-milli]
  (< duration-milli (- (System/currentTimeMillis) start)))

(defn wait-until
  ([pred tag]
   (wait-until pred tag 10000))
  ([pred tag duration]
   (let [start (System/currentTimeMillis)
         rate-limiter (RateLimiter/create 1.0)
         pred-fn #(let [p (when pred
                            (pred))
                        timeout (time-up? tag start duration)]
                    (and (not p)
                         (not timeout)))]
     (while (pred-fn)
       (.acquire rate-limiter)))))

(defn omethods [obj]
  (map #(.getName %) (-> obj class .getMethods)))

(defn assert!
  "Check that all values to be replaced in the query are present in the
  `data` map."
  [template values]
  (doseq [[_ key] (re-seq #"\{\{(\w+-?\w+)\}\}" template)]
    (assert (get values (keyword key))
            (format "Don't know value for {{%s}}" key))))
