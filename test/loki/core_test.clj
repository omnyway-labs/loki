(ns loki.core-test
  (:require
   [clojure.test :refer :all]
   [saw.core :as saw]
   [loki.util :as u]
   [loki.core :as loki]
   [loki.athena :as athena]))

(defn setup []
  (loki/init! (System/getenv "LOKI_QUERY_BUCKET")
              (saw/session)))

(deftest render-test
  (is (= {:select [:a :b]
          :from   :foo
          :where  '(= :a "bar")}
         (loki/render {:select [:a :b]
                       :from   :foo
                       :where  '(= :a "{{a}}")}
                      {:a "bar"}))))

(deftest duration-test
  (is (= [:from :to]
         (keys (loki/parse-duration "1d")))))

(deftest ^:integration schema-test
  (setup)
  (is (= {:lat  "double"
          :lon  "double"
          :name "string"
          :pop  "bigint"
          :year "string"}
         (loki/schema :labs :us_cities)))

  (is (= 1
         (count
          (loki/query {:select {:lat  :lat
                                :lon  :lon
                                :name :name}
                       :from   :labs.us-cities
                       :where   '(= :name "{{name}}")}
                      :db :labs
                      :values   {:name "New York"})))))
