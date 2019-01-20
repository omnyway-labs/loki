(ns loki.core-test
  (:require
   [clojure.test :refer :all]
   [saw.core :as saw]
   [loki.core :as loki]
   [loki.athena :as athena]))

(defn setup []
  (loki/init! (System/getenv "QUERY_BUCKET")
              (saw/session)))

(deftest render-test
  (is (= {:select [:a :b]
          :from   :foo
          :where  '(= :a "bar")}
         (loki/render {:select [:a :b]
                       :from   :foo
                       :where  '(= :a "{{a}}")}
                      {:a "bar"}))))

(deftest ^:integration basic-test
  (setup)
  (is (= 3
         (-> (loki/query :my-db
                         {:select   {:timestamp :datetime
                                     :id :session-id}
                          :from     :orders
                          :order-by [[:timestamp :desc]]
                          :where    {:id "{{order-id}}"}}
                         {:order-id "xyz123"}
                         {:limit 3})
             (count)))))
