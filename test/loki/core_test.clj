(ns loki.core-test
  (:require
   [clojure.test :refer :all]
   [loki.core :as loki]
   [loki.athena :as athena]))


(def auth {:auth-type :profile
           :profile (System/getenv "AWS_PROFILE")})


(use-fixtures :once
  (fn [f]
    (loki/init! (System/getenv "QUERY_BUCKET")
                auth)
    (f)))


(deftest ^:integration basic-test
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
