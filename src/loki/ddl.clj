(ns loki.ddl
  (:require
   [clojure.string :as str]
   [loki.util :as u]
   [loki.core :refer [query exec]]))

(defn list-databases []
  (->> (exec "show databases")
       (map :database_name)))

(defn list-tables [db]
  (->> (exec (str "show tables in " db))
       (map #(keyword (:tab_name %)))))

(defn list-partitions [db table]
  (letfn [(as-part [parts]
            (->> (map (fn [{:keys [key value] :as m}]
                        [(keyword key) value]) parts)
                 (into {})))]
    (->> (query {:select {:num   :partition-number
                          :key   :partition-key
                          :value :partition-value}
                 :from   :information_schema.__internal_partitions__
                 :where  '(and
                           (= :table-schema "{{db}}")
                           (= :table-name   "{{table}}"))}
                :db "information_schema"
                :values {:db db :table table})
         (group-by :num)
         (vals)
         (map as-part))))
