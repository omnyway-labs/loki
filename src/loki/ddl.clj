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

(defn- make-part-query [bucket prefix db tb {:keys [year month day]}]
  (->> [(format "ALTER TABLE %s.%s" (name db) (name tb))
        (format "ADD IF NOT EXISTS PARTITION (year='%s',month='%s',day='%s')"
                year month day)
        (format "location 's3://%s/%s/%s/%s/%s/'"
                bucket prefix
                year month day)]
       (interpose " ")
       (apply str)))

(defn add-partition
  ([bucket prefix db tb]
   (add-partition bucket prefix db tb (u/ymd)))
  ([bucket prefix db tb ymd]
   (when ymd
     (let [result (exec
                   (make-part-query bucket prefix db tb ymd))]
       (if (empty? result) ymd result)))))
