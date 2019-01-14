(ns loki.core
  (:require
   [clojure.walk :refer [postwalk]]
   [clojure.string :as str]
   [stencil.core :as stencil]
   [sqly.core :as sql]
   [loki.athena :as athena]
   [loki.util :as lu]))

(defn list-databases []
  (->> (athena/exec "show databases")
       (map :database_name)))

(defn list-tables [db]
  (->> (athena/exec db (str "show tables in " db))
       (map #(str db "." (:tab_name %)))))

(defn describe [tb]
  (let [stmt (athena/exec (str "show create table " (name tb)))]
    (apply str (interpose "\n" (map :createtab_stmt stmt)))))

(defn assert!
  "Check that all values to be replaced in the query are present in the
  `data` map."
  [template values]
  (doseq [[_ key] (re-seq #"\{\{(\w+-?\w+)\}\}" template)]
    (assert (get values (keyword key))
            (format "Don't know value for {{%s}}" key))))

(defn render
  "Replace template placeholders in query with actual values."
  [query values]
  (postwalk (fn [x]
              (if (string? x)
                (do (assert! x values)
                    (stencil/render-string x values))
                x))
            query))

(defn render-query
  "Takes a query map with template variables, renders it with
  given values and executes the query"
  [query values]
  (-> (render query values)
      (sql/sql)))

(defn exec [db query-str]
  (athena/exec db (format "%s" query-str)))

(defn query
  ([db query-map]
   (query db query-map {} {}))
  ([db query-map values]
   (query db query-map values {}))
  ([db query-map values overrides]
   (let [q (-> (merge query-map overrides)
               (render-query values))]
     (exec db q))))

(defn init! [bucket aws-auth]
  (athena/init! bucket aws-auth))
