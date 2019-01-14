(ns loki.athena
  (:require
   [loki.cred :as cred]
   [loki.util :as u])
  (:import
   [com.amazonaws.services.athena
    AmazonAthenaClientBuilder]
   [com.amazonaws.services.athena.model
    GetQueryExecutionRequest
    GetQueryResultsRequest
    QueryExecutionContext
    QueryExecutionState
    ResultConfiguration
    Row
    StartQueryExecutionRequest
    StartQueryExecutionResult]))

(defonce client (atom nil))

(defonce result-bucket (atom nil))

(defn get-bucket []
  @result-bucket)

(defn- make-client [region]
  (-> (AmazonAthenaClientBuilder/standard)
      (.withCredentials (cred/cred-provider))
      (.withRegion region)
      .build))

(defn- make-exec-context [db]
  (doto (QueryExecutionContext.)
    (.withDatabase (name db))))

(defn- make-result-config []
  (doto (ResultConfiguration.)
    (.withOutputLocation (get-bucket))))

(defn start-query [db query-str]
  (println @client)
  (->> (doto (StartQueryExecutionRequest.)
         (.withQueryString query-str)
         (.withQueryExecutionContext (make-exec-context db))
         (.withResultConfiguration (make-result-config)))
       (.startQueryExecution @client)
       (.getQueryExecutionId)))

(defn as-state [ob]
  (.. ob getQueryExecution getStatus getState))

(defn get-state [query-id]
  (->> (doto (GetQueryExecutionRequest.)
         (.withQueryExecutionId query-id))
       (.getQueryExecution @client)
       (as-state)))

(defn succeeded? [query-id]
  (let [state (get-state query-id)]
    (when (= state "FAILED")
      (throw (Exception. (format "Query Failed: %s" query-id))))
    (=  state "SUCCEEDED")))

(defn as-resultset [x]
  {:token     (.getNextToken x)
   :resultset (.getResultSet x)
   :cols      (->> (.. x getResultSet getResultSetMetadata
                       getColumnInfo)
                   (map #(.getName %)))
   :rows      (.. x getResultSet getRows)})

(defn get-resultset
  ([query-id]
   (->> (doto (GetQueryResultsRequest.)
          (.withQueryExecutionId query-id))
        (.getQueryResults @client)
        (as-resultset)))
  ([query-id token]
   (->> (doto (GetQueryResultsRequest.)
          (.withQueryExecutionId query-id)
          (.withNextToken query-id))
        (.getQueryResults @client)
        (as-resultset))))

(defn process-row [row cols]
  (->> (.getData row)
       (map-indexed vector)
       (map (fn [[k datum]]
              [(keyword (nth cols k)) (.getVarCharValue datum)]))
       (into {})))

(defn process-rows [rows cols]
  (map #(process-row % cols) (rest rows)))

(defn get-resultseq [query-id]
  (loop [{:keys [token rows cols]} (get-resultset query-id)
         acc  []]
    (if-not token
      (->> (process-rows rows cols)
           (conj acc)
           (flatten))
      (recur (get-resultset query-id token)
             (->> (process-rows rows cols)
                  (conj acc))))))

(defn get-results [query-id]
  (u/wait-until #(succeeded? query-id)
                query-id
                6000)
  (get-resultseq query-id))

(defn exec [db query-str]
  (let [query-id (start-query db query-str)]
    (get-results query-id)))

(defn init! [bucket {:keys [region] :as auth}]
  (let [region (or region "us-east-1")]
    (cred/init! auth)
    (reset! result-bucket bucket)
    (reset! client (make-client region))))
