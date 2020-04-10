(ns loki.athena
  (:require
   [saw.core :as saw]
   [loki.util :as u])
  (:import
   [com.amazonaws.services.athena
    AmazonAthenaClientBuilder]
   [com.amazonaws.services.athena.model
    AmazonAthenaException
    GetQueryExecutionRequest
    GetQueryResultsRequest
    QueryExecutionContext
    QueryExecutionState
    ResultConfiguration
    Row
    StartQueryExecutionRequest
    StartQueryExecutionResult
    InvalidRequestException
    ListQueryExecutionsRequest]))

(defonce client (atom nil))

(defonce result-bucket (atom nil))

(defn get-bucket []
  @result-bucket)

(defn- make-client [region]
  (-> (AmazonAthenaClientBuilder/standard)
      (.withCredentials (saw/creds))
      (.withRegion region)
      .build))

(defn- make-exec-context [db]
  (doto (QueryExecutionContext.)
    (.withDatabase (name db))))

(defn- make-result-config []
  (doto (ResultConfiguration.)
    (.withOutputLocation (get-bucket))))

(defn- make-request
  ([query-str]
   (doto (StartQueryExecutionRequest.)
     (.withQueryString query-str)
     (.withResultConfiguration (make-result-config))))
  ([db query-str]
   (doto (StartQueryExecutionRequest.)
     (.withQueryString query-str)
     (.withResultConfiguration (make-result-config))
     (.withQueryExecutionContext (make-exec-context db))))
  ([db query-str request-id]
   (doto (StartQueryExecutionRequest.)
     (.withQueryString query-str)
     (.withClientRequestToken request-id)
     (.withResultConfiguration (make-result-config))
     (.withQueryExecutionContext (make-exec-context db)))))

(defmacro with-query [& body]
  `(try
    ~@body
    (catch InvalidRequestException e#
      {:error-id   :invalid-request
       :error-code (.getAthenaErrorCode e#)
       :msg        (.getErrorMessage e#)})))

(defn- start-query
  ([query-str]
   (with-query
     (->> (make-request query-str)
        (.startQueryExecution @client)
        (.getQueryExecutionId))))
  ([db query-str]
   (with-query
    (->> (make-request db query-str)
         (.startQueryExecution @client)
         (.getQueryExecutionId))))
  ([db query-str request-id]
   (with-query
    (->> (make-request db query-str request-id)
         (.startQueryExecution @client)
         (.getQueryExecutionId)))))

(defn- as-state [ob]
  (let [ex (.. ob getQueryExecution)]
    {:status (.. ex getStatus getState)
     :reason (.. ex getStatus getStateChangeReason)}))

(defn- as-stat [query-id ob]
  (let [ex (.getQueryExecution ob)
        st (.getStatistics ex)]
    {:id         query-id
     :status     (.. ex getStatus getState)
     :reason     (.. ex getStatus getStateChangeReason)
     :scanned-kb (u/bytes->kb
                  (.getDataScannedInBytes st))
     :time-ms    (.getEngineExecutionTimeInMillis st)}))

(defn- get-query-stat [query-id]
  (->> (doto (GetQueryExecutionRequest.)
         (.withQueryExecutionId query-id))
       (.getQueryExecution @client)
       (as-stat query-id)))

(defn list-query-executions []
  (->> (doto (ListQueryExecutionsRequest.)
         (.withMaxResults (int 10)))
       (.listQueryExecutions @client)
       (.getQueryExecutionIds)
       (map get-query-stat)))

(defn- get-state [query-id]
  (->> (doto (GetQueryExecutionRequest.)
         (.withQueryExecutionId query-id))
       (.getQueryExecution @client)
       (as-state)))

(defn- succeeded? [query-id]
  (let [state (get-state query-id)]
    (when (= (:status state) "FAILED")
      (throw (Exception. (format "Query Failed: %s - %s" query-id (:reason state)))))
    (=  (:status state) "SUCCEEDED")))

(defn- as-resultset [rs]
  {:token     (.getNextToken rs)
   :resultset (.getResultSet rs)
   :cols      (->> (.. rs getResultSet
                       getResultSetMetadata
                       getColumnInfo)
                   (map #(.getName %)))
   :rows      (.. rs getResultSet getRows)})

(defn- get-resultset
  ([query-id]
   (->> (doto (GetQueryResultsRequest.)
          (.withQueryExecutionId query-id))
        (.getQueryResults @client)
        (as-resultset)))
  ([query-id token]
   (->> (doto (GetQueryResultsRequest.)
          (.withQueryExecutionId query-id)
          (.withNextToken token))
        (.getQueryResults @client)
        (as-resultset))))

(defn- process-row [row cols]
  (->> (.getData row)
       (map-indexed vector)
       (map (fn [[k datum]]
              [(keyword (nth cols k)) (.getVarCharValue datum)]))
       (into {})))

(defn- process-rows [rows cols]
  (map #(process-row % cols) (rest rows)))

(defn- get-resultseq [query-id]
  (loop [{:keys [token rows cols]} (get-resultset query-id)
         acc  []]
    (if-not token
      (->> (process-rows rows cols)
           (conj acc)
           (flatten))
      (recur (get-resultset query-id token)
             (->> (process-rows rows cols)
                  (conj acc))))))
;; 10min max
(def timeout 600000)

(defn- wait! [query-id]
  (u/wait-until #(succeeded? query-id)
                query-id
                timeout))

(defn- get-results [query-id]
  (try
    (wait! query-id)
    (get-resultseq query-id)
    (catch Exception e
      {:error-id :error
       :msg      (.getMessage e)})))

(defn exec* [param]
  (if (and (map? param) (:error-id param))
    param
    (get-results param)))

(defn exec
  ([query-str]
   (-> (start-query query-str)
       (exec*)))
  ([db query-str]
   (-> (start-query db query-str)
       (exec*)))
  ([db query-str request-id]
   (-> (start-query db query-str request-id)
       (exec*))))

(defn init! [bucket provider]
  (saw/login provider)
  (reset! result-bucket bucket)
  (reset! client (make-client (saw/region))))
