-- DBSQL Consumption Analysis by Consumer Type (Last 6 Months)
-- This query analyzes DBSQL consumption patterns by identifying different consumer types
-- (BI tools, Databricks Apps, Python/JDBC clients, etc.)

WITH query_consumption AS (
  SELECT
    -- Identify consumer type from client_application field
    CASE
      -- BI Tools
      WHEN LOWER(client_application) LIKE '%tableau%' THEN 'BI - Tableau'
      WHEN LOWER(client_application) LIKE '%powerbi%' OR LOWER(client_application) LIKE '%power bi%' THEN 'BI - Power BI'
      WHEN LOWER(client_application) LIKE '%looker%' THEN 'BI - Looker'
      WHEN LOWER(client_application) LIKE '%qlik%' THEN 'BI - Qlik'
      WHEN LOWER(client_application) LIKE '%sisense%' THEN 'BI - Sisense'
      WHEN LOWER(client_application) LIKE '%thoughtspot%' THEN 'BI - ThoughtSpot'
      WHEN LOWER(client_application) LIKE '%metabase%' THEN 'BI - Metabase'

      -- Databricks Native
      WHEN LOWER(client_application) LIKE '%databricks app%' OR LOWER(client_application) LIKE '%dash%' THEN 'Databricks Apps'
      WHEN LOWER(client_application) LIKE '%notebook%' THEN 'Databricks Notebooks'
      WHEN LOWER(client_application) LIKE '%sql editor%' OR LOWER(client_application) LIKE '%databricks sql%' THEN 'Databricks SQL Editor'

      -- Programmatic Access
      WHEN LOWER(client_application) LIKE '%python%' OR LOWER(client_application) LIKE '%databricks-sql-connector%' THEN 'Python SDK'
      WHEN LOWER(client_application) LIKE '%jdbc%' THEN 'JDBC Client'
      WHEN LOWER(client_application) LIKE '%odbc%' THEN 'ODBC Client'

      -- Other/Unknown
      WHEN client_application IS NULL OR client_application = '' THEN 'Unknown/Unspecified'
      ELSE 'Other - ' || client_application
    END AS consumer_type,

    warehouse_id,
    COUNT(*) AS query_count,
    SUM(duration) / 1000.0 AS total_execution_time_sec,
    AVG(duration) / 1000.0 AS avg_execution_time_sec,
    SUM(CASE WHEN status = 'FINISHED' THEN 1 ELSE 0 END) AS successful_queries,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_queries,

    -- Approximate DBU consumption (if compute_resources field is available)
    -- Note: This is an approximation based on execution time
    -- Actual DBU consumption requires system.billing.usage table
    SUM(duration) / 1000.0 / 3600.0 AS approx_compute_hours

  FROM system.query.history
  WHERE start_time >= current_date() - INTERVAL 6 MONTHS
    AND warehouse_id IS NOT NULL
  GROUP BY
    CASE
      WHEN LOWER(client_application) LIKE '%tableau%' THEN 'BI - Tableau'
      WHEN LOWER(client_application) LIKE '%powerbi%' OR LOWER(client_application) LIKE '%power bi%' THEN 'BI - Power BI'
      WHEN LOWER(client_application) LIKE '%looker%' THEN 'BI - Looker'
      WHEN LOWER(client_application) LIKE '%qlik%' THEN 'BI - Qlik'
      WHEN LOWER(client_application) LIKE '%sisense%' THEN 'BI - Sisense'
      WHEN LOWER(client_application) LIKE '%thoughtspot%' THEN 'BI - ThoughtSpot'
      WHEN LOWER(client_application) LIKE '%metabase%' THEN 'BI - Metabase'
      WHEN LOWER(client_application) LIKE '%databricks app%' OR LOWER(client_application) LIKE '%dash%' THEN 'Databricks Apps'
      WHEN LOWER(client_application) LIKE '%notebook%' THEN 'Databricks Notebooks'
      WHEN LOWER(client_application) LIKE '%sql editor%' OR LOWER(client_application) LIKE '%databricks sql%' THEN 'Databricks SQL Editor'
      WHEN LOWER(client_application) LIKE '%python%' OR LOWER(client_application) LIKE '%databricks-sql-connector%' THEN 'Python SDK'
      WHEN LOWER(client_application) LIKE '%jdbc%' THEN 'JDBC Client'
      WHEN LOWER(client_application) LIKE '%odbc%' THEN 'ODBC Client'
      WHEN client_application IS NULL OR client_application = '' THEN 'Unknown/Unspecified'
      ELSE 'Other - ' || client_application
    END,
    warehouse_id
),

consumption_summary AS (
  SELECT
    consumer_type,
    SUM(query_count) AS total_queries,
    SUM(total_execution_time_sec) AS total_exec_time_sec,
    SUM(approx_compute_hours) AS total_compute_hours,
    AVG(avg_execution_time_sec) AS avg_query_time_sec,
    SUM(successful_queries) AS total_successful,
    SUM(failed_queries) AS total_failed
  FROM query_consumption
  GROUP BY consumer_type
),

totals AS (
  SELECT
    SUM(total_queries) AS grand_total_queries,
    SUM(total_compute_hours) AS grand_total_compute_hours
  FROM consumption_summary
)

SELECT
  cs.consumer_type,
  cs.total_queries,
  ROUND((cs.total_queries * 100.0 / t.grand_total_queries), 2) AS pct_of_total_queries,
  ROUND(cs.total_exec_time_sec, 2) AS total_execution_time_sec,
  ROUND(cs.total_compute_hours, 4) AS total_compute_hours,
  ROUND((cs.total_compute_hours * 100.0 / t.grand_total_compute_hours), 2) AS pct_of_total_consumption,
  ROUND(cs.avg_query_time_sec, 2) AS avg_query_time_sec,
  cs.total_successful,
  cs.total_failed,
  ROUND((cs.total_failed * 100.0 / NULLIF(cs.total_queries, 0)), 2) AS failure_rate_pct
FROM consumption_summary cs
CROSS JOIN totals t
ORDER BY cs.total_compute_hours DESC;

-- Optional: Get detailed breakdown by warehouse for each consumer type
-- Uncomment the query below if you want to see warehouse-level details

/*
SELECT
  qc.consumer_type,
  qc.warehouse_id,
  qc.query_count,
  ROUND(qc.total_execution_time_sec, 2) AS total_exec_time_sec,
  ROUND(qc.approx_compute_hours, 4) AS compute_hours,
  ROUND(qc.avg_execution_time_sec, 2) AS avg_query_time_sec,
  qc.successful_queries,
  qc.failed_queries
FROM query_consumption qc
ORDER BY qc.consumer_type, qc.approx_compute_hours DESC;
*/
