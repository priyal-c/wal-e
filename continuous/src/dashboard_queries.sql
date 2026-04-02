-- =============================================================================
-- WAL-E Continuous Mode — Lakeview Dashboard Queries
-- Import these into a Databricks AI/BI (Lakeview) dashboard.
-- Replace ${catalog} and ${schema} with your values (default: wal_e.continuous).
-- =============================================================================


-- ---------------------------------------------------------------------------
-- 1. OVERALL SCORE TREND (line chart)
-- ---------------------------------------------------------------------------
SELECT
  run_timestamp,
  MIN(overall_score / 2.0 * 100) AS score_pct,
  MIN(maturity_level)            AS maturity
FROM ${catalog}.${schema}.wal_e_assessments
GROUP BY run_id, run_timestamp
ORDER BY run_timestamp;


-- ---------------------------------------------------------------------------
-- 2. PILLAR SCORES — LATEST RUN (bar chart or radar)
-- ---------------------------------------------------------------------------
WITH latest_run AS (
  SELECT run_id
  FROM ${catalog}.${schema}.wal_e_assessments
  ORDER BY run_timestamp DESC
  LIMIT 1
)
SELECT
  a.pillar,
  ROUND(AVG(a.score) / 2.0 * 100, 1) AS avg_score_pct,
  COUNT(*)                              AS bp_count
FROM ${catalog}.${schema}.wal_e_assessments a
JOIN latest_run lr ON a.run_id = lr.run_id
GROUP BY a.pillar
ORDER BY avg_score_pct;


-- ---------------------------------------------------------------------------
-- 3. BEST PRACTICES HEATMAP — LATEST RUN (table / heatmap)
-- ---------------------------------------------------------------------------
WITH latest_run AS (
  SELECT run_id
  FROM ${catalog}.${schema}.wal_e_assessments
  ORDER BY run_timestamp DESC
  LIMIT 1
)
SELECT
  a.pillar,
  a.best_practice,
  a.score,
  CASE
    WHEN a.score = 0 THEN '🔴 Not Implemented'
    WHEN a.score = 1 THEN '🟡 Partial'
    WHEN a.score = 2 THEN '🟢 Implemented'
  END AS status,
  a.verified,
  a.finding_notes
FROM ${catalog}.${schema}.wal_e_assessments a
JOIN latest_run lr ON a.run_id = lr.run_id
ORDER BY a.pillar, a.score;


-- ---------------------------------------------------------------------------
-- 4. OPEN VIOLATIONS (table with severity badges)
-- ---------------------------------------------------------------------------
SELECT
  detected_at,
  severity,
  pillar,
  best_practice,
  previous_score,
  current_score,
  ROUND(current_score - previous_score, 1) AS score_change,
  finding_notes,
  acknowledged,
  acknowledged_by
FROM ${catalog}.${schema}.wal_e_violations
WHERE acknowledged = FALSE
ORDER BY
  CASE severity
    WHEN 'critical' THEN 1
    WHEN 'high'     THEN 2
    WHEN 'medium'   THEN 3
    WHEN 'low'      THEN 4
  END,
  detected_at DESC;


-- ---------------------------------------------------------------------------
-- 5. VIOLATION TREND (bar chart — violations per run)
-- ---------------------------------------------------------------------------
SELECT
  v.detected_at::DATE        AS detection_date,
  v.severity,
  COUNT(*)                    AS violation_count
FROM ${catalog}.${schema}.wal_e_violations v
GROUP BY detection_date, v.severity
ORDER BY detection_date, v.severity;


-- ---------------------------------------------------------------------------
-- 6. DRIFT HISTORY — SPECIFIC BP OVER TIME (line chart, filter by BP)
-- ---------------------------------------------------------------------------
SELECT
  a.run_timestamp,
  a.best_practice,
  a.score,
  a.score / 2.0 * 100 AS score_pct
FROM ${catalog}.${schema}.wal_e_assessments a
WHERE a.best_practice = '${best_practice_filter}'
ORDER BY a.run_timestamp;


-- ---------------------------------------------------------------------------
-- 7. COVERAGE SUMMARY (counter / stat)
-- ---------------------------------------------------------------------------
WITH latest_run AS (
  SELECT run_id
  FROM ${catalog}.${schema}.wal_e_assessments
  ORDER BY run_timestamp DESC
  LIMIT 1
)
SELECT
  COUNT(*)                                              AS total_bps,
  SUM(CASE WHEN a.score = 2 THEN 1 ELSE 0 END)        AS fully_implemented,
  SUM(CASE WHEN a.score = 1 THEN 1 ELSE 0 END)        AS partially_implemented,
  SUM(CASE WHEN a.score = 0 THEN 1 ELSE 0 END)        AS not_implemented,
  SUM(CASE WHEN a.verified THEN 1 ELSE 0 END)          AS verified_count,
  ROUND(SUM(CASE WHEN a.verified THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS coverage_pct
FROM ${catalog}.${schema}.wal_e_assessments a
JOIN latest_run lr ON a.run_id = lr.run_id;


-- ---------------------------------------------------------------------------
-- 8. RUN HISTORY (table — all runs with summary stats)
-- ---------------------------------------------------------------------------
SELECT
  run_id,
  MIN(run_timestamp)                                    AS run_time,
  MIN(run_mode)                                         AS mode,
  MIN(auth_method)                                      AS auth,
  MIN(overall_score)                                    AS overall_score,
  MIN(maturity_level)                                   AS maturity,
  COUNT(*)                                              AS bps_evaluated,
  SUM(CASE WHEN score = 0 THEN 1 ELSE 0 END)           AS red_count,
  SUM(CASE WHEN score = 2 THEN 1 ELSE 0 END)           AS green_count
FROM ${catalog}.${schema}.wal_e_assessments
GROUP BY run_id
ORDER BY run_time DESC;
