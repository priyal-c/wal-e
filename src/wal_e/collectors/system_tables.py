"""System tables collector for deep assessment mode.

Queries Databricks system tables via SQL Warehouse to collect operational
reality data: billing usage, cluster lifecycle, query performance, job
run history, and audit events. Requires a running SQL warehouse and
SELECT grants on system.* schemas.
"""

from __future__ import annotations

import json
import subprocess
import time
from typing import Any

from wal_e.collectors.base import AuditEntry, BaseCollector


class SystemTablesCollector(BaseCollector):
    """Collects data from Databricks system tables via SQL statements."""

    def __init__(self, profile_name: str = "DEFAULT", warehouse_id: str = "") -> None:
        super().__init__(profile_name)
        self.warehouse_id = warehouse_id

    def _run_sql(self, sql: str, label: str = "") -> tuple[list[dict[str, Any]] | None, bool]:
        """Execute SQL via Databricks statement execution API.

        Uses `databricks api post /api/2.0/sql/statements` with the configured
        warehouse. Returns parsed rows or None on failure.
        """
        payload = json.dumps({
            "warehouse_id": self.warehouse_id,
            "statement": sql,
            "wait_timeout": "60s",
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        })
        cmd = [
            "databricks", "api", "post",
            "/api/2.0/sql/statements",
            "--json", payload,
            "--profile", self.profile_name,
        ]
        start = time.perf_counter()
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            duration = time.perf_counter() - start
            output = result.stdout or ""
            stderr = result.stderr.strip() if result.stderr else ""
            success = result.returncode == 0

            self.audit_entries.append(AuditEntry(
                command=["SQL"] + ([label] if label else []) + [sql[:120]],
                raw_output=output[:2000] if output else stderr,
                duration_seconds=duration,
                success=success,
                error=None if success else (stderr or f"Exit code {result.returncode}"),
            ))

            if not success or not output:
                return None, False

            resp = json.loads(output)
            status = resp.get("status", {}).get("state", "")
            if status not in ("SUCCEEDED",):
                err = resp.get("status", {}).get("error", {}).get("message", status)
                return None, False

            manifest = resp.get("manifest", {})
            columns = [c.get("name", f"col{i}") for i, c in enumerate(manifest.get("schema", {}).get("columns", []))]
            chunks = resp.get("result", {}).get("data_array", [])

            rows: list[dict[str, Any]] = []
            for row_arr in chunks:
                row = {}
                for i, col_name in enumerate(columns):
                    row[col_name] = row_arr[i] if i < len(row_arr) else None
                rows.append(row)
            return rows, True

        except (subprocess.TimeoutExpired, json.JSONDecodeError, Exception) as e:
            duration = time.perf_counter() - start
            self.audit_entries.append(AuditEntry(
                command=["SQL"] + ([label] if label else []) + [sql[:120]],
                raw_output="",
                duration_seconds=duration,
                success=False,
                error=str(e),
            ))
            return None, False

    def collect(self) -> dict[str, Any]:
        """Collect all system table data. Each query is independent — failures are isolated."""
        findings: dict[str, Any] = {
            "available": False,
            "billing": {},
            "compute_history": {},
            "query_history": {},
            "job_runs": {},
            "audit_events": {},
        }

        if not self.warehouse_id:
            return findings

        # Quick connectivity check
        test_rows, ok = self._run_sql("SELECT 1 AS test", "connectivity-check")
        if not ok:
            return findings
        findings["available"] = True

        findings["billing"] = self._collect_billing()
        findings["compute_history"] = self._collect_compute_history()
        findings["query_history"] = self._collect_query_history()
        findings["job_runs"] = self._collect_job_runs()
        findings["audit_events"] = self._collect_audit_events()

        return findings

    # ------------------------------------------------------------------
    # Billing / Cost
    # ------------------------------------------------------------------
    def _collect_billing(self) -> dict[str, Any]:
        result: dict[str, Any] = {"available": False}

        # Total DBU spend last 30 days by SKU
        rows, ok = self._run_sql("""
            SELECT sku_name,
                   SUM(usage_quantity) AS total_dbus,
                   COUNT(DISTINCT workspace_id) AS workspace_count
            FROM system.billing.usage
            WHERE usage_date >= current_date() - INTERVAL 30 DAYS
            GROUP BY sku_name
            ORDER BY total_dbus DESC
            LIMIT 20
        """, "billing-by-sku-30d")
        if ok and rows:
            result["available"] = True
            result["spend_by_sku"] = rows
            result["total_dbus_30d"] = sum(float(r.get("total_dbus") or 0) for r in rows)

        # Daily spend trend (last 30 days)
        rows, ok = self._run_sql("""
            SELECT usage_date,
                   SUM(usage_quantity) AS daily_dbus
            FROM system.billing.usage
            WHERE usage_date >= current_date() - INTERVAL 30 DAYS
            GROUP BY usage_date
            ORDER BY usage_date
        """, "billing-daily-trend-30d")
        if ok and rows:
            result["daily_trend"] = rows
            dbvals = [float(r.get("daily_dbus") or 0) for r in rows]
            if len(dbvals) >= 7:
                first_week = sum(dbvals[:7]) / 7
                last_week = sum(dbvals[-7:]) / 7
                if first_week > 0:
                    result["trend_pct_change"] = round(((last_week - first_week) / first_week) * 100, 1)

        # Top 10 most expensive clusters
        rows, ok = self._run_sql("""
            SELECT usage_metadata.cluster_id AS cluster_id,
                   SUM(usage_quantity) AS total_dbus
            FROM system.billing.usage
            WHERE usage_date >= current_date() - INTERVAL 30 DAYS
              AND usage_metadata.cluster_id IS NOT NULL
            GROUP BY usage_metadata.cluster_id
            ORDER BY total_dbus DESC
            LIMIT 10
        """, "billing-top-clusters-30d")
        if ok and rows:
            result["top_cost_clusters"] = rows

        return result

    # ------------------------------------------------------------------
    # Compute lifecycle / idle analysis
    # ------------------------------------------------------------------
    def _collect_compute_history(self) -> dict[str, Any]:
        result: dict[str, Any] = {"available": False}

        # Cluster uptime and idle time (last 30 days)
        rows, ok = self._run_sql("""
            SELECT cluster_id,
                   cluster_name,
                   SUM(CASE WHEN state = 'RUNNING' THEN duration_ms ELSE 0 END) / 3600000.0 AS running_hours,
                   SUM(duration_ms) / 3600000.0 AS total_hours,
                   COUNT(DISTINCT DATE(change_time)) AS active_days
            FROM system.compute.clusters
            WHERE change_time >= current_date() - INTERVAL 30 DAYS
            GROUP BY cluster_id, cluster_name
            HAVING running_hours > 0
            ORDER BY running_hours DESC
            LIMIT 20
        """, "compute-cluster-uptime-30d")
        if ok and rows:
            result["available"] = True
            result["cluster_uptime"] = rows
            total_running = sum(float(r.get("running_hours") or 0) for r in rows)
            result["total_running_hours_30d"] = round(total_running, 1)

        # Clusters that were running but had no jobs (potential idle waste)
        rows, ok = self._run_sql("""
            SELECT c.cluster_id,
                   c.cluster_name,
                   SUM(CASE WHEN c.state = 'RUNNING' THEN c.duration_ms ELSE 0 END) / 3600000.0 AS running_hours
            FROM system.compute.clusters c
            LEFT JOIN system.lakeflow.job_run_timeline j
              ON c.cluster_id = j.cluster_id
              AND j.period_start_time >= current_date() - INTERVAL 30 DAYS
            WHERE c.change_time >= current_date() - INTERVAL 30 DAYS
              AND j.cluster_id IS NULL
            GROUP BY c.cluster_id, c.cluster_name
            HAVING running_hours > 1
            ORDER BY running_hours DESC
            LIMIT 10
        """, "compute-idle-clusters-30d")
        if ok and rows:
            result["idle_clusters"] = rows
            result["idle_hours_30d"] = round(sum(float(r.get("running_hours") or 0) for r in rows), 1)

        return result

    # ------------------------------------------------------------------
    # Query performance
    # ------------------------------------------------------------------
    def _collect_query_history(self) -> dict[str, Any]:
        result: dict[str, Any] = {"available": False}

        # Query stats last 30 days
        rows, ok = self._run_sql("""
            SELECT COUNT(*) AS total_queries,
                   SUM(CASE WHEN status = 'FINISHED' THEN 1 ELSE 0 END) AS succeeded,
                   SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
                   SUM(CASE WHEN status = 'CANCELED' THEN 1 ELSE 0 END) AS canceled,
                   AVG(duration) AS avg_duration_ms,
                   PERCENTILE(duration, 0.95) AS p95_duration_ms,
                   PERCENTILE(duration, 0.99) AS p99_duration_ms
            FROM system.query.history
            WHERE start_time >= current_date() - INTERVAL 30 DAYS
        """, "query-stats-30d")
        if ok and rows and rows[0].get("total_queries"):
            result["available"] = True
            r = rows[0]
            total = int(r.get("total_queries") or 0)
            failed = int(r.get("failed") or 0)
            result["total_queries_30d"] = total
            result["failed_queries_30d"] = failed
            result["failure_rate_pct"] = round((failed / total) * 100, 2) if total > 0 else 0
            result["avg_duration_ms"] = float(r.get("avg_duration_ms") or 0)
            result["p95_duration_ms"] = float(r.get("p95_duration_ms") or 0)
            result["p99_duration_ms"] = float(r.get("p99_duration_ms") or 0)

        # Slow queries (>5 min)
        rows, ok = self._run_sql("""
            SELECT COUNT(*) AS slow_query_count
            FROM system.query.history
            WHERE start_time >= current_date() - INTERVAL 30 DAYS
              AND status = 'FINISHED'
              AND duration > 300000
        """, "query-slow-count-30d")
        if ok and rows:
            result["slow_queries_30d"] = int(rows[0].get("slow_query_count") or 0)

        # Warehouse utilization
        rows, ok = self._run_sql("""
            SELECT warehouse_id,
                   COUNT(*) AS query_count,
                   AVG(duration) AS avg_duration_ms,
                   SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failures
            FROM system.query.history
            WHERE start_time >= current_date() - INTERVAL 30 DAYS
              AND warehouse_id IS NOT NULL
            GROUP BY warehouse_id
            ORDER BY query_count DESC
            LIMIT 10
        """, "query-warehouse-utilization-30d")
        if ok and rows:
            result["warehouse_utilization"] = rows

        return result

    # ------------------------------------------------------------------
    # Job run success/failure
    # ------------------------------------------------------------------
    def _collect_job_runs(self) -> dict[str, Any]:
        result: dict[str, Any] = {"available": False}

        # Job run stats last 30 days
        rows, ok = self._run_sql("""
            SELECT COUNT(*) AS total_runs,
                   SUM(CASE WHEN result_state = 'SUCCESS' THEN 1 ELSE 0 END) AS succeeded,
                   SUM(CASE WHEN result_state IN ('FAILED', 'TIMEDOUT', 'INTERNAL_ERROR') THEN 1 ELSE 0 END) AS failed,
                   SUM(CASE WHEN result_state = 'CANCELED' THEN 1 ELSE 0 END) AS canceled
            FROM system.lakeflow.job_run_timeline
            WHERE period_start_time >= current_date() - INTERVAL 30 DAYS
        """, "jobs-run-stats-30d")
        if ok and rows and rows[0].get("total_runs"):
            result["available"] = True
            r = rows[0]
            total = int(r.get("total_runs") or 0)
            failed = int(r.get("failed") or 0)
            result["total_runs_30d"] = total
            result["failed_runs_30d"] = failed
            result["success_rate_pct"] = round(((total - failed) / total) * 100, 2) if total > 0 else 0

        # Top failing jobs
        rows, ok = self._run_sql("""
            SELECT job_id,
                   COUNT(*) AS total_runs,
                   SUM(CASE WHEN result_state IN ('FAILED', 'TIMEDOUT', 'INTERNAL_ERROR') THEN 1 ELSE 0 END) AS failures
            FROM system.lakeflow.job_run_timeline
            WHERE period_start_time >= current_date() - INTERVAL 30 DAYS
            GROUP BY job_id
            HAVING failures > 0
            ORDER BY failures DESC
            LIMIT 10
        """, "jobs-top-failing-30d")
        if ok and rows:
            result["top_failing_jobs"] = rows

        return result

    # ------------------------------------------------------------------
    # Security audit events
    # ------------------------------------------------------------------
    def _collect_audit_events(self) -> dict[str, Any]:
        result: dict[str, Any] = {"available": False}

        # Audit event counts by action category (last 30 days)
        rows, ok = self._run_sql("""
            SELECT action_name,
                   COUNT(*) AS event_count
            FROM system.access.audit
            WHERE event_date >= current_date() - INTERVAL 30 DAYS
            GROUP BY action_name
            ORDER BY event_count DESC
            LIMIT 30
        """, "audit-events-by-action-30d")
        if ok and rows:
            result["available"] = True
            result["event_counts_by_action"] = rows
            result["total_events_30d"] = sum(int(r.get("event_count") or 0) for r in rows)

        # Failed authentication attempts
        rows, ok = self._run_sql("""
            SELECT COUNT(*) AS failed_logins
            FROM system.access.audit
            WHERE event_date >= current_date() - INTERVAL 30 DAYS
              AND action_name IN ('login', 'tokenLogin', 'aadTokenLogin')
              AND response.status_code >= 400
        """, "audit-failed-logins-30d")
        if ok and rows:
            result["failed_logins_30d"] = int(rows[0].get("failed_logins") or 0)

        # Permission change events
        rows, ok = self._run_sql("""
            SELECT COUNT(*) AS permission_changes
            FROM system.access.audit
            WHERE event_date >= current_date() - INTERVAL 30 DAYS
              AND action_name IN (
                'changePermissions', 'updatePermissions',
                'changeClusterAcl', 'changeDbTokenAcl',
                'grantPermission', 'revokePermission'
              )
        """, "audit-permission-changes-30d")
        if ok and rows:
            result["permission_changes_30d"] = int(rows[0].get("permission_changes") or 0)

        return result
