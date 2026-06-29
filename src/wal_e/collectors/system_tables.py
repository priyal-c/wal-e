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

# The SQL Statement Execution API caps the synchronous wait at 50s; values
# outside 5-50s (or 0s for async) are rejected. Statements that exceed the
# wait fall back to asynchronous execution and are polled via GET.
_WAIT_TIMEOUT = "50s"
_POLL_INTERVAL_SECONDS = 5.0
_MAX_POLL_SECONDS = 180.0
_TERMINAL_STATES = ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED")


class SystemTablesCollector(BaseCollector):
    """Collects data from Databricks system tables via SQL statements."""

    def __init__(self, profile_name: str = "DEFAULT", warehouse_id: str = "") -> None:
        super().__init__(profile_name)
        self.warehouse_id = warehouse_id

    @staticmethod
    def _parse_rows(resp: dict[str, Any]) -> list[dict[str, Any]]:
        """Map an INLINE JSON_ARRAY statement response into row dicts."""
        manifest = resp.get("manifest", {}) or {}
        columns = [
            c.get("name", f"col{i}")
            for i, c in enumerate(manifest.get("schema", {}).get("columns", []) or [])
        ]
        data_array = (resp.get("result", {}) or {}).get("data_array", []) or []
        rows: list[dict[str, Any]] = []
        for row_arr in data_array:
            rows.append({
                col_name: (row_arr[i] if i < len(row_arr) else None)
                for i, col_name in enumerate(columns)
            })
        return rows

    def _poll_statement(self, statement_id: str) -> dict[str, Any] | None:
        """Poll a long-running statement until it reaches a terminal state."""
        deadline = time.perf_counter() + _MAX_POLL_SECONDS
        while time.perf_counter() < deadline:
            time.sleep(_POLL_INTERVAL_SECONDS)
            cmd = [
                "databricks", "api", "get",
                f"/api/2.0/sql/statements/{statement_id}",
                "--profile", self.profile_name,
            ]
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            except (subprocess.TimeoutExpired, OSError):
                continue
            if result.returncode != 0 or not result.stdout:
                continue
            try:
                resp = json.loads(result.stdout)
            except json.JSONDecodeError:
                continue
            if resp.get("status", {}).get("state", "") in _TERMINAL_STATES:
                return resp
        return None

    def _run_sql(self, sql: str, label: str = "") -> tuple[list[dict[str, Any]] | None, bool]:
        """Execute SQL via the Databricks statement execution API.

        Submits via `databricks api post /api/2.0/sql/statements` with the
        configured warehouse, waiting up to the API-allowed maximum. Statements
        that need longer than the wait window continue asynchronously and are
        polled to completion. Returns parsed rows or None on failure.
        """
        payload = json.dumps({
            "warehouse_id": self.warehouse_id,
            "statement": sql,
            "wait_timeout": _WAIT_TIMEOUT,
            "on_wait_timeout": "CONTINUE",
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
            output = result.stdout or ""
            stderr = result.stderr.strip() if result.stderr else ""

            if result.returncode != 0 or not output:
                self._audit(label, sql, start, False, output or stderr,
                            stderr or f"Exit code {result.returncode}")
                return None, False

            resp = json.loads(output)
            state = resp.get("status", {}).get("state", "")

            if state in ("PENDING", "RUNNING"):
                statement_id = resp.get("statement_id", "")
                polled = self._poll_statement(statement_id) if statement_id else None
                if polled is not None:
                    resp = polled
                    state = resp.get("status", {}).get("state", "")

            if state != "SUCCEEDED":
                err = resp.get("status", {}).get("error", {}).get("message", state or "unknown")
                self._audit(label, sql, start, False, output[:2000], err)
                return None, False

            rows = self._parse_rows(resp)
            self._audit(label, sql, start, True, output[:2000], None)
            return rows, True

        except (subprocess.TimeoutExpired, json.JSONDecodeError, Exception) as e:
            self._audit(label, sql, start, False, "", str(e))
            return None, False

    def _audit(self, label: str, sql: str, start: float, success: bool,
               raw_output: str, error: str | None) -> None:
        self.audit_entries.append(AuditEntry(
            command=["SQL"] + ([label] if label else []) + [sql[:120]],
            raw_output=raw_output,
            duration_seconds=time.perf_counter() - start,
            success=success,
            error=error,
        ))

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

        # Cluster uptime derived from per-minute node telemetry. system.compute.clusters
        # is a config (SCD2) dimension with no runtime state, so running hours come from
        # node_timeline driver rows (one per running minute per cluster).
        rows, ok = self._run_sql("""
            WITH uptime AS (
                SELECT cluster_id,
                       COUNT(*) / 60.0 AS running_hours,
                       AVG(cpu_user_percent + cpu_system_percent) AS avg_cpu_pct
                FROM system.compute.node_timeline
                WHERE start_time >= current_date() - INTERVAL 30 DAYS
                  AND driver = true
                GROUP BY cluster_id
                HAVING running_hours > 0
            ),
            names AS (
                SELECT cluster_id, ANY_VALUE(cluster_name) AS cluster_name
                FROM system.compute.clusters
                GROUP BY cluster_id
            )
            SELECT u.cluster_id,
                   n.cluster_name,
                   u.running_hours,
                   u.avg_cpu_pct
            FROM uptime u
            LEFT JOIN names n ON u.cluster_id = n.cluster_id
            ORDER BY u.running_hours DESC
            LIMIT 50
        """, "compute-cluster-uptime-30d")
        if ok and rows:
            result["available"] = True
            result["cluster_uptime"] = rows
            total_running = sum(float(r.get("running_hours") or 0) for r in rows)
            result["total_running_hours_30d"] = round(total_running, 1)

        # Idle waste: clusters that ran for meaningful time but stayed near-idle
        # (average CPU under 10%), signalling over-provisioning or missing auto-stop.
        rows, ok = self._run_sql("""
            WITH uptime AS (
                SELECT cluster_id,
                       COUNT(*) / 60.0 AS running_hours,
                       AVG(cpu_user_percent + cpu_system_percent) AS avg_cpu_pct
                FROM system.compute.node_timeline
                WHERE start_time >= current_date() - INTERVAL 30 DAYS
                  AND driver = true
                GROUP BY cluster_id
                HAVING running_hours > 1 AND avg_cpu_pct < 10
            ),
            names AS (
                SELECT cluster_id, ANY_VALUE(cluster_name) AS cluster_name
                FROM system.compute.clusters
                GROUP BY cluster_id
            )
            SELECT u.cluster_id,
                   n.cluster_name,
                   u.running_hours,
                   u.avg_cpu_pct
            FROM uptime u
            LEFT JOIN names n ON u.cluster_id = n.cluster_id
            ORDER BY u.running_hours DESC
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

        # Query stats last 30 days. system.query.history uses execution_status
        # (FINISHED/FAILED/CANCELED) and total_duration_ms; the warehouse id lives
        # in the compute struct.
        rows, ok = self._run_sql("""
            SELECT COUNT(*) AS total_queries,
                   SUM(CASE WHEN execution_status = 'FINISHED' THEN 1 ELSE 0 END) AS succeeded,
                   SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
                   SUM(CASE WHEN execution_status = 'CANCELED' THEN 1 ELSE 0 END) AS canceled,
                   AVG(total_duration_ms) AS avg_duration_ms,
                   PERCENTILE(total_duration_ms, 0.95) AS p95_duration_ms,
                   PERCENTILE(total_duration_ms, 0.99) AS p99_duration_ms
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
              AND execution_status = 'FINISHED'
              AND total_duration_ms > 300000
        """, "query-slow-count-30d")
        if ok and rows:
            result["slow_queries_30d"] = int(rows[0].get("slow_query_count") or 0)

        # Warehouse utilization
        rows, ok = self._run_sql("""
            SELECT compute.warehouse_id AS warehouse_id,
                   COUNT(*) AS query_count,
                   AVG(total_duration_ms) AS avg_duration_ms,
                   SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS failures
            FROM system.query.history
            WHERE start_time >= current_date() - INTERVAL 30 DAYS
              AND compute.warehouse_id IS NOT NULL
            GROUP BY compute.warehouse_id
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
