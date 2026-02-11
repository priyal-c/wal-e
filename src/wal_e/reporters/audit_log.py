"""
Audit log reporter - Generates WAL_Assessment_Audit_Report.md with full evidence trail.
"""

from pathlib import Path
from typing import Any, Dict, List, Union

from .base import AuditEntry, BaseReporter, BestPracticeScore, ScoredAssessment


# Maximum characters of raw output to show before truncation
OUTPUT_TRUNCATE_LEN = 2000


# Recommended system table SQL queries for deeper follow-up
RECOMMENDED_SQL_QUERIES = """
## Recommended System Table Queries for Deeper Follow-up

The following system table queries can provide additional evidence for assessment findings.
Run these in a Databricks SQL warehouse or notebook for further analysis.

### Unity Catalog & Governance

```sql
-- List all catalogs and schemas
SELECT * FROM system.information_schema.catalogs;
SELECT * FROM system.information_schema.schemata;

-- Table metadata and lineage
SELECT * FROM system.information_schema.tables LIMIT 100;
```

### Compute & Jobs

```sql
-- Cluster usage and runtime
SELECT * FROM system.compute.cluster_usage LIMIT 100;

-- Job run history
SELECT * FROM system.compute.job_runs LIMIT 100;

-- Warehouse usage
SELECT * FROM system.compute.warehouse_usage LIMIT 100;
```

### Cost & Billing

```sql
-- Billing usage (if enabled)
SELECT * FROM system.billing.usage LIMIT 100;
```

### Operational Logs

```sql
-- Query history
SELECT * FROM system.query_history LIMIT 100;

-- Audit logs (if streaming to system tables)
SELECT * FROM system.access.audit LIMIT 100;
```

*Note: Availability of system tables depends on your workspace configuration and Unity Catalog setup.*
"""


class AuditLogReporter(BaseReporter):
    """Generates WAL_Assessment_Audit_Report.md documenting all API/CLI commands and outputs."""

    def __init__(self):
        super().__init__("WAL_Assessment_Audit_Report.md")

    def generate(
        self,
        scored_assessment: ScoredAssessment,
        collected_data: Dict[str, Any],
        audit_entries: List[AuditEntry],
        output_dir: Union[str, Path],
    ) -> Path:
        output_path = self._ensure_output_dir(output_dir) / self.output_filename

        best_practice_scores = self._get_best_practice_scores(scored_assessment)
        workspace_host = self._get_workspace_host(scored_assessment)
        assessment_date = self._get_assessment_date(scored_assessment)
        cloud = self._get_cloud_provider(scored_assessment)
        cloud_display = self._cloud_display_name(cloud)

        sections: List[str] = [
            self._render_header(workspace_host, assessment_date, cloud_display),
            self._render_summary(audit_entries),
            self._render_audit_entries(audit_entries),
            self._render_evidence_mapping(best_practice_scores, audit_entries),
            self._render_limitations(),
            RECOMMENDED_SQL_QUERIES,
        ]

        output_path.write_text("\n\n".join(sections), encoding="utf-8")
        return output_path

    def _render_header(self, workspace_host: str, assessment_date: str, cloud_display: str = "Unknown Cloud") -> str:
        return f"""# WAL Assessment Audit Report

**Workspace:** `{workspace_host}`  
**Cloud Provider:** {cloud_display}  
**Assessment Date:** {assessment_date}

This document lists every API call and CLI command executed during the WAL-E assessment, including raw outputs (truncated where appropriate). It serves as an audit trail and evidence base for the assessment findings.

---

"""

    def _render_summary(self, audit_entries: List[AuditEntry]) -> str:
        total = len(audit_entries)
        total_duration = sum(
            e.get("duration") or 0
            for e in audit_entries
            if isinstance(e.get("duration"), (int, float))
        )
        return f"""## Summary

| Metric | Value |
|--------|-------|
| Total API/CLI commands executed | {total} |
| Total execution time (seconds) | {total_duration:.1f} |

---

"""

    def _render_audit_entries(self, audit_entries: List[AuditEntry]) -> str:
        lines = ["## Audit Entries", ""]

        for i, entry in enumerate(audit_entries, 1):
            command = entry.get("command") or "(unknown command)"
            output = entry.get("output") or ""
            timestamp = entry.get("timestamp") or ""
            duration = entry.get("duration")

            # Truncate output for readability
            if len(output) > OUTPUT_TRUNCATE_LEN:
                output_display = output[:OUTPUT_TRUNCATE_LEN] + "\n\n... [truncated]"
            else:
                output_display = output if output else "(no output)"

            duration_str = f" ({duration}s)" if duration is not None else ""

            lines.append(f"### Entry {i}{duration_str}")
            lines.append("")
            lines.append(f"**Command:**")
            lines.append("```")
            lines.append(command)
            lines.append("```")
            lines.append("")
            lines.append(f"**Timestamp:** {timestamp}")
            lines.append("")
            lines.append("**Output:**")
            lines.append("```")
            lines.append(output_display)
            lines.append("```")
            lines.append("")

        return "\n".join(lines)

    def _render_evidence_mapping(
        self,
        best_practice_scores: List[BestPracticeScore],
        audit_entries: List[AuditEntry],
    ) -> str:
        """Document what evidence (audit entries) was used for which finding."""
        lines = [
            "## Evidence-to-Finding Mapping",
            "",
            "This section maps assessment findings to the audit entries that provided supporting evidence.",
            "",
        ]

        if not best_practice_scores:
            lines.append("*No scored findings to map.*")
            return "\n".join(lines)

        for bp in best_practice_scores:
            name = bp.get("name") or "Unknown"
            pillar = bp.get("pillar") or "Unknown"
            notes = bp.get("finding_notes") or ""
            lines.append(f"### {pillar}: {name}")
            lines.append("")
            lines.append(f"**Finding notes:** {notes}")
            lines.append("")
            lines.append("**Relevant evidence:** Assessment scoring uses aggregated data from all collectors. See audit entries above for the API/CLI commands that populated the data used to evaluate this practice.")
            lines.append("")

        return "\n".join(lines)

    def _render_limitations(self) -> str:
        return """## Limitations

- **Read-only scope:** WAL-E performs only read operations. No configuration or data is modified.
- **Point-in-time:** The assessment reflects the state of the workspace at the time of execution.
- **API coverage:** Findings are based on data available through the Databricks CLI and REST APIs. Some configurations may not be accessible.
- **Permissions:** The breadth of data collected depends on the permissions of the token used. Admin access is recommended for a complete assessment.
- **Output truncation:** Long command outputs are truncated in this report for readability. Full outputs may be available in debug logs.

---

"""
