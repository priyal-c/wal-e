"""
Markdown reporter - Generates WAL_Assessment_Readout.md with full detailed assessment.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from .base import (
    PILLAR_ORDER,
    AuditEntry,
    BaseReporter,
    BestPracticeScore,
    ScoredAssessment,
)


class MarkdownReporter(BaseReporter):
    """Generates the full detailed WAL Assessment Readout in Markdown."""

    def __init__(self):
        super().__init__("WAL_Assessment_Readout.md")

    def generate(
        self,
        scored_assessment: ScoredAssessment,
        collected_data: Dict[str, Any],
        audit_entries: List[AuditEntry],
        output_dir: Union[str, Path],
    ) -> Path:
        output_path = self._ensure_output_dir(output_dir) / self.output_filename

        pillar_scores = self._get_pillar_scores(scored_assessment)
        best_practice_scores = self._get_best_practice_scores(scored_assessment)
        overall_score = self._get_overall_score(scored_assessment)
        maturity_level = self._get_maturity_level(scored_assessment)
        workspace_host = self._get_workspace_host(scored_assessment)
        assessment_date = self._get_assessment_date(scored_assessment)

        sections: List[str] = []

        # Title and Executive Summary
        sections.append(self._render_title(workspace_host, assessment_date, collected_data))
        sections.append(
            self._render_executive_summary(
                overall_score, maturity_level, pillar_scores
            )
        )

        # Per-pillar sections
        for i, pillar in enumerate(PILLAR_ORDER, 1):
            section = self._render_pillar_section(
                i,
                pillar,
                pillar_scores.get(pillar),
                best_practice_scores,
                collected_data,
            )
            if section:
                sections.append(section)

        # Critical findings summary
        sections.append(self._render_critical_findings(best_practice_scores, pillar_scores))

        # Remediation roadmap
        sections.append(self._render_remediation_roadmap(best_practice_scores))

        # Appendices
        sections.append(self._render_appendices(collected_data, audit_entries))

        output_path.write_text("\n\n".join(sections), encoding="utf-8")
        return output_path

    def _render_title(self, workspace_host: str, assessment_date: str, collected_data: Dict[str, Any]) -> str:
        # Extract workspace metadata
        gov = collected_data.get("GovernanceCollector", {})
        metastore_name = gov.get("metastore_name", "N/A")
        catalog_count = gov.get("catalog_count", "N/A")

        return f"""# Well-Architected Lakehouse Assessment
## Final Readout

---

| **Field** | **Details** |
|---|---|
| **Workspace** | {workspace_host} |
| **Metastore** | {metastore_name} |
| **Assessment Date** | {assessment_date} |
| **Catalogs** | {catalog_count} |

---"""

    def _render_executive_summary(
        self,
        overall_score: float,
        maturity_level: str,
        pillar_scores: Dict[str, float],
    ) -> str:
        lines = [
            "## Executive Summary",
            "",
            "This document presents the findings of a Well-Architected Lakehouse (WAL) assessment. "
            "The assessment follows the Databricks Well-Architected Framework covering all **seven pillars**.",
            "",
            "### Overall Maturity Score",
            "",
            "| Pillar | Score (0-2) | Percentage | Maturity |",
            "|--------|:-----------:|:----------:|----------|",
        ]
        for pillar in PILLAR_ORDER:
            score = pillar_scores.get(pillar, 0)
            pct = self._score_to_pct(score)
            display = self._pillar_display_name(pillar)
            maturity = self._maturity_from_score(score)
            lines.append(f"| {display} | **{score:.1f}** | {pct:.0f}% | {maturity} |")
        overall_pct = self._score_to_pct(overall_score)
        lines.append(f"| **Overall Average** | **{overall_score:.2f}** | **{overall_pct:.0f}%** | **{maturity_level}** |")
        lines.append("")
        lines.append("> **Scoring Key:** 0 = Not Implemented, 1 = Partially Implemented, 2 = Fully Implemented / Best Practice")
        lines.append("")
        return "\n".join(lines)

    def _render_pillar_section(
        self,
        pillar_num: int,
        pillar: str,
        pillar_score: Optional[float],
        best_practice_scores: List[BestPracticeScore],
        collected_data: Dict[str, Any],
    ) -> str:
        display_name = self._pillar_display_name(pillar)
        pillar_bps = self._get_bps_for_pillar(best_practice_scores, pillar)

        lines = [
            f"## Pillar {pillar_num}: {display_name}",
            "",
        ]

        # Current state findings table from collected data
        findings_table = self._extract_pillar_findings(pillar, collected_data)
        if findings_table:
            lines.append(f"### {pillar_num}.1 Current State Findings")
            lines.append("")
            lines.append("| Area | Finding | Status |")
            lines.append("|------|---------|--------|")
            for area, finding, status in findings_table:
                lines.append(f"| **{area}** | {finding} | {status} |")
            lines.append("")

        # Best practice assessment table
        lines.append(f"### {pillar_num}.2 Assessment Against Best Practices")
        lines.append("")
        lines.append("| # | Best Practice | Score (0-2) | Notes |")
        lines.append("|---|---------------|:-----------:|-------|")
        for j, bp in enumerate(pillar_bps, 1):
            score_val = bp.get("score", 0)
            notes = bp.get("finding_notes", "")
            lines.append(f"| {j} | {bp.get('name', 'Unknown')} | **{score_val}** | {notes} |")
        lines.append("")

        # Recommendations
        low_bps = [bp for bp in pillar_bps if float(bp.get("score", 2)) < 1.5]
        lines.append(f"### {pillar_num}.3 Recommendations (Prioritized)")
        lines.append("")
        if low_bps:
            lines.append("| Priority | Recommendation | Impact |")
            lines.append("|----------|---------------|--------|")
            for k, bp in enumerate(sorted(low_bps, key=lambda x: float(x.get("score", 0)))):
                priority = "**P0**" if float(bp.get("score", 0)) == 0 else "**P1**"
                name = bp.get("name", "Unknown")
                notes = bp.get("finding_notes", "Review and remediate.")
                impact = "Critical" if float(bp.get("score", 0)) == 0 else "High"
                lines.append(f"| {priority} | **{name}:** {notes} | {impact} |")
        else:
            lines.append("- No specific recommendations; maintain current practices.")
        lines.append("")
        lines.append("---")
        return "\n".join(lines)

    def _render_critical_findings(
        self,
        best_practice_scores: List[BestPracticeScore],
        pillar_scores: Dict[str, float],
    ) -> str:
        # Get all zero-scored BPs
        critical = [bp for bp in best_practice_scores if float(bp.get("score", 2)) == 0]
        critical.sort(key=lambda x: x.get("pillar", ""))

        lines = [
            "## Summary of Critical Findings (Score = 0)",
            "",
            "| # | Finding | Pillar | Risk Level |",
            "|---|---------|--------|------------|",
        ]
        for i, bp in enumerate(critical[:15], 1):
            display = self._pillar_display_name(bp.get("pillar", ""))
            lines.append(f"| {i} | {bp.get('name', 'Unknown')}: {bp.get('finding_notes', '')} | {display} | **Critical** |")
        if not critical:
            lines.append("| - | No critical findings | - | - |")
        lines.append("")
        return "\n".join(lines)

    def _render_remediation_roadmap(
        self,
        best_practice_scores: List[BestPracticeScore],
    ) -> str:
        zero_scored = [bp for bp in best_practice_scores if float(bp.get("score", 2)) == 0]
        partial_scored = [bp for bp in best_practice_scores if float(bp.get("score", 2)) == 1]

        lines = [
            "## Recommended Remediation Roadmap",
            "",
            "### Phase 1: Immediate Actions (Week 1-2) - Quick Wins",
            "",
            "| Action | Pillar | Effort | Impact |",
            "|--------|--------|--------|--------|",
        ]
        for bp in zero_scored[:6]:
            display = self._pillar_display_name(bp.get("pillar", ""))
            lines.append(f"| {bp.get('name', '')}: {bp.get('finding_notes', '')} | {display} | Low-Medium | Critical |")
        lines.append("")

        lines.extend([
            "### Phase 2: Foundation (Week 3-6) - Governance & Security",
            "",
            "| Action | Pillar | Effort | Impact |",
            "|--------|--------|--------|--------|",
        ])
        for bp in zero_scored[6:12]:
            display = self._pillar_display_name(bp.get("pillar", ""))
            lines.append(f"| {bp.get('name', '')}: {bp.get('finding_notes', '')} | {display} | Medium | High |")
        lines.append("")

        lines.extend([
            "### Phase 3: Operational Maturity (Week 7-12) - Automation & Monitoring",
            "",
            "| Action | Pillar | Effort | Impact |",
            "|--------|--------|--------|--------|",
        ])
        for bp in partial_scored[:8]:
            display = self._pillar_display_name(bp.get("pillar", ""))
            lines.append(f"| Improve: {bp.get('name', '')} | {display} | Medium-High | High |")
        lines.append("")

        lines.extend([
            "### Phase 4: Optimization (Week 13-20) - Performance & Advanced",
            "",
            "| Action | Pillar | Effort | Impact |",
            "|--------|--------|--------|--------|",
        ])
        for bp in partial_scored[8:14]:
            display = self._pillar_display_name(bp.get("pillar", ""))
            lines.append(f"| Optimize: {bp.get('name', '')} | {display} | High | Medium |")
        lines.append("")
        return "\n".join(lines)

    def _render_appendices(
        self,
        collected_data: Dict[str, Any],
        audit_entries: List[AuditEntry],
    ) -> str:
        lines = [
            "---",
            "",
            "## Appendix A: Workspace Inventory Summary",
            "",
        ]

        # Summary table from collected data
        lines.append("| Resource | Count | Notes |")
        lines.append("|----------|:-----:|-------|")

        gov = collected_data.get("GovernanceCollector", {})
        compute = collected_data.get("ComputeCollector", {})
        ops = collected_data.get("OperationsCollector", {})
        sec = collected_data.get("SecurityCollector", {})
        ws = collected_data.get("WorkspaceCollector", {})

        lines.append(f"| Catalogs | {gov.get('catalog_count', 'N/A')} | Metastore: {gov.get('metastore_name', 'N/A')} |")
        lines.append(f"| External Locations | {gov.get('external_location_count', 'N/A')} | |")
        lines.append(f"| Storage Credentials | {gov.get('storage_credential_count', 'N/A')} | |")
        lines.append(f"| Running Clusters | {compute.get('running_clusters', compute.get('cluster_count', 'N/A'))} | |")
        lines.append(f"| SQL Warehouses | {compute.get('warehouse_count', 'N/A')} | |")
        lines.append(f"| Cluster Policies | {compute.get('policy_count', 'N/A')} | |")
        lines.append(f"| Instance Pools | {compute.get('pool_count', 'N/A')} | |")
        lines.append(f"| Jobs | {ops.get('job_count', 'N/A')} | |")
        lines.append(f"| DLT Pipelines | {ops.get('pipeline_count', 'N/A')} | |")
        lines.append(f"| Serving Endpoints | {ops.get('endpoint_count', 'N/A')} | |")
        lines.append(f"| Git Repos | {ops.get('repo_count', 'N/A')} | |")
        lines.append(f"| Secret Scopes | {ops.get('scope_count', 'N/A')} | |")
        lines.append(f"| Groups | {ops.get('group_count', 'N/A')} | |")
        lines.append(f"| IP Access Lists | {sec.get('ip_access_list_count', 'N/A')} | |")
        lines.append(f"| Workspace Objects | {ws.get('object_count', 'N/A')} | |")
        lines.append("")

        # Security configuration
        sec_settings = sec.get("security_settings", {})
        if sec_settings:
            lines.append("## Appendix B: Workspace Configuration")
            lines.append("")
            lines.append("| Setting | Value | Recommendation |")
            lines.append("|---------|-------|----------------|")
            for key, val in sec_settings.items():
                rec = self._security_recommendation(key, val)
                lines.append(f"| {key} | **{val}** | {rec} |")
            lines.append("")

        # Token info
        token_info = sec.get("token_info", {})
        if token_info:
            lines.append(f"Active tokens: {token_info.get('count', 'N/A')}")
            lines.append("")

        lines.extend([
            "## Appendix C: Assessment Methodology",
            "",
            "This assessment was conducted following the **Databricks Well-Architected Lakehouse Assessment Delivery Playbook**.",
            "",
            "**Data Collection Methods:**",
            "- Databricks CLI workspace queries",
            "- REST API calls for configuration inspection",
            "- Unity Catalog API for governance assessment",
            "",
            "**Scoring Methodology:**",
            "- **0 (Not Implemented):** Best practice not in place; no evidence of implementation",
            "- **1 (Partially Implemented):** Some aspects of the best practice are in place but not fully adopted",
            "- **2 (Fully Implemented):** Best practice fully adopted and actively maintained",
            "",
            f"Total API/CLI commands executed: {len(audit_entries)}",
            "",
        ])
        return "\n".join(lines)

    def _extract_pillar_findings(self, pillar: str, collected_data: Dict[str, Any]) -> List[tuple]:
        """Extract current-state findings from collected data for a pillar."""
        findings = []
        gov = collected_data.get("GovernanceCollector", {})
        compute = collected_data.get("ComputeCollector", {})
        sec = collected_data.get("SecurityCollector", {})
        ops = collected_data.get("OperationsCollector", {})
        ws = collected_data.get("WorkspaceCollector", {})

        if pillar == "Data & AI Governance":
            if gov.get("metastore_name"):
                findings.append(("Unity Catalog", f"Metastore: `{gov['metastore_name']}`", "Implemented"))
            catalog_count = gov.get("catalog_count", 0)
            if catalog_count:
                status = "Critical Gap" if catalog_count > 100 else ("Gap" if catalog_count > 20 else "Implemented")
                findings.append(("Catalog Count", f"**{catalog_count} catalogs**", status))
            if gov.get("external_location_count"):
                findings.append(("External Locations", f"{gov['external_location_count']} external locations", "Gap" if gov["external_location_count"] > 50 else "Implemented"))
            if gov.get("storage_credential_count"):
                findings.append(("Storage Credentials", f"{gov['storage_credential_count']} storage credentials", "Gap" if gov["storage_credential_count"] > 50 else "Implemented"))
            iso = gov.get("isolation_modes", [])
            if isinstance(iso, list) and "OPEN" in iso:
                findings.append(("Catalog Isolation", "OPEN isolation mode detected on catalogs", "Gap"))
            elif isinstance(iso, dict) and iso.get("OPEN", 0) > 0:
                findings.append(("Catalog Isolation", f"{iso['OPEN']} catalogs in OPEN mode", "Gap"))

        elif pillar == "Interoperability & Usability":
            wh_count = compute.get("warehouse_count", 0)
            if wh_count:
                findings.append(("SQL Warehouses", f"{wh_count} warehouses configured", "Implemented" if wh_count < 20 else "Gap"))
            policy_count = compute.get("policy_count", 0)
            if policy_count:
                findings.append(("Cluster Policies", f"{policy_count} policies", "Gap" if policy_count > 20 else "Implemented"))
            repos = ops.get("repo_count", 0)
            if repos:
                findings.append(("Git Integration", f"{repos} repositories connected", "Implemented"))

        elif pillar == "Operational Excellence":
            job_count = ops.get("job_count", 0)
            if job_count:
                findings.append(("Job Management", f"{job_count} jobs configured", "Implemented" if job_count > 0 else "Gap"))
            pipeline_count = ops.get("pipeline_count", 0)
            if pipeline_count:
                findings.append(("DLT Pipelines", f"{pipeline_count} pipelines", "Implemented"))
            init_status = ops.get("init_script_status", {})
            if init_status:
                disabled = init_status.get("DISABLED", 0)
                if disabled:
                    findings.append(("Global Init Scripts", f"{disabled} scripts DISABLED", "Gap"))
            untitled = ws.get("untitled_notebooks_count", 0)
            if untitled:
                findings.append(("Workspace Hygiene", f"{untitled} untitled notebooks", "Gap"))
            pool_count = compute.get("pool_count", 0)
            if pool_count:
                findings.append(("Instance Pools", f"{pool_count} pools configured", "Implemented"))

        elif pillar == "Security":
            sec_settings = sec.get("security_settings", {})
            dbfs = sec_settings.get("enableDbfsFileBrowser")
            if dbfs and str(dbfs).lower() == "true":
                findings.append(("DBFS File Browser", "**ENABLED** (security risk)", "Critical Gap"))
            ipl = sec.get("ip_access_list_count", 0)
            findings.append(("IP Access Lists", f"{ipl} lists configured", "Implemented" if ipl > 0 else "Gap"))
            token_info = sec.get("token_info", {})
            if token_info:
                findings.append(("Tokens", f"{token_info.get('count', 0)} active tokens", "Implemented"))
            scope_count = ops.get("scope_count", 0)
            if scope_count:
                findings.append(("Secret Scopes", f"{scope_count} scopes configured", "Implemented"))
            max_token = sec_settings.get("maxTokenLifetimeDays")
            if max_token:
                findings.append(("Token Lifetime", f"Max {max_token} days", "Gap" if int(max_token) > 30 else "Implemented"))

        elif pillar == "Reliability":
            pipeline_states = ops.get("pipeline_states", [])
            if pipeline_states:
                failed = sum(1 for p in pipeline_states if p.get("state") == "FAILED")
                if failed:
                    findings.append(("DLT Pipelines", f"**{failed} pipelines in FAILED state**", "Critical Gap"))
            cluster_count = compute.get("cluster_count", 0)
            if cluster_count:
                findings.append(("Compute", f"{cluster_count} clusters available", "Implemented"))
            endpoint_count = ops.get("endpoint_count", 0)
            if endpoint_count:
                findings.append(("Model Serving", f"{endpoint_count} serving endpoints", "Implemented"))

        elif pillar == "Performance":
            wh_configs = compute.get("warehouse_configs", [])
            if wh_configs:
                serverless = sum(1 for w in wh_configs if w.get("cluster_size"))
                findings.append(("SQL Warehouses", f"{len(wh_configs)} warehouses", "Implemented"))
            cluster_count = compute.get("cluster_count", 0)
            if cluster_count:
                findings.append(("Clusters", f"{cluster_count} clusters configured", "Implemented"))

        elif pillar == "Cost":
            cluster_count = compute.get("running_clusters", compute.get("cluster_count", 0))
            if cluster_count:
                status = "Critical Gap" if cluster_count > 10 else "Implemented"
                findings.append(("Running Clusters", f"**{cluster_count} clusters running**", status))
            wh_count = compute.get("warehouse_count", 0)
            if wh_count:
                status = "Gap" if wh_count > 20 else "Implemented"
                findings.append(("SQL Warehouses", f"{wh_count} warehouses (consolidation opportunity)", status))
            policy_count = compute.get("policy_count", 0)
            if policy_count:
                findings.append(("Cluster Policies", f"{policy_count} policies (cost controls needed)", "Gap" if policy_count > 20 else "Implemented"))

        return findings

    def _security_recommendation(self, key: str, val: Any) -> str:
        """Generate security recommendation for a given setting."""
        recommendations = {
            "enableDbfsFileBrowser": ("false", "Change to **false** — data exfiltration risk"),
            "enableResultsDownloading": ("false", "Consider **false** for production"),
            "enableExportNotebook": ("false", "Consider **false** for IP protection"),
            "enableIpAccessLists": ("true", "Good — maintain"),
            "maxTokenLifetimeDays": ("30", "Reduce to **30 days** or less"),
        }
        if key in recommendations:
            expected, rec = recommendations[key]
            if str(val).lower() == expected:
                return "Good — current value is recommended"
            return rec
        return ""

    def _maturity_from_score(self, score: float) -> str:
        if score >= 1.75:
            return "Optimized"
        if score >= 1.25:
            return "Established"
        if score >= 0.5:
            return "Developing"
        return "Beginning"
