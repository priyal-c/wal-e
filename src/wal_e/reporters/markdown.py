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
        cloud = self._get_cloud_provider(scored_assessment)

        sections: List[str] = []

        # Title and Executive Summary
        sections.append(self._render_title(workspace_host, assessment_date, cloud, collected_data))
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

        # Unverified best practices
        sections.append(self._render_unverified_bps(best_practice_scores))

        # Appendices
        sections.append(self._render_appendices(collected_data, audit_entries))

        output_path.write_text("\n\n".join(sections), encoding="utf-8")
        return output_path

    def _render_title(self, workspace_host: str, assessment_date: str, cloud: str, collected_data: Dict[str, Any]) -> str:
        # Extract workspace metadata
        gov = collected_data.get("GovernanceCollector", {})
        metastore_name = gov.get("metastore_name", "N/A")
        catalog_count = gov.get("catalog_count", "N/A")
        cloud_display = self._cloud_display_name(cloud)

        return f"""# Well-Architected Lakehouse Assessment
## Final Readout

---

| **Field** | **Details** |
|---|---|
| **Workspace** | {workspace_host} |
| **Cloud Provider** | {cloud_display} |
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

    def _render_unverified_bps(self, best_practice_scores: List[BestPracticeScore]) -> str:
        """Render a section listing all BPs that WAL-E could not verify."""
        unverified = [bp for bp in best_practice_scores if not bp.get("verified", True)]
        if not unverified:
            return ""

        lines = [
            "## Requires Manual Verification",
            "",
            f"The following **{len(unverified)} best practices** could not be automatically assessed "
            "from the available API data or system tables. These require manual review by the customer "
            "and SA together.",
            "",
            "| # | Pillar | Best Practice | Reason |",
            "|---|--------|--------------|--------|",
        ]

        for i, bp in enumerate(sorted(unverified, key=lambda x: x.get("pillar", "")), 1):
            pillar = self._pillar_display_name(bp.get("pillar", ""))
            name = bp.get("name", "Unknown")
            notes = bp.get("finding_notes", "Not verifiable from API")
            lines.append(f"| {i} | {pillar} | {name} | {notes} |")

        lines.append("")
        lines.append("### How to Increase Coverage")
        lines.append("")
        lines.append("| Action | Impact |")
        lines.append("|--------|--------|")
        lines.append("| Run with **workspace admin** access | Unlocks security config settings |")
        lines.append("| Run with **metastore admin** access | Unlocks full catalog and governance data |")
        lines.append("| Use `--deep` mode with system tables | Adds 11 operational best practices (cost, performance, reliability, security) |")
        lines.append("| Manual review with SA | Addresses process/organizational checks (compliance, shared responsibility, SIEM) |")
        lines.append("")
        lines.append("---")
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

        # Rich detail lists from new collectors
        clusters = compute.get("clusters", []) or []
        warehouses = compute.get("warehouses", []) or []
        jobs = ops.get("jobs", []) or []
        pipelines = ops.get("pipelines", []) or ops.get("pipeline_states", []) or []
        sec_settings = sec.get("security_settings", {}) or {}

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
            wh_count = compute.get("warehouse_count", 0) or len(warehouses)
            if wh_count:
                # Show serverless/PRO breakdown
                serverless = sum(1 for w in warehouses if isinstance(w, dict) and w.get("enable_serverless_compute"))
                pro = sum(1 for w in warehouses if isinstance(w, dict) and w.get("warehouse_type") == "PRO")
                detail = f"{wh_count} warehouses"
                if serverless:
                    detail += f" ({serverless} serverless)"
                elif pro:
                    detail += f" ({pro} PRO)"
                findings.append(("SQL Warehouses", detail, "Implemented" if wh_count < 20 else "Gap"))
            policy_count = compute.get("policy_count", 0)
            if policy_count:
                names = compute.get("policy_names", [])
                detail = f"{policy_count} policies"
                if names:
                    detail += f" (e.g. {', '.join(names[:3])})"
                findings.append(("Cluster Policies", detail, "Gap" if policy_count > 20 else "Implemented"))
            elif policy_count == 0:
                findings.append(("Cluster Policies", "**No cluster policies defined**", "Critical Gap"))
            repos = ops.get("repo_count", 0)
            if repos:
                findings.append(("Git Integration", f"{repos} repositories connected", "Implemented"))

        elif pillar == "Operational Excellence":
            job_count = ops.get("job_count", 0) or len(jobs)
            if job_count:
                git_jobs = sum(1 for j in jobs if isinstance(j, dict) and j.get("has_git_source"))
                detail = f"{job_count} jobs configured"
                if git_jobs:
                    detail += f" ({git_jobs} with Git source)"
                findings.append(("Job Management", detail, "Implemented"))
            pipeline_count = ops.get("pipeline_count", 0) or len(pipelines)
            if pipeline_count:
                failed = sum(1 for p in pipelines if isinstance(p, dict) and p.get("state") == "FAILED")
                detail = f"{pipeline_count} pipelines"
                if failed:
                    detail += f" (**{failed} FAILED**)"
                findings.append(("DLT Pipelines", detail, "Implemented" if not failed else "Gap"))
            init_scripts = ops.get("init_scripts", []) or ops.get("init_script_status", [])
            if isinstance(init_scripts, list) and init_scripts:
                disabled = sum(1 for s in init_scripts if isinstance(s, dict) and s.get("enabled") is False)
                if disabled:
                    findings.append(("Global Init Scripts", f"{disabled} scripts DISABLED", "Gap"))
            untitled = ws.get("untitled_notebooks_count", 0)
            if untitled:
                findings.append(("Workspace Hygiene", f"{untitled} untitled notebooks", "Gap"))
            pool_count = compute.get("pool_count", 0)
            if pool_count:
                findings.append(("Instance Pools", f"{pool_count} pools configured", "Implemented"))

        elif pillar == "Security":
            # DBFS browser
            dbfs = sec_settings.get("enableDbfsFileBrowser")
            if dbfs:
                if str(dbfs).lower() == "true":
                    findings.append(("DBFS File Browser", "**ENABLED** — data exfiltration risk", "Critical Gap"))
                else:
                    findings.append(("DBFS File Browser", "Disabled", "Implemented"))
            # Results downloading
            dl = sec_settings.get("enableResultsDownloading")
            if dl:
                if str(dl).lower() == "true":
                    findings.append(("Results Downloading", "**ENABLED** — consider disabling for production", "Gap"))
                else:
                    findings.append(("Results Downloading", "Disabled", "Implemented"))
            # Notebook export
            export = sec_settings.get("enableExportNotebook")
            if export:
                if str(export).lower() == "true":
                    findings.append(("Notebook Export", "**ENABLED** — IP protection concern", "Gap"))
                else:
                    findings.append(("Notebook Export", "Disabled", "Implemented"))
            # Token lifetime
            max_token = sec_settings.get("maxTokenLifetimeDays")
            if max_token:
                try:
                    days = int(max_token)
                    status = "Implemented" if days <= 30 else ("Gap" if days <= 90 else "Critical Gap")
                    findings.append(("Token Lifetime", f"Max **{max_token} days**", status))
                except ValueError:
                    findings.append(("Token Lifetime", f"Set to {max_token}", "Gap"))
            # IP access lists
            ipl = sec.get("ip_access_list_count", 0)
            ip_enabled = str(sec_settings.get("enableIpAccessLists", "")).lower() == "true"
            if ipl > 0 and ip_enabled:
                findings.append(("IP Access Lists", f"{ipl} lists configured and **enabled**", "Implemented"))
            elif ipl > 0:
                findings.append(("IP Access Lists", f"{ipl} lists but **not enabled**", "Gap"))
            else:
                findings.append(("IP Access Lists", "**None configured**", "Gap"))
            # Tokens
            token_info = sec.get("token_info", {})
            if token_info:
                findings.append(("Active Tokens", f"{token_info.get('count', 0)} tokens", "Implemented"))
            # Secret scopes
            scope_count = ops.get("scope_count", 0)
            if scope_count:
                findings.append(("Secret Scopes", f"{scope_count} scopes configured", "Implemented"))

        elif pillar == "Reliability":
            # Pipelines
            if pipelines:
                failed = sum(1 for p in pipelines if isinstance(p, dict) and p.get("state") == "FAILED")
                if failed:
                    findings.append(("DLT Pipelines", f"**{failed} pipelines in FAILED state**", "Critical Gap"))
            # Clusters with Photon
            cluster_count = compute.get("cluster_count", 0) or len(clusters)
            if clusters:
                photon = sum(1 for c in clusters if isinstance(c, dict) and c.get("runtime_engine") == "PHOTON")
                findings.append(("Compute", f"{cluster_count} clusters ({photon} Photon-enabled)", "Implemented" if photon else "Gap"))
            elif cluster_count:
                findings.append(("Compute", f"{cluster_count} clusters", "Implemented"))
            # Jobs with retries
            if jobs:
                with_retries = sum(1 for j in jobs if isinstance(j, dict) and (j.get("max_retries") or 0) > 0)
                if with_retries:
                    findings.append(("Job Resilience", f"{with_retries}/{len(jobs)} jobs with retries configured", "Implemented"))
                else:
                    findings.append(("Job Resilience", "**No jobs have retries configured**", "Gap"))
            # Model serving
            endpoint_count = ops.get("endpoint_count", 0)
            if endpoint_count:
                findings.append(("Model Serving", f"{endpoint_count} serving endpoints", "Implemented"))

        elif pillar == "Performance":
            # Warehouses
            wh_count = compute.get("warehouse_count", 0) or len(warehouses)
            if warehouses:
                photon_wh = sum(1 for w in warehouses if isinstance(w, dict) and w.get("enable_photon"))
                serverless = sum(1 for w in warehouses if isinstance(w, dict) and w.get("enable_serverless_compute"))
                detail = f"{wh_count} warehouses"
                parts = []
                if photon_wh:
                    parts.append(f"{photon_wh} Photon")
                if serverless:
                    parts.append(f"{serverless} serverless")
                if parts:
                    detail += f" ({', '.join(parts)})"
                findings.append(("SQL Warehouses", detail, "Implemented"))
            elif wh_count:
                findings.append(("SQL Warehouses", f"{wh_count} warehouses", "Implemented"))
            # Clusters
            cluster_count = compute.get("cluster_count", 0) or len(clusters)
            if clusters:
                autoscale = sum(1 for c in clusters if isinstance(c, dict) and c.get("autoscale"))
                photon = sum(1 for c in clusters if isinstance(c, dict) and c.get("runtime_engine") == "PHOTON")
                detail = f"{cluster_count} clusters"
                parts = []
                if autoscale:
                    parts.append(f"{autoscale} with autoscaling")
                if photon:
                    parts.append(f"{photon} Photon")
                if parts:
                    detail += f" ({', '.join(parts)})"
                findings.append(("Clusters", detail, "Implemented"))
            elif cluster_count:
                findings.append(("Clusters", f"{cluster_count} clusters configured", "Implemented"))

        elif pillar == "Cost":
            running = compute.get("running_clusters", 0)
            cluster_count = compute.get("cluster_count", 0) or len(clusters)
            if cluster_count:
                # Auto-termination
                with_auto_term = sum(1 for c in clusters if isinstance(c, dict) and (c.get("auto_termination_minutes") or 0) > 0)
                status = "Critical Gap" if running > 10 else "Implemented"
                detail = f"**{cluster_count} clusters** ({running} running)"
                if with_auto_term:
                    detail += f", {with_auto_term} with auto-termination"
                else:
                    detail += ", **no auto-termination configured**"
                    status = "Gap" if status != "Critical Gap" else status
                findings.append(("Clusters", detail, status))
            # Tagging
            if clusters:
                with_tags = sum(1 for c in clusters if isinstance(c, dict) and len(c.get("custom_tags") or {}) > 0)
                if with_tags:
                    findings.append(("Cost Tags", f"{with_tags}/{len(clusters)} clusters tagged", "Implemented" if with_tags == len(clusters) else "Gap"))
                else:
                    findings.append(("Cost Tags", "**No clusters have custom tags**", "Gap"))
            # Warehouses
            wh_count = compute.get("warehouse_count", 0) or len(warehouses)
            if warehouses:
                with_autostop = sum(1 for w in warehouses if isinstance(w, dict) and (w.get("auto_stop_mins") or 0) > 0)
                serverless = sum(1 for w in warehouses if isinstance(w, dict) and w.get("enable_serverless_compute"))
                detail = f"{wh_count} warehouses"
                parts = []
                if with_autostop:
                    parts.append(f"{with_autostop} auto-stop")
                if serverless:
                    parts.append(f"{serverless} serverless")
                if parts:
                    detail += f" ({', '.join(parts)})"
                findings.append(("SQL Warehouses", detail, "Implemented" if wh_count < 20 else "Gap"))
            elif wh_count:
                findings.append(("SQL Warehouses", f"{wh_count} warehouses", "Gap" if wh_count > 20 else "Implemented"))
            # Policies
            policy_count = compute.get("policy_count", 0)
            if policy_count:
                findings.append(("Cluster Policies", f"{policy_count} policies for cost controls", "Implemented"))
            else:
                findings.append(("Cluster Policies", "**No policies** — cannot enforce cost controls", "Critical Gap"))

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
