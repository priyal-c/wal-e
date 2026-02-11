"""
Markdown reporter - Generates WAL_Assessment_Readout.md with full detailed assessment.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from .base import (
    BEST_PRACTICES,
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
        sections.append(self._render_title(workspace_host, assessment_date))
        sections.append(
            self._render_executive_summary(
                overall_score, maturity_level, pillar_scores
            )
        )

        # Per-pillar sections
        for pillar in PILLAR_ORDER:
            section = self._render_pillar_section(
                pillar,
                pillar_scores.get(pillar),
                best_practice_scores,
                collected_data,
            )
            if section:
                sections.append(section)

        # Remediation roadmap (4 phases)
        sections.append(
            self._render_remediation_roadmap(
                pillar_scores, best_practice_scores
            )
        )

        # Appendices
        sections.append(
            self._render_appendices(collected_data, audit_entries)
        )

        output_path.write_text("\n\n".join(sections), encoding="utf-8")
        return output_path

    def _render_title(self, workspace_host: str, assessment_date: str) -> str:
        return f"""# Well-Architected Lakehouse Assessment Readout

**Workspace:** `{workspace_host}`  
**Assessment Date:** {assessment_date}

---

"""

    def _render_executive_summary(
        self,
        overall_score: float,
        maturity_level: str,
        pillar_scores: Dict[str, float],
    ) -> str:
        lines = [
            "## Executive Summary",
            "",
            f"**Overall Score:** {self._format_score(overall_score)}",
            f"**Maturity Level:** {maturity_level}",
            "",
            "### Pillar Scores",
            "",
            "| Pillar | Score |",
            "|--------|-------|",
        ]
        for pillar in PILLAR_ORDER:
            score = pillar_scores.get(pillar)
            score_str = self._format_score(score) if score is not None else "N/A"
            lines.append(f"| {pillar} | {score_str} |")
        lines.append("")
        return "\n".join(lines)

    def _render_pillar_section(
        self,
        pillar: str,
        pillar_score: Optional[float],
        best_practice_scores: List[BestPracticeScore],
        collected_data: Dict[str, Any],
    ) -> str:
        lines = [
            f"## {pillar}",
            "",
            f"**Pillar Score:** {self._format_score(pillar_score)}",
            "",
            "### Findings",
            "",
        ]

        pillar_bps = [bp for bp in BEST_PRACTICES if bp["pillar"] == pillar]
        for bp in pillar_bps:
            match = self._lookup_best_practice_score(
                best_practice_scores, pillar, bp["best_practice"]
            )
            if match:
                score_val = match.get("score", 0)
                notes = match.get("finding_notes") or ""
                score_label = self._score_label(score_val)
                lines.append(f"- **{bp['best_practice']}** ({score_label})")
                if notes:
                    lines.append(f"  - {notes}")
                lines.append("")
            else:
                lines.append(f"- **{bp['best_practice']}** (Not scored)")
                lines.append("")

        # Recommendations
        lines.append("### Recommendations")
        lines.append("")
        low_scores = [
            m for m in best_practice_scores
            if (m.get("pillar") == pillar and float(m.get("score", 2)) < 1.5)
        ]
        if low_scores:
            for m in low_scores[:5]:  # Top 5 recommendations
                name = m.get("name", "Unknown")
                notes = m.get("finding_notes") or "Review and remediate."
                lines.append(f"- **{name}:** {notes}")
        else:
            lines.append("- No specific recommendations; maintain current practices.")
        lines.append("")
        return "\n".join(lines)

    def _score_label(self, score: Union[int, float]) -> str:
        try:
            s = float(score)
            if s >= 1.5:
                return "✓ Compliant"
            if s >= 0.5:
                return "○ Partial"
            return "✗ Needs improvement"
        except (TypeError, ValueError):
            return "N/A"

    def _render_remediation_roadmap(
        self,
        pillar_scores: Dict[str, float],
        best_practice_scores: List[BestPracticeScore],
    ) -> str:
        lines = [
            "## Remediation Roadmap",
            "",
            "### Phase 1: Foundation (Weeks 1–2)",
            "- Establish Unity Catalog as primary metastore",
            "- Configure cluster policies for consistency",
            "- Enable audit logging and basic monitoring",
            "",
            "### Phase 2: Governance & Security (Weeks 3–4)",
            "- Implement UC grants and access controls",
            "- Configure IP access lists and network security",
            "- Set up secret scopes for credentials",
            "",
            "### Phase 3: Operations & Reliability (Weeks 5–6)",
            "- Implement CI/CD for notebooks and jobs",
            "- Configure job retry and auto-scaling policies",
            "- Define backup and recovery procedures",
            "",
            "### Phase 4: Optimization (Weeks 7–8)",
            "- Optimize data layout (Z-ordering, liquid clustering)",
            "- Implement cost allocation tags and dashboards",
            "- Fine-tune warehouse and cluster sizing",
            "",
        ]
        return "\n".join(lines)

    def _render_appendices(
        self,
        collected_data: Dict[str, Any],
        audit_entries: List[AuditEntry],
    ) -> str:
        lines = [
            "---",
            "",
            "## Appendix A: Workspace Inventory",
            "",
        ]

        if collected_data:
            for key, value in collected_data.items():
                if isinstance(value, (list, dict)):
                    lines.append(f"### {key}")
                    lines.append("")
                    lines.append("```json")
                    try:
                        import json
                        lines.append(
                            json.dumps(value, indent=2, default=str)[:2000]
                        )
                        if len(json.dumps(value, default=str)) > 2000:
                            lines.append("... (truncated)")
                    except Exception:
                        lines.append(str(value)[:2000])
                    lines.append("```")
                    lines.append("")
                else:
                    lines.append(f"- **{key}:** {value}")
                    lines.append("")
        else:
            lines.append("*No collected data available.*")
            lines.append("")

        lines.extend([
            "## Appendix B: Configuration Details",
            "",
            "Configuration details are derived from the workspace inventory above.",
            "",
            "## Appendix C: Audit Trail Summary",
            "",
            f"Total API/CLI commands executed: {len(audit_entries)}",
            "",
        ])
        return "\n".join(lines)
