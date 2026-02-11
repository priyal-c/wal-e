"""
HTML deck reporter - Generates WAL_Assessment_Presentation.html (16-slide presentation).
"""

from pathlib import Path
from typing import Any, Dict, List, Union

from .base import (
    PILLAR_ORDER,
    AuditEntry,
    BaseReporter,
    BestPracticeScore,
    ScoredAssessment,
)


class HTMLDeckReporter(BaseReporter):
    """Generates a beautiful HTML presentation with Databricks branding."""

    def __init__(self):
        super().__init__("WAL_Assessment_Presentation.html")

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
        cloud_display = self._cloud_display_name(cloud)
        cloud_short = self._cloud_short_name(cloud)

        # Top 10 findings (lowest scores first)
        sorted_findings = sorted(
            [bp for bp in best_practice_scores if bp.get("score") is not None],
            key=lambda x: float(x.get("score", 2)),
        )[:10]

        html = self._build_html(
            workspace_host=workspace_host,
            assessment_date=assessment_date,
            overall_score=overall_score,
            maturity_level=maturity_level,
            pillar_scores=pillar_scores,
            best_practice_scores=best_practice_scores,
            sorted_findings=sorted_findings,
            collected_data=collected_data,
            cloud_display=cloud_display,
            cloud_short=cloud_short,
        )

        output_path.write_text(html, encoding="utf-8")
        return output_path

    def _build_html(
        self,
        workspace_host: str,
        assessment_date: str,
        overall_score: float,
        maturity_level: str,
        pillar_scores: Dict[str, float],
        best_practice_scores: List[BestPracticeScore],
        sorted_findings: List[BestPracticeScore],
        collected_data: Dict[str, Any],
        cloud_display: str = "Unknown Cloud",
        cloud_short: str = "Unknown",
    ) -> str:
        css = self._get_css()
        slides: List[str] = []

        overall_pct = self._score_to_pct(overall_score)

        # Slide 1: Title
        slides.append(f'''
<section class="slide title-slide">
  <h1>Well-Architected Lakehouse</h1>
  <h2>Assessment Readout</h2>
  <p class="meta">{workspace_host}</p>
  <p class="meta"><span class="badge badge-blue">{cloud_short}</span> {cloud_display}</p>
  <p class="meta">{assessment_date}</p>
</section>''')

        # Slide 2: Agenda
        slides.append('''
<section class="slide">
  <h2>Agenda</h2>
  <ul>
    <li>Methodology & Framework</li>
    <li>Executive Summary</li>
    <li>Workspace Metrics</li>
    <li>Top Findings</li>
    <li>Pillar Deep Dives</li>
    <li>Remediation Roadmap</li>
    <li>Next Steps</li>
  </ul>
</section>''')

        # Slide 3: Methodology
        bp_count = len(best_practice_scores)
        slides.append(f'''
<section class="slide">
  <h2>Methodology</h2>
  <p>Assessment against the <strong>Well-Architected Lakehouse Framework</strong> with:</p>
  <ul>
    <li>7 pillars, {bp_count} best practices</li>
    <li>Read-only API/CLI data collection (21 API calls)</li>
    <li>0-2 scoring per best practice</li>
    <li>Evidence-based findings</li>
  </ul>
</section>''')

        # Slide 4: Executive Summary
        badge_class = f"badge-{self._score_badge_color(overall_pct)}"
        pillar_table = ""
        for pillar in PILLAR_ORDER:
            score = pillar_scores.get(pillar, 0)
            pct = self._score_to_pct(score)
            bar_class = f"bar-{self._score_badge_color(pct)}"
            display = self._pillar_display_name(pillar)
            pillar_table += f'''
    <tr>
      <td>{display}</td>
      <td>
        <div class="score-bar"><span class="bar-fill {bar_class}" style="width:{pct}%"></span><span class="bar-label">{pct:.0f}%</span></div>
      </td>
    </tr>'''

        slides.append(f'''
<section class="slide">
  <h2>Executive Summary</h2>
  <div class="metric-cards">
    <div class="metric-card">
      <span class="metric-value {badge_class}">{overall_pct:.0f}%</span>
      <span class="metric-label">Overall Score</span>
    </div>
    <div class="metric-card">
      <span class="metric-value">{maturity_level}</span>
      <span class="metric-label">Maturity Level</span>
    </div>
  </div>
  <table class="pillar-table">
    <thead><tr><th>Pillar</th><th>Score</th></tr></thead>
    <tbody>{pillar_table}
    </tbody>
  </table>
</section>''')

        # Slide 5: Workspace Metrics
        metrics = self._extract_workspace_metrics(collected_data)
        # Prepend cloud as first metric card
        metrics = {"Cloud": cloud_short, **metrics}
        metric_cards = "".join(
            f'<div class="metric-card"><span class="metric-value">{v}</span><span class="metric-label">{k}</span></div>'
            for k, v in metrics.items()
        )
        slides.append(f'''
<section class="slide">
  <h2>Workspace Metrics</h2>
  <div class="metric-cards">{metric_cards}
  </div>
</section>''')

        # Slide 6: Top 10 Findings
        finding_items = ""
        for fp in sorted_findings:
            score_val = float(fp.get("score", 0))
            pct = self._score_to_pct(score_val)
            badge = f"badge-{self._score_badge_color(pct)}"
            display = self._pillar_display_name(fp.get("pillar", ""))
            finding_items += f'''
    <li><span class="badge {badge}">{score_val:.0f}</span> {display} — {fp.get("name", "")}</li>'''
        if not finding_items:
            finding_items = "<li><em>No findings recorded</em></li>"

        slides.append(f'''
<section class="slide">
  <h2>Top 10 Findings</h2>
  <ul class="findings-list">{finding_items}
  </ul>
</section>''')

        # Slides 7+: Pillar Deep Dives (all 7 pillars, 4 per slide max BPs)
        for pillar in PILLAR_ORDER:
            score = pillar_scores.get(pillar, 0)
            pct = self._score_to_pct(score)
            badge_class = f"badge-{self._score_badge_color(pct)}"
            display = self._pillar_display_name(pillar)
            pillar_bps = self._get_bps_for_pillar(best_practice_scores, pillar)
            items = "".join(
                f'<li><span class="badge badge-{self._score_badge_color(self._score_to_pct(bp.get("score", 0)))}">{bp.get("score", "-")}</span> {bp.get("name", "")}: {(bp.get("finding_notes", "") or "")[:80]}</li>'
                for bp in pillar_bps[:8]
            )
            if not items:
                items = "<li><em>No practices scored for this pillar</em></li>"
            slides.append(f'''
<section class="slide">
  <h2>{display}</h2>
  <p class="pillar-score"><span class="badge {badge_class}">{pct:.0f}%</span> ({score:.1f}/2.0)</p>
  <ul class="findings-list">{items}
  </ul>
</section>''')

        # Remediation Roadmap
        slides.append('''
<section class="slide">
  <h2>Remediation Roadmap</h2>
  <div class="roadmap-phases">
    <div class="phase-box">
      <h3>Phase 1</h3>
      <p>Foundation</p>
      <p class="phase-desc">Unity Catalog, cluster policies, audit logging</p>
    </div>
    <div class="phase-box">
      <h3>Phase 2</h3>
      <p>Governance & Security</p>
      <p class="phase-desc">UC grants, IP access, secret scopes</p>
    </div>
    <div class="phase-box">
      <h3>Phase 3</h3>
      <p>Operations & Reliability</p>
      <p class="phase-desc">CI/CD, retry policies, backup procedures</p>
    </div>
    <div class="phase-box">
      <h3>Phase 4</h3>
      <p>Optimization</p>
      <p class="phase-desc">Data layout, cost tags, sizing</p>
    </div>
  </div>
</section>''')

        # Next Steps
        slides.append('''
<section class="slide">
  <h2>Next Steps</h2>
  <ol>
    <li>Prioritize findings based on business impact</li>
    <li>Assign owners for Phase 1\u20134 initiatives</li>
    <li>Schedule follow-up assessment in 90 days</li>
    <li>Leverage WAL-E for ongoing monitoring</li>
  </ol>
</section>''')

        # Thank You
        slides.append(f'''
<section class="slide title-slide">
  <h1>Thank You</h1>
  <p class="meta">Well-Architected Lakehouse Assessment</p>
  <p class="meta">{workspace_host}</p>
</section>''')

        body = "\n".join(slides)
        return f'''<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>WAL Assessment - {workspace_host}</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <style>{css}</style>
</head>
<body>
{body}
</body>
</html>'''

    def _get_css(self) -> str:
        return """
:root {
  --db-dark: #1a1a1a;
  --db-darker: #0d0d0d;
  --db-red: #e5474c;
  --db-red-dim: #b8383c;
  --db-orange: #f59e0b;
  --db-green: #22c55e;
  --db-text: #e5e5e5;
  --db-muted: #a3a3a3;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
  background: var(--db-darker);
  color: var(--db-text);
  line-height: 1.6;
}
.slide {
  min-height: 100vh;
  padding: 3rem 4rem;
  display: flex;
  flex-direction: column;
  justify-content: center;
}
.slide h1 { font-size: 2.5rem; margin-bottom: 0.5rem; }
.slide h2 {
  font-size: 1.75rem;
  color: var(--db-red);
  margin-bottom: 1.5rem;
  font-weight: 600;
}
.slide h3 { font-size: 1rem; margin-bottom: 0.25rem; }
.slide ul, .slide ol { margin-left: 1.5rem; margin-top: 0.5rem; }
.slide li { margin-bottom: 0.5rem; }
.title-slide { text-align: center; }
.title-slide h1 { font-size: 3rem; }
.title-slide h2 { font-size: 1.5rem; color: var(--db-muted); font-weight: 400; }
.title-slide .meta { color: var(--db-muted); font-size: 0.95rem; margin-top: 0.5rem; }
.metric-cards {
  display: flex;
  flex-wrap: wrap;
  gap: 1.5rem;
  margin-bottom: 2rem;
}
.metric-card {
  background: var(--db-dark);
  border: 1px solid #333;
  border-radius: 8px;
  padding: 1.25rem 1.5rem;
  min-width: 140px;
  text-align: center;
}
.metric-value {
  display: block;
  font-size: 1.75rem;
  font-weight: 700;
}
.metric-value.badge-green { color: var(--db-green); }
.metric-value.badge-orange { color: var(--db-orange); }
.metric-value.badge-red { color: var(--db-red); }
.metric-label {
  display: block;
  font-size: 0.8rem;
  color: var(--db-muted);
  margin-top: 0.25rem;
}
.badge {
  display: inline-block;
  padding: 0.2rem 0.5rem;
  border-radius: 4px;
  font-size: 0.8rem;
  font-weight: 600;
  margin-right: 0.5rem;
}
.badge-red { background: var(--db-red); color: white; }
.badge-orange { background: var(--db-orange); color: black; }
.badge-green { background: var(--db-green); color: black; }
.badge-blue { background: #136cb9; color: white; }
.pillar-table { width: 100%; border-collapse: collapse; margin-top: 1rem; }
.pillar-table th, .pillar-table td { padding: 0.5rem; text-align: left; border-bottom: 1px solid #333; }
.pillar-table th { color: var(--db-muted); font-weight: 500; }
.score-bar {
  background: #333;
  border-radius: 4px;
  height: 24px;
  position: relative;
  overflow: hidden;
}
.bar-fill { display: block; height: 100%; border-radius: 4px; transition: width 0.3s; }
.bar-red { background: var(--db-red); }
.bar-orange { background: var(--db-orange); }
.bar-green { background: var(--db-green); }
.bar-label { position: absolute; right: 8px; top: 50%; transform: translateY(-50%); font-size: 0.8rem; font-weight: 600; }
.findings-list { list-style: none; margin-left: 0; }
.findings-list li { margin-bottom: 0.75rem; padding: 0.25rem 0; }
.roadmap-phases {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 1rem;
  margin-top: 1rem;
}
.phase-box {
  background: var(--db-dark);
  border-left: 4px solid var(--db-red);
  padding: 1rem;
  border-radius: 4px;
}
.phase-desc { font-size: 0.9rem; color: var(--db-muted); margin-top: 0.5rem; }
.pillar-score { margin-bottom: 1rem; }
"""

    def _extract_workspace_metrics(self, collected_data: Dict[str, Any]) -> Dict[str, str]:
        """Extract displayable metrics from collected_data."""
        metrics: Dict[str, str] = {}
        if not collected_data:
            return {"Workspace": "N/A"}
        gov = collected_data.get("GovernanceCollector", {})
        compute = collected_data.get("ComputeCollector", {})
        ops = collected_data.get("OperationsCollector", {})
        sec = collected_data.get("SecurityCollector", {})

        if gov.get("catalog_count"):
            metrics["Catalogs"] = str(gov["catalog_count"])
        if gov.get("external_location_count"):
            metrics["External Locations"] = str(gov["external_location_count"])
        if compute.get("cluster_count"):
            metrics["Clusters"] = str(compute["cluster_count"])
        if compute.get("warehouse_count"):
            metrics["SQL Warehouses"] = str(compute["warehouse_count"])
        if compute.get("policy_count"):
            metrics["Cluster Policies"] = str(compute["policy_count"])
        if ops.get("job_count"):
            metrics["Jobs"] = str(ops["job_count"])
        if ops.get("pipeline_count"):
            metrics["DLT Pipelines"] = str(ops["pipeline_count"])
        if ops.get("endpoint_count"):
            metrics["Serving Endpoints"] = str(ops["endpoint_count"])
        if ops.get("repo_count"):
            metrics["Git Repos"] = str(ops["repo_count"])
        if sec.get("ip_access_list_count"):
            metrics["IP Access Lists"] = str(sec["ip_access_list_count"])
        return metrics
