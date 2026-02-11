"""
PPTX deck reporter - Generates WAL_Assessment_Presentation.pptx using python-pptx.
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

# Conditional import - skip if python-pptx not installed
try:
    from pptx import Presentation
    from pptx.util import Inches, Pt
    from pptx.dml.color import RGBColor

    PPTX_AVAILABLE = True
except ImportError:
    PPTX_AVAILABLE = False
    Presentation = None  # type: ignore


# Databricks branding colors (RGB)
DB_RED = RGBColor(0xE5, 0x47, 0x4C) if PPTX_AVAILABLE else None
DB_DARK = RGBColor(0x1A, 0x1A, 0x1A) if PPTX_AVAILABLE else None
DB_WHITE = RGBColor(0xFF, 0xFF, 0xFF) if PPTX_AVAILABLE else None
DB_GRAY = RGBColor(0xA3, 0xA3, 0xA3) if PPTX_AVAILABLE else None
DB_GREEN = RGBColor(0x22, 0xC5, 0x5E) if PPTX_AVAILABLE else None
DB_ORANGE = RGBColor(0xF5, 0x9E, 0x0B) if PPTX_AVAILABLE else None


class PPTXDeckReporter(BaseReporter):
    """Generates PPTX presentation with Databricks branding."""

    def __init__(self):
        super().__init__("WAL_Assessment_Presentation.pptx")

    def generate(
        self,
        scored_assessment: ScoredAssessment,
        collected_data: Dict[str, Any],
        audit_entries: List[AuditEntry],
        output_dir: Union[str, Path],
    ) -> Path:
        if not PPTX_AVAILABLE:
            raise ImportError(
                "python-pptx is not installed. Install with: pip install python-pptx"
            )

        output_path = self._ensure_output_dir(output_dir) / self.output_filename

        prs = Presentation()
        prs.slide_width = Inches(13.333)
        prs.slide_height = Inches(7.5)

        pillar_scores = self._get_pillar_scores(scored_assessment)
        best_practice_scores = self._get_best_practice_scores(scored_assessment)
        overall_score = self._get_overall_score(scored_assessment)
        maturity_level = self._get_maturity_level(scored_assessment)
        workspace_host = self._get_workspace_host(scored_assessment)
        assessment_date = self._get_assessment_date(scored_assessment)

        overall_pct = self._score_to_pct(overall_score)

        # Top findings
        sorted_findings = sorted(
            [bp for bp in best_practice_scores if bp.get("score") is not None],
            key=lambda x: float(x.get("score", 2)),
        )[:10]

        # Slide 1: Title
        self._add_title_slide(prs, workspace_host, assessment_date)

        # Slide 2: Executive Summary
        self._add_executive_slide(prs, overall_pct, maturity_level, pillar_scores)

        # Slide 3: Workspace Inventory
        self._add_workspace_slide(prs, collected_data)

        # Slide 4: Top Findings
        self._add_findings_slide(prs, sorted_findings)

        # Slides 5+: Pillar Deep Dives
        for pillar in PILLAR_ORDER:
            self._add_pillar_slide(prs, pillar, pillar_scores, best_practice_scores)

        # Roadmap
        self._add_roadmap_slide(prs)

        # Next Steps
        self._add_next_steps_slide(prs)

        # Thank You
        self._add_thank_you_slide(prs, workspace_host)

        prs.save(str(output_path))
        return output_path

    def _add_title_slide(
        self, prs: "Presentation", workspace_host: str, assessment_date: str
    ) -> None:
        slide_layout = prs.slide_layouts[6]  # Blank
        slide = prs.slides.add_slide(slide_layout)
        left = Inches(0.5)
        top = Inches(2)
        width = Inches(12)
        height = Inches(1.5)
        tx = slide.shapes.add_textbox(left, top, width, height)
        tf = tx.text_frame
        p = tf.paragraphs[0]
        p.text = "Well-Architected Lakehouse Assessment Readout"
        p.font.size = Pt(44)
        p.font.bold = True
        p.font.color.rgb = DB_RED

        p2 = tf.add_paragraph()
        p2.text = workspace_host
        p2.font.size = Pt(18)
        p2.font.color.rgb = DB_GRAY

        p3 = tf.add_paragraph()
        p3.text = assessment_date
        p3.font.size = Pt(14)
        p3.font.color.rgb = DB_GRAY

    def _add_executive_slide(
        self,
        prs: "Presentation",
        overall_pct: float,
        maturity_level: str,
        pillar_scores: Dict[str, float],
    ) -> None:
        slide_layout = prs.slide_layouts[6]
        slide = prs.slides.add_slide(slide_layout)
        left, top = Inches(0.5), Inches(0.5)
        tx = slide.shapes.add_textbox(left, top, Inches(12), Inches(1))
        p = tx.text_frame.paragraphs[0]
        p.text = "Executive Summary"
        p.font.size = Pt(28)
        p.font.bold = True
        p.font.color.rgb = DB_RED

        # Metric boxes
        tx2 = slide.shapes.add_textbox(Inches(0.5), Inches(1.5), Inches(3), Inches(1))
        tx2.text_frame.text = f"{overall_pct:.0f}%"
        tx2.text_frame.paragraphs[0].font.size = Pt(36)
        tx2.text_frame.paragraphs[0].font.bold = True
        tx2.text_frame.paragraphs[0].font.color.rgb = DB_RED

        tx3 = slide.shapes.add_textbox(Inches(0.5), Inches(2.2), Inches(3), Inches(0.5))
        tx3.text_frame.text = "Overall Score"
        tx3.text_frame.paragraphs[0].font.size = Pt(12)
        tx3.text_frame.paragraphs[0].font.color.rgb = DB_GRAY

        tx4 = slide.shapes.add_textbox(Inches(4), Inches(1.5), Inches(4), Inches(1))
        tx4.text_frame.text = maturity_level
        tx4.text_frame.paragraphs[0].font.size = Pt(24)
        tx4.text_frame.paragraphs[0].font.color.rgb = DB_WHITE

        tx5 = slide.shapes.add_textbox(Inches(4), Inches(2.2), Inches(4), Inches(0.5))
        tx5.text_frame.text = "Maturity Level"
        tx5.text_frame.paragraphs[0].font.size = Pt(12)
        tx5.text_frame.paragraphs[0].font.color.rgb = DB_GRAY

        # Pillar scores
        y = 3.0
        for pillar in PILLAR_ORDER:
            score = pillar_scores.get(pillar, 0)
            pct = self._score_to_pct(score)
            display = self._pillar_display_name(pillar)
            tx_p = slide.shapes.add_textbox(Inches(0.5), Inches(y), Inches(8), Inches(0.4))
            tx_p.text_frame.text = f"{display}: {pct:.0f}% ({score:.1f}/2.0)"
            tx_p.text_frame.paragraphs[0].font.size = Pt(12)
            tx_p.text_frame.paragraphs[0].font.color.rgb = DB_WHITE
            y += 0.45

    def _add_workspace_slide(
        self, prs: "Presentation", collected_data: Dict[str, Any]
    ) -> None:
        slide_layout = prs.slide_layouts[6]
        slide = prs.slides.add_slide(slide_layout)
        left, top = Inches(0.5), Inches(0.5)
        tx = slide.shapes.add_textbox(left, top, Inches(12), Inches(1))
        p = tx.text_frame.paragraphs[0]
        p.text = "Workspace Inventory"
        p.font.size = Pt(28)
        p.font.bold = True
        p.font.color.rgb = DB_RED

        y = 1.5
        gov = collected_data.get("GovernanceCollector", {})
        compute = collected_data.get("ComputeCollector", {})
        ops = collected_data.get("OperationsCollector", {})

        metrics = [
            ("Catalogs", gov.get("catalog_count", "N/A")),
            ("External Locations", gov.get("external_location_count", "N/A")),
            ("Storage Credentials", gov.get("storage_credential_count", "N/A")),
            ("Running Clusters", compute.get("running_clusters", compute.get("cluster_count", "N/A"))),
            ("SQL Warehouses", compute.get("warehouse_count", "N/A")),
            ("Cluster Policies", compute.get("policy_count", "N/A")),
            ("Jobs", ops.get("job_count", "N/A")),
            ("DLT Pipelines", ops.get("pipeline_count", "N/A")),
            ("Serving Endpoints", ops.get("endpoint_count", "N/A")),
            ("Git Repos", ops.get("repo_count", "N/A")),
        ]
        for label, value in metrics:
            tx_i = slide.shapes.add_textbox(Inches(0.5), Inches(y), Inches(12), Inches(0.4))
            tx_i.text_frame.text = f"{label}: {value}"
            tx_i.text_frame.paragraphs[0].font.size = Pt(12)
            tx_i.text_frame.paragraphs[0].font.color.rgb = DB_WHITE
            y += 0.45

    def _add_findings_slide(
        self, prs: "Presentation", findings: List[BestPracticeScore]
    ) -> None:
        slide_layout = prs.slide_layouts[6]
        slide = prs.slides.add_slide(slide_layout)
        left, top = Inches(0.5), Inches(0.5)
        tx = slide.shapes.add_textbox(left, top, Inches(12), Inches(1))
        p = tx.text_frame.paragraphs[0]
        p.text = "Top Findings"
        p.font.size = Pt(28)
        p.font.bold = True
        p.font.color.rgb = DB_RED

        y = 1.5
        for fp in findings:
            display = self._pillar_display_name(fp.get("pillar", ""))
            name = fp.get("name", "Unknown")
            score_val = fp.get("score", "-")
            text = f"\u2022 [{score_val}] {display}: {name}"
            tx_i = slide.shapes.add_textbox(Inches(0.5), Inches(y), Inches(12), Inches(0.5))
            tx_i.text_frame.text = text[:90] + ("..." if len(text) > 90 else "")
            tx_i.text_frame.paragraphs[0].font.size = Pt(11)
            tx_i.text_frame.paragraphs[0].font.color.rgb = DB_WHITE
            y += 0.5
        if not findings:
            tx_n = slide.shapes.add_textbox(Inches(0.5), Inches(y), Inches(12), Inches(0.5))
            tx_n.text_frame.text = "No findings recorded."
            tx_n.text_frame.paragraphs[0].font.color.rgb = DB_GRAY

    def _add_pillar_slide(
        self,
        prs: "Presentation",
        pillar: str,
        pillar_scores: Dict[str, float],
        best_practice_scores: List[BestPracticeScore],
    ) -> None:
        display = self._pillar_display_name(pillar)
        slide_layout = prs.slide_layouts[6]
        slide = prs.slides.add_slide(slide_layout)
        left, top = Inches(0.5), Inches(0.5)
        tx = slide.shapes.add_textbox(left, top, Inches(12), Inches(1))
        p = tx.text_frame.paragraphs[0]
        p.text = display
        p.font.size = Pt(28)
        p.font.bold = True
        p.font.color.rgb = DB_RED

        score = pillar_scores.get(pillar, 0)
        pct = self._score_to_pct(score)
        tx2 = slide.shapes.add_textbox(Inches(0.5), Inches(1.2), Inches(4), Inches(0.5))
        tx2.text_frame.text = f"Pillar Score: {pct:.0f}% ({score:.1f}/2.0)"
        tx2.text_frame.paragraphs[0].font.size = Pt(14)
        tx2.text_frame.paragraphs[0].font.color.rgb = DB_WHITE

        y = 2.0
        pillar_bps = self._get_bps_for_pillar(best_practice_scores, pillar)
        for bp in pillar_bps[:8]:
            text = f"\u2022 {bp.get('name', 'Unknown')} \u2014 Score: {bp.get('score', '-')}"
            tx_i = slide.shapes.add_textbox(Inches(0.5), Inches(y), Inches(12), Inches(0.5))
            tx_i.text_frame.text = text[:90] + ("..." if len(text) > 90 else "")
            tx_i.text_frame.paragraphs[0].font.size = Pt(11)
            tx_i.text_frame.paragraphs[0].font.color.rgb = DB_WHITE
            y += 0.5
        if not pillar_bps:
            tx_n = slide.shapes.add_textbox(Inches(0.5), Inches(y), Inches(12), Inches(0.5))
            tx_n.text_frame.text = "No practices scored for this pillar."
            tx_n.text_frame.paragraphs[0].font.color.rgb = DB_GRAY

    def _add_roadmap_slide(self, prs: "Presentation") -> None:
        slide_layout = prs.slide_layouts[6]
        slide = prs.slides.add_slide(slide_layout)
        tx = slide.shapes.add_textbox(Inches(0.5), Inches(0.5), Inches(12), Inches(1))
        p = tx.text_frame.paragraphs[0]
        p.text = "Remediation Roadmap"
        p.font.size = Pt(28)
        p.font.bold = True
        p.font.color.rgb = DB_RED

        phases = [
            ("Phase 1", "Foundation", "Unity Catalog, cluster policies, audit logging"),
            ("Phase 2", "Governance & Security", "UC grants, IP access, secret scopes"),
            ("Phase 3", "Operations & Reliability", "CI/CD, retry policies, backups"),
            ("Phase 4", "Optimization", "Data layout, cost tags, sizing"),
        ]
        y = 1.5
        for phase, title, desc in phases:
            tx_p = slide.shapes.add_textbox(Inches(0.5), Inches(y), Inches(12), Inches(0.5))
            tx_p.text_frame.text = f"{phase}: {title} \u2014 {desc}"
            tx_p.text_frame.paragraphs[0].font.size = Pt(12)
            tx_p.text_frame.paragraphs[0].font.color.rgb = DB_WHITE
            y += 0.7

    def _add_next_steps_slide(self, prs: "Presentation") -> None:
        slide_layout = prs.slide_layouts[6]
        slide = prs.slides.add_slide(slide_layout)
        tx = slide.shapes.add_textbox(Inches(0.5), Inches(0.5), Inches(12), Inches(1))
        p = tx.text_frame.paragraphs[0]
        p.text = "Next Steps"
        p.font.size = Pt(28)
        p.font.bold = True
        p.font.color.rgb = DB_RED

        steps = [
            "1. Prioritize findings based on business impact",
            "2. Assign owners for Phase 1\u20134 initiatives",
            "3. Schedule follow-up assessment in 90 days",
            "4. Leverage WAL-E for ongoing monitoring",
        ]
        y = 1.5
        for step in steps:
            tx_s = slide.shapes.add_textbox(Inches(0.5), Inches(y), Inches(12), Inches(0.5))
            tx_s.text_frame.text = step
            tx_s.text_frame.paragraphs[0].font.size = Pt(14)
            tx_s.text_frame.paragraphs[0].font.color.rgb = DB_WHITE
            y += 0.7

    def _add_thank_you_slide(self, prs: "Presentation", workspace_host: str) -> None:
        slide_layout = prs.slide_layouts[6]
        slide = prs.slides.add_slide(slide_layout)
        tx = slide.shapes.add_textbox(Inches(2), Inches(2.5), Inches(9), Inches(1.5))
        p = tx.text_frame.paragraphs[0]
        p.text = "Thank You"
        p.font.size = Pt(44)
        p.font.bold = True
        p.font.color.rgb = DB_RED
        p.alignment = 1  # Center

        tx2 = slide.shapes.add_textbox(Inches(2), Inches(4), Inches(9), Inches(0.5))
        p2 = tx2.text_frame.paragraphs[0]
        p2.text = f"Well-Architected Lakehouse Assessment \u2014 {workspace_host}"
        p2.font.size = Pt(14)
        p2.font.color.rgb = DB_GRAY
        p2.alignment = 1
