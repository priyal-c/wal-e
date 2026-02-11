"""
PPTX deck reporter - Generates WAL_Assessment_Presentation.pptx using python-pptx.

Produces a 15+ slide executive readout matching the original WAL assessment deck:
  1. Title
  2. Assessment Objective
  3. Overall Maturity Summary
  4. Workspace at a Glance
  5. Top 10 Critical Findings
  6-12. Pillar Deep Dives (Strengths / Critical Gaps / Recommendations)
  13. Remediation Roadmap
  14. Recommended Next Steps
  15. Assessment Summary Statistics
  16. Thank You
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from .base import (
    PILLAR_DISPLAY_NAMES,
    PILLAR_ORDER,
    AuditEntry,
    BaseReporter,
    BestPracticeScore,
    ScoredAssessment,
)

# Conditional import
try:
    from pptx import Presentation
    from pptx.util import Inches, Pt, Emu
    from pptx.dml.color import RGBColor
    from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
    from pptx.oxml.ns import qn

    PPTX_AVAILABLE = True
except ImportError:
    PPTX_AVAILABLE = False
    Presentation = None  # type: ignore

# ---------------------------------------------------------------------------
# Databricks-inspired color palette
# ---------------------------------------------------------------------------
DB_RED = RGBColor(0xFF, 0x38, 0x21) if PPTX_AVAILABLE else None       # Databricks red
DB_DARK = RGBColor(0x1B, 0x1F, 0x23) if PPTX_AVAILABLE else None      # Dark background
DB_DARKER = RGBColor(0x14, 0x17, 0x1A) if PPTX_AVAILABLE else None    # Darker bg
DB_WHITE = RGBColor(0xFF, 0xFF, 0xFF) if PPTX_AVAILABLE else None
DB_LIGHT = RGBColor(0xE8, 0xEA, 0xED) if PPTX_AVAILABLE else None     # Light text
DB_GRAY = RGBColor(0x9A, 0xA0, 0xA6) if PPTX_AVAILABLE else None      # Muted text
DB_GREEN = RGBColor(0x00, 0xA9, 0x72) if PPTX_AVAILABLE else None     # Score 2
DB_ORANGE = RGBColor(0xF5, 0x9E, 0x0B) if PPTX_AVAILABLE else None    # Score 1
DB_CRIT_RED = RGBColor(0xEF, 0x44, 0x44) if PPTX_AVAILABLE else None  # Score 0 / Critical
DB_BLUE = RGBColor(0x13, 0x6C, 0xB9) if PPTX_AVAILABLE else None      # Accent
TBL_HEADER = RGBColor(0x2D, 0x33, 0x3A) if PPTX_AVAILABLE else None   # Table header bg
TBL_ROW_ODD = RGBColor(0x22, 0x27, 0x2E) if PPTX_AVAILABLE else None  # Table odd row
TBL_ROW_EVEN = RGBColor(0x1B, 0x1F, 0x23) if PPTX_AVAILABLE else None # Table even row


class PPTXDeckReporter(BaseReporter):
    """Generates a polished PPTX executive readout presentation."""

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
            raise ImportError("python-pptx is not installed. Install with: pip install 'python-pptx>=1.0'")

        output_path = self._ensure_output_dir(output_dir) / self.output_filename

        prs = Presentation()
        prs.slide_width = Inches(13.333)
        prs.slide_height = Inches(7.5)

        # Extract all data
        pillar_scores = self._get_pillar_scores(scored_assessment)
        bps = self._get_best_practice_scores(scored_assessment)
        overall = self._get_overall_score(scored_assessment)
        maturity = self._get_maturity_level(scored_assessment)
        host = self._get_workspace_host(scored_assessment)
        date = self._get_assessment_date(scored_assessment)
        overall_pct = self._score_to_pct(overall)

        gov = collected_data.get("GovernanceCollector", {})
        compute = collected_data.get("ComputeCollector", {})
        ops = collected_data.get("OperationsCollector", {})
        sec = collected_data.get("SecurityCollector", {})
        ws = collected_data.get("WorkspaceCollector", {})

        # Build slides
        self._slide_title(prs, host, date, gov)
        self._slide_objective(prs)
        self._slide_maturity_summary(prs, pillar_scores, overall, overall_pct, maturity)
        self._slide_workspace_glance(prs, gov, compute, ops, sec, ws)
        self._slide_top_findings(prs, bps)

        for pillar in PILLAR_ORDER:
            self._slide_pillar_deep_dive(prs, pillar, pillar_scores, bps, collected_data)

        self._slide_roadmap(prs, bps)
        self._slide_next_steps(prs, sec, compute)
        self._slide_stats_summary(prs, bps, pillar_scores)
        self._slide_thank_you(prs, host)

        prs.save(str(output_path))
        return output_path

    # -----------------------------------------------------------------------
    # Slide-building helpers
    # -----------------------------------------------------------------------

    def _dark_bg(self, slide) -> None:
        """Set dark background on a slide."""
        bg = slide.background
        fill = bg.fill
        fill.solid()
        fill.fore_color.rgb = DB_DARK

    def _add_title_box(self, slide, text: str, top: float = 0.4, size: int = 32) -> None:
        tx = slide.shapes.add_textbox(Inches(0.8), Inches(top), Inches(11.5), Inches(0.8))
        p = tx.text_frame.paragraphs[0]
        p.text = text
        p.font.size = Pt(size)
        p.font.bold = True
        p.font.color.rgb = DB_RED

    def _add_subtitle(self, slide, text: str, top: float, color=None, size: int = 14, bold: bool = False) -> None:
        tx = slide.shapes.add_textbox(Inches(0.8), Inches(top), Inches(11.5), Inches(0.5))
        p = tx.text_frame.paragraphs[0]
        p.text = text
        p.font.size = Pt(size)
        p.font.bold = bold
        p.font.color.rgb = color or DB_LIGHT

    def _add_body_text(self, slide, text: str, left: float, top: float, width: float = 11.5, size: int = 12, color=None) -> None:
        tx = slide.shapes.add_textbox(Inches(left), Inches(top), Inches(width), Inches(0.4))
        tf = tx.text_frame
        tf.word_wrap = True
        p = tf.paragraphs[0]
        p.text = text
        p.font.size = Pt(size)
        p.font.color.rgb = color or DB_LIGHT

    def _add_bullet_list(self, slide, items: List[str], left: float, top: float, width: float = 5.5, size: int = 11, color=None) -> float:
        """Add a bulleted list. Returns y position after last item."""
        y = top
        for item in items:
            tx = slide.shapes.add_textbox(Inches(left), Inches(y), Inches(width), Inches(0.35))
            tf = tx.text_frame
            tf.word_wrap = True
            p = tf.paragraphs[0]
            p.text = f"\u2022  {item}"
            p.font.size = Pt(size)
            p.font.color.rgb = color or DB_LIGHT
            y += 0.35
        return y

    def _add_table(self, slide, rows_data: List[List[str]], headers: List[str],
                   left: float, top: float, width: float, col_widths: Optional[List[float]] = None) -> None:
        """Add a styled table to the slide."""
        n_rows = len(rows_data) + 1  # +1 for header
        n_cols = len(headers)
        tbl_shape = slide.shapes.add_table(n_rows, n_cols, Inches(left), Inches(top), Inches(width), Inches(0.3 * n_rows))
        tbl = tbl_shape.table

        # Set column widths
        if col_widths:
            for i, cw in enumerate(col_widths):
                tbl.columns[i].width = Inches(cw)

        # Header row
        for j, hdr in enumerate(headers):
            cell = tbl.cell(0, j)
            cell.text = hdr
            self._style_cell(cell, Pt(10), DB_WHITE, TBL_HEADER, bold=True)

        # Data rows
        for i, row in enumerate(rows_data):
            bg = TBL_ROW_ODD if i % 2 == 0 else TBL_ROW_EVEN
            for j, val in enumerate(row):
                cell = tbl.cell(i + 1, j)
                cell.text = str(val)
                self._style_cell(cell, Pt(9), DB_LIGHT, bg)

    def _style_cell(self, cell, font_size, font_color, bg_color, bold: bool = False) -> None:
        """Style a table cell."""
        cell.fill.solid()
        cell.fill.fore_color.rgb = bg_color
        for p in cell.text_frame.paragraphs:
            p.font.size = font_size
            p.font.color.rgb = font_color
            p.font.bold = bold
        cell.margin_left = Inches(0.08)
        cell.margin_right = Inches(0.08)
        cell.margin_top = Inches(0.04)
        cell.margin_bottom = Inches(0.04)

    def _score_color(self, score: float):
        if score >= 1.5:
            return DB_GREEN
        if score >= 0.5:
            return DB_ORANGE
        return DB_CRIT_RED

    def _maturity_label(self, score: float) -> str:
        if score >= 1.75:
            return "Optimized"
        if score >= 1.25:
            return "Established"
        if score >= 0.5:
            return "Developing"
        return "Beginning"

    # -----------------------------------------------------------------------
    # Slide 1: Title
    # -----------------------------------------------------------------------
    def _slide_title(self, prs, host: str, date: str, gov: dict) -> None:
        slide = prs.slides.add_slide(prs.slide_layouts[6])
        self._dark_bg(slide)

        # Title
        tx = slide.shapes.add_textbox(Inches(0.8), Inches(1.8), Inches(11.5), Inches(1.5))
        p = tx.text_frame.paragraphs[0]
        p.text = "Well-Architected Lakehouse"
        p.font.size = Pt(48)
        p.font.bold = True
        p.font.color.rgb = DB_WHITE
        p2 = tx.text_frame.add_paragraph()
        p2.text = "Assessment Readout"
        p2.font.size = Pt(48)
        p2.font.bold = True
        p2.font.color.rgb = DB_RED

        # Metadata table
        meta = [
            ["Customer Workspace", host],
            ["Metastore", gov.get("metastore_name", "N/A")],
            ["Assessment Date", date],
            ["Framework", "Databricks Well-Architected Framework"],
        ]
        y = 4.0
        for label, val in meta:
            tx_l = slide.shapes.add_textbox(Inches(0.8), Inches(y), Inches(3), Inches(0.35))
            tx_l.text_frame.paragraphs[0].text = label
            tx_l.text_frame.paragraphs[0].font.size = Pt(12)
            tx_l.text_frame.paragraphs[0].font.color.rgb = DB_GRAY

            tx_v = slide.shapes.add_textbox(Inches(4), Inches(y), Inches(8), Inches(0.35))
            tx_v.text_frame.paragraphs[0].text = str(val)
            tx_v.text_frame.paragraphs[0].font.size = Pt(12)
            tx_v.text_frame.paragraphs[0].font.bold = True
            tx_v.text_frame.paragraphs[0].font.color.rgb = DB_WHITE
            y += 0.4

    # -----------------------------------------------------------------------
    # Slide 2: Assessment Objective
    # -----------------------------------------------------------------------
    def _slide_objective(self, prs) -> None:
        slide = prs.slides.add_slide(prs.slide_layouts[6])
        self._dark_bg(slide)
        self._add_title_box(slide, "Assessment Objective")

        self._add_body_text(slide,
            "Evaluate the existing Databricks workspace architecture against industry standard "
            "best practices and provide actionable recommendations based on the Well-Architected "
            "Lakehouse framework.",
            0.8, 1.4, size=14)

        self._add_subtitle(slide, "Scope: All 7 pillars of the WAL framework", 2.2, bold=True, size=14)

        pillars = [
            "1. Data & AI Governance",
            "2. Interoperability & Usability",
            "3. Operational Excellence",
            "4. Security, Compliance & Privacy",
            "5. Reliability",
            "6. Performance Efficiency",
            "7. Cost Optimization",
        ]
        self._add_bullet_list(slide, pillars, 1.0, 2.8, size=12)

        self._add_subtitle(slide, "Methodology", 5.4, bold=True, size=14)
        self._add_body_text(slide,
            "Automated workspace inspection via Databricks CLI, REST APIs, Unity Catalog APIs, "
            "and workspace configuration analysis. All calls are read-only.",
            0.8, 5.9, size=12)

    # -----------------------------------------------------------------------
    # Slide 3: Overall Maturity Summary
    # -----------------------------------------------------------------------
    def _slide_maturity_summary(self, prs, pillar_scores, overall, overall_pct, maturity) -> None:
        slide = prs.slides.add_slide(prs.slide_layouts[6])
        self._dark_bg(slide)
        self._add_title_box(slide, "Overall Maturity Summary")

        # Big score
        tx = slide.shapes.add_textbox(Inches(0.8), Inches(1.4), Inches(3), Inches(1))
        p = tx.text_frame.paragraphs[0]
        p.text = f"{overall_pct:.0f}%"
        p.font.size = Pt(54)
        p.font.bold = True
        p.font.color.rgb = self._score_color(overall)

        self._add_body_text(slide, f"Overall Score: {overall:.2f}/2.0  |  Maturity: {maturity}", 0.8, 2.4, size=16, color=DB_GRAY)

        # Pillar scores table
        headers = ["Pillar", "Score (0-2)", "Percentage", "Maturity"]
        rows = []
        for pillar in PILLAR_ORDER:
            score = pillar_scores.get(pillar, 0)
            pct = self._score_to_pct(score)
            mat = self._maturity_label(score)
            display = self._pillar_display_name(pillar)
            rows.append([display, f"{score:.1f}", f"{pct:.0f}%", mat])

        self._add_table(slide, rows, headers, 0.8, 3.2, 11.5, col_widths=[5.0, 2.0, 2.0, 2.5])

        # Maturity legend
        self._add_body_text(slide, "Beginning: 0-25%  |  Developing: 25-63%  |  Established: 63-88%  |  Optimized: 88-100%", 0.8, 6.6, size=9, color=DB_GRAY)

    # -----------------------------------------------------------------------
    # Slide 4: Workspace at a Glance
    # -----------------------------------------------------------------------
    def _slide_workspace_glance(self, prs, gov, compute, ops, sec, ws) -> None:
        slide = prs.slides.add_slide(prs.slide_layouts[6])
        self._dark_bg(slide)
        self._add_title_box(slide, "Workspace at a Glance")

        # Resource inventory (left column)
        self._add_subtitle(slide, "Resource Inventory", 1.3, bold=True, size=14)
        clusters = compute.get("clusters", []) or []
        warehouses = compute.get("warehouses", []) or []

        inv_headers = ["Resource", "Count", "Health"]
        inv_rows = []

        catalog_count = gov.get("catalog_count", 0)
        health = "CRITICAL" if catalog_count > 100 else ("WARNING" if catalog_count > 20 else "OK")
        inv_rows.append(["Unity Catalog Catalogs", str(catalog_count), health])
        inv_rows.append(["External Locations", str(gov.get("external_location_count", 0)), "WARNING" if gov.get("external_location_count", 0) > 50 else "OK"])
        inv_rows.append(["Storage Credentials", str(gov.get("storage_credential_count", 0)), "WARNING" if gov.get("storage_credential_count", 0) > 50 else "OK"])
        inv_rows.append(["Cluster Policies", str(compute.get("policy_count", 0)), "CRITICAL" if compute.get("policy_count", 0) == 0 else "OK"])
        inv_rows.append(["Running Clusters", str(compute.get("running_clusters", len(clusters))), "CRITICAL" if compute.get("running_clusters", 0) > 10 else "OK"])
        inv_rows.append(["SQL Warehouses", str(compute.get("warehouse_count", len(warehouses))), "WARNING" if compute.get("warehouse_count", 0) > 20 else "OK"])
        inv_rows.append(["Instance Pools", str(compute.get("pool_count", 0)), "OK"])
        inv_rows.append(["DLT Pipelines", str(ops.get("pipeline_count", 0)), "OK"])
        inv_rows.append(["Serving Endpoints", str(ops.get("endpoint_count", 0)), "OK"])
        inv_rows.append(["Git Repos", str(ops.get("repo_count", 0)), "OK"])

        self._add_table(slide, inv_rows, inv_headers, 0.5, 1.8, 6.0, col_widths=[2.8, 1.2, 2.0])

        # Key configuration (right column)
        sec_settings = sec.get("security_settings", {}) or {}
        if sec_settings:
            tx = slide.shapes.add_textbox(Inches(7.0), Inches(1.3), Inches(5.5), Inches(0.4))
            p = tx.text_frame.paragraphs[0]
            p.text = "Key Configuration"
            p.font.size = Pt(14)
            p.font.bold = True
            p.font.color.rgb = DB_LIGHT

            conf_headers = ["Setting", "Current", "Recommended"]
            conf_rows = []
            dbfs = sec_settings.get("enableDbfsFileBrowser", "")
            conf_rows.append(["DBFS File Browser", str(dbfs).upper(), "DISABLE" if str(dbfs).lower() == "true" else "OK"])
            dl = sec_settings.get("enableResultsDownloading", "")
            conf_rows.append(["Results Download", str(dl).upper(), "EVALUATE"])
            exp = sec_settings.get("enableExportNotebook", "")
            conf_rows.append(["Notebook Export", str(exp).upper(), "EVALUATE"])
            ipl = sec_settings.get("enableIpAccessLists", "")
            conf_rows.append(["IP Access Lists", str(ipl).upper(), "MAINTAIN" if str(ipl).lower() == "true" else "ENABLE"])
            mtl = sec_settings.get("maxTokenLifetimeDays", "")
            rec = "OK" if mtl and int(mtl) <= 30 else "30 days"
            conf_rows.append(["Max Token Lifetime", f"{mtl} days" if mtl else "N/A", rec])

            ms = gov.get("metastore_summary", {})
            if isinstance(ms, dict):
                owner = ms.get("owner", "N/A")
                conf_rows.append(["Metastore Owner", str(owner), "Admin group" if "account" in str(owner).lower() else "OK"])

            iso = gov.get("isolation_modes", [])
            if isinstance(iso, list) and "OPEN" in iso:
                conf_rows.append(["Catalog Isolation", "OPEN", "ISOLATED"])

            self._add_table(slide, conf_rows, conf_headers, 7.0, 1.8, 5.8, col_widths=[2.4, 1.7, 1.7])

    # -----------------------------------------------------------------------
    # Slide 5: Top 10 Critical Findings
    # -----------------------------------------------------------------------
    def _slide_top_findings(self, prs, bps: List[BestPracticeScore]) -> None:
        slide = prs.slides.add_slide(prs.slide_layouts[6])
        self._dark_bg(slide)
        self._add_title_box(slide, "Top 10 Critical Findings")

        # Sort by score ascending (worst first), then by pillar
        sorted_bps = sorted(
            [bp for bp in bps if bp.get("score") is not None],
            key=lambda x: (float(x.get("score", 2)), x.get("pillar", "")),
        )[:10]

        headers = ["#", "Finding", "Pillar", "Risk"]
        rows = []
        for i, bp in enumerate(sorted_bps, 1):
            score = float(bp.get("score", 0))
            name = bp.get("name", "Unknown")
            notes = bp.get("finding_notes", "")
            finding_text = f"{name}: {notes}"
            if len(finding_text) > 80:
                finding_text = finding_text[:77] + "..."
            display = self._pillar_display_name(bp.get("pillar", ""))
            risk = "CRITICAL" if score == 0 else ("HIGH" if score <= 1 else "MEDIUM")
            rows.append([str(i), finding_text, display, risk])

        self._add_table(slide, rows, headers, 0.5, 1.3, 12.3, col_widths=[0.5, 6.5, 3.3, 2.0])

    # -----------------------------------------------------------------------
    # Slides 6-12: Pillar Deep Dives
    # -----------------------------------------------------------------------
    def _slide_pillar_deep_dive(self, prs, pillar: str, pillar_scores, bps, collected_data) -> None:
        slide = prs.slides.add_slide(prs.slide_layouts[6])
        self._dark_bg(slide)

        display = self._pillar_display_name(pillar)
        score = pillar_scores.get(pillar, 0)
        pct = self._score_to_pct(score)
        mat = self._maturity_label(score)
        self._add_title_box(slide, f"{display}")
        self._add_subtitle(slide, f"Score: {score:.1f}/2.0  ({pct:.0f}%)  \u2014  {mat}", 1.1, color=self._score_color(score), size=16, bold=True)

        pillar_bps = self._get_bps_for_pillar(bps, pillar)

        # Derive strengths (score == 2), gaps (score == 0), partial (score == 1)
        strengths = [bp for bp in pillar_bps if float(bp.get("score", 0)) == 2]
        critical_gaps = [bp for bp in pillar_bps if float(bp.get("score", 0)) == 0]
        partial = [bp for bp in pillar_bps if float(bp.get("score", 0)) == 1]

        # LEFT COLUMN: Strengths & Gaps
        y = 1.7
        if strengths:
            tx = slide.shapes.add_textbox(Inches(0.8), Inches(y), Inches(5.5), Inches(0.35))
            p = tx.text_frame.paragraphs[0]
            p.text = "Strengths"
            p.font.size = Pt(13)
            p.font.bold = True
            p.font.color.rgb = DB_GREEN
            y += 0.35
            strength_items = [f"{bp.get('name', '')}: {bp.get('finding_notes', '')}"[:70] for bp in strengths[:5]]
            y = self._add_bullet_list(slide, strength_items, 0.8, y, width=5.5, size=10, color=DB_LIGHT)
            y += 0.15

        if critical_gaps:
            tx = slide.shapes.add_textbox(Inches(0.8), Inches(y), Inches(5.5), Inches(0.35))
            p = tx.text_frame.paragraphs[0]
            p.text = "Critical Gaps"
            p.font.size = Pt(13)
            p.font.bold = True
            p.font.color.rgb = DB_CRIT_RED
            y += 0.35
            gap_items = [f"{bp.get('name', '')}: {bp.get('finding_notes', '')}"[:70] for bp in critical_gaps[:5]]
            y = self._add_bullet_list(slide, gap_items, 0.8, y, width=5.5, size=10, color=DB_LIGHT)
            y += 0.15

        if partial and y < 5.5:
            tx = slide.shapes.add_textbox(Inches(0.8), Inches(y), Inches(5.5), Inches(0.35))
            p = tx.text_frame.paragraphs[0]
            p.text = "Needs Improvement"
            p.font.size = Pt(13)
            p.font.bold = True
            p.font.color.rgb = DB_ORANGE
            y += 0.35
            partial_items = [f"{bp.get('name', '')}"[:50] for bp in partial[:5]]
            self._add_bullet_list(slide, partial_items, 0.8, y, width=5.5, size=10, color=DB_LIGHT)

        # RIGHT COLUMN: Key Recommendations
        tx = slide.shapes.add_textbox(Inches(7.0), Inches(1.7), Inches(5.5), Inches(0.35))
        p = tx.text_frame.paragraphs[0]
        p.text = "Key Recommendations"
        p.font.size = Pt(13)
        p.font.bold = True
        p.font.color.rgb = DB_WHITE

        # Generate recommendations from gaps + partial, prioritized
        recs = []
        for bp in critical_gaps:
            notes = bp.get("finding_notes", "")
            if notes:
                recs.append(notes[:75])
        for bp in partial[:5]:
            notes = bp.get("finding_notes", "")
            if notes and len(recs) < 6:
                recs.append(notes[:75])

        if not recs:
            recs = ["Maintain current best practices"]

        self._add_bullet_list(slide, recs[:6], 7.0, 2.1, width=5.5, size=10, color=DB_LIGHT)

        # BP Score table (bottom)
        bp_table_data = []
        for bp in pillar_bps:
            s = float(bp.get("score", 0))
            bp_table_data.append([bp.get("name", ""), f"{s:.0f}", bp.get("finding_notes", "")[:55]])

        if len(bp_table_data) <= 12:
            tbl_top = max(5.0, y + 0.3) if y < 5.0 else 5.0
            if tbl_top + 0.3 * (len(bp_table_data) + 1) <= 7.2:
                self._add_table(slide, bp_table_data, ["Best Practice", "Score", "Notes"],
                               0.5, tbl_top, 12.3, col_widths=[3.5, 1.0, 7.8])

    # -----------------------------------------------------------------------
    # Slide 13: Remediation Roadmap
    # -----------------------------------------------------------------------
    def _slide_roadmap(self, prs, bps) -> None:
        slide = prs.slides.add_slide(prs.slide_layouts[6])
        self._dark_bg(slide)
        self._add_title_box(slide, "Remediation Roadmap")

        zero = [bp for bp in bps if float(bp.get("score", 2)) == 0]
        partial = [bp for bp in bps if float(bp.get("score", 2)) == 1]

        # Phase 1 - Quick Wins (score=0 items)
        self._add_subtitle(slide, "Phase 1 (Week 1-2): Quick Wins", 1.3, color=DB_RED, bold=True, size=14)
        p1_items = [f"{bp.get('name', '')}: {bp.get('finding_notes', '')}"[:70] for bp in zero[:5]]
        if not p1_items:
            p1_items = ["No critical gaps found - focus on partial improvements"]
        y = self._add_bullet_list(slide, p1_items, 0.8, 1.8, size=10)

        # Phase 2 - Foundation
        y += 0.2
        self._add_subtitle(slide, "Phase 2 (Week 3-6): Foundation", y, color=DB_ORANGE, bold=True, size=14)
        p2_items = [f"{bp.get('name', '')}"[:50] for bp in zero[5:] + partial[:3]][:5]
        if not p2_items:
            p2_items = ["Continue strengthening governance and security controls"]
        y = self._add_bullet_list(slide, p2_items, 0.8, y + 0.4, size=10)

        # Phase 3 - Operational Maturity
        y += 0.2
        self._add_subtitle(slide, "Phase 3 (Week 7-12): Operational Maturity", y, color=DB_BLUE, bold=True, size=14)
        p3_items = [f"{bp.get('name', '')}"[:50] for bp in partial[3:8]]
        if not p3_items:
            p3_items = ["Standardize CI/CD, monitoring, and reliability patterns"]
        y = self._add_bullet_list(slide, p3_items, 0.8, y + 0.4, size=10)

        # Phase 4 - Optimization
        y += 0.2
        if y < 6.0:
            self._add_subtitle(slide, "Phase 4 (Week 13-20): Optimization", y, color=DB_GREEN, bold=True, size=14)
            p4_items = [f"{bp.get('name', '')}"[:50] for bp in partial[8:13]]
            if not p4_items:
                p4_items = ["Performance tuning, cost optimization, advanced DR"]
            self._add_bullet_list(slide, p4_items, 0.8, y + 0.4, size=10)

    # -----------------------------------------------------------------------
    # Slide 14: Recommended Next Steps
    # -----------------------------------------------------------------------
    def _slide_next_steps(self, prs, sec, compute) -> None:
        slide = prs.slides.add_slide(prs.slide_layouts[6])
        self._dark_bg(slide)
        self._add_title_box(slide, "Recommended Next Steps")

        sec_settings = sec.get("security_settings", {}) or {}

        # Immediate actions
        self._add_subtitle(slide, "Immediate Actions (This Week)", 1.3, bold=True, size=14)
        immediate = []
        if str(sec_settings.get("enableDbfsFileBrowser", "")).lower() == "true":
            immediate.append("Disable DBFS file browser (Workspace Admin > Settings, 5 minutes)")
        if str(sec_settings.get("maxTokenLifetimeDays", "")).strip() and int(sec_settings.get("maxTokenLifetimeDays", "90")) > 30:
            immediate.append(f"Reduce token lifetime from {sec_settings.get('maxTokenLifetimeDays')} to 30 days")
        if compute.get("policy_count", 0) == 0:
            immediate.append("Create cluster policies for cost controls and standardization")
        immediate.append("Prioritize findings based on business impact")
        immediate.append("Assign owners for Phase 1-4 initiatives")
        y = self._add_bullet_list(slide, immediate[:5], 0.8, 1.8, size=12)

        # Follow-up engagements
        y += 0.3
        self._add_subtitle(slide, "Follow-Up Engagements", y, bold=True, size=14)
        engagements = [
            ["Catalog Cleanup & Governance", "Define catalog strategy, naming conventions, lifecycle", "2-3 weeks"],
            ["Security Hardening", "IAM review, Private Link, SAT deployment", "3-4 weeks"],
            ["Platform Standardization", "Cluster policy templates, CI/CD, monitoring", "4-6 weeks"],
            ["Cost Optimization Workshop", "Tagging strategy, chargeback, resource consolidation", "2-3 weeks"],
            ["Disaster Recovery Planning", "DR design, cross-region replication, backup strategy", "3-4 weeks"],
        ]
        self._add_table(slide, engagements, ["Engagement", "Description", "Duration"],
                       0.5, y + 0.4, 12.3, col_widths=[3.5, 6.3, 2.5])

    # -----------------------------------------------------------------------
    # Slide 15: Assessment Summary Statistics
    # -----------------------------------------------------------------------
    def _slide_stats_summary(self, prs, bps, pillar_scores) -> None:
        slide = prs.slides.add_slide(prs.slide_layouts[6])
        self._dark_bg(slide)
        self._add_title_box(slide, "Assessment Summary Statistics")

        self._add_subtitle(slide, "Scored Best Practices by Pillar", 1.3, bold=True, size=14)

        headers = ["Pillar", "Total BPs", "Score 0", "Score 1", "Score 2", "Avg Score"]
        rows = []
        total_bps = 0
        total_0 = 0
        total_1 = 0
        total_2 = 0
        for pillar in PILLAR_ORDER:
            pillar_bps = self._get_bps_for_pillar(bps, pillar)
            cnt = len(pillar_bps)
            s0 = sum(1 for bp in pillar_bps if float(bp.get("score", 0)) == 0)
            s1 = sum(1 for bp in pillar_bps if float(bp.get("score", 0)) == 1)
            s2 = sum(1 for bp in pillar_bps if float(bp.get("score", 0)) == 2)
            avg = pillar_scores.get(pillar, 0)
            display = self._pillar_display_name(pillar)
            rows.append([display, str(cnt), str(s0), str(s1), str(s2), f"{avg:.2f}"])
            total_bps += cnt
            total_0 += s0
            total_1 += s1
            total_2 += s2

        total_avg = sum(pillar_scores.values()) / len(pillar_scores) if pillar_scores else 0
        rows.append(["TOTAL", str(total_bps), str(total_0), str(total_1), str(total_2), f"{total_avg:.2f}"])

        self._add_table(slide, rows, headers, 0.5, 1.8, 12.3, col_widths=[4.5, 1.5, 1.3, 1.3, 1.3, 2.4])

        # Summary callout
        pct_not_impl = (total_0 / total_bps * 100) if total_bps else 0
        pct_full = (total_2 / total_bps * 100) if total_bps else 0
        self._add_body_text(slide,
            f"{pct_not_impl:.0f}% of best practices are NOT implemented. "
            f"{pct_full:.0f}% are fully implemented.",
            0.8, 5.5, size=14, color=DB_ORANGE)

    # -----------------------------------------------------------------------
    # Slide 16: Thank You
    # -----------------------------------------------------------------------
    def _slide_thank_you(self, prs, host: str) -> None:
        slide = prs.slides.add_slide(prs.slide_layouts[6])
        self._dark_bg(slide)

        tx = slide.shapes.add_textbox(Inches(2), Inches(2.0), Inches(9.5), Inches(1.5))
        p = tx.text_frame.paragraphs[0]
        p.text = "Thank You"
        p.font.size = Pt(54)
        p.font.bold = True
        p.font.color.rgb = DB_RED
        p.alignment = PP_ALIGN.CENTER

        tx2 = slide.shapes.add_textbox(Inches(2), Inches(3.5), Inches(9.5), Inches(0.5))
        p2 = tx2.text_frame.paragraphs[0]
        p2.text = f"Well-Architected Lakehouse Assessment \u2014 {host}"
        p2.font.size = Pt(16)
        p2.font.color.rgb = DB_GRAY
        p2.alignment = PP_ALIGN.CENTER

        # Deliverables
        self._add_subtitle(slide, "Deliverables:", 4.5, bold=True, size=14)
        deliverables = [
            "Executive Readout Presentation (this deck)",
            "Detailed Assessment Report (WAL_Assessment_Readout.md)",
            "Scored Assessment Tool (WAL_Assessment_Scores.csv)",
            "Complete Audit Trail (WAL_Assessment_Audit_Report.md)",
        ]
        self._add_bullet_list(slide, deliverables, 2.5, 5.0, size=12)
