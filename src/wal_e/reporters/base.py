"""
Base reporter class and shared utilities for WAL-E report generation.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, TypedDict, Union


# ---------------------------------------------------------------------------
# Type definitions for ScoredAssessment and related structures
# ---------------------------------------------------------------------------


class BestPracticeScore(TypedDict, total=False):
    """A scored best practice from the assessment."""

    name: str
    pillar: str
    principle: str
    score: float
    finding_notes: Optional[str]


class ScoredAssessment(TypedDict, total=False):
    """The scored assessment result from the scoring engine."""

    pillar_scores: Dict[str, float]
    best_practice_scores: List[BestPracticeScore]
    overall_score: float
    maturity_level: str
    workspace_host: str
    assessment_date: str
    cloud_provider: str


class AuditEntry(TypedDict):
    """A single audit log entry from data collection."""

    command: str
    output: str
    timestamp: str
    duration: Optional[float]


# Pillar display order — matches the scoring engine pillar names.
# The CLI applies aliases (e.g. "Security" → "Security, Compliance & Privacy")
# before the scored data reaches reporters, so we list the *display* names here.
PILLAR_ORDER: List[str] = [
    "Data & AI Governance",
    "Interoperability & Usability",
    "Operational Excellence",
    "Security",
    "Reliability",
    "Performance",
    "Cost",
]

# Display-friendly pillar names for reports/presentations
PILLAR_DISPLAY_NAMES: Dict[str, str] = {
    "Data & AI Governance": "Data & AI Governance",
    "Interoperability & Usability": "Interoperability & Usability",
    "Operational Excellence": "Operational Excellence",
    "Security": "Security, Compliance & Privacy",
    "Reliability": "Reliability",
    "Performance": "Performance Efficiency",
    "Cost": "Cost Optimization",
}


# ---------------------------------------------------------------------------
# Base Reporter
# ---------------------------------------------------------------------------


class BaseReporter:
    """Base class for all WAL-E reporters."""

    def __init__(self, output_filename: str):
        self.output_filename = output_filename

    def generate(
        self,
        scored_assessment: ScoredAssessment,
        collected_data: Dict[str, Any],
        audit_entries: List[AuditEntry],
        output_dir: Union[str, Path],
    ) -> Path:
        raise NotImplementedError("Subclasses must implement generate()")

    # -----------------------------------------------------------------------
    # Helper methods for formatting
    # -----------------------------------------------------------------------

    def _get_pillar_scores(self, scored_assessment: ScoredAssessment) -> Dict[str, float]:
        return scored_assessment.get("pillar_scores") or {}

    def _get_best_practice_scores(
        self, scored_assessment: ScoredAssessment
    ) -> List[BestPracticeScore]:
        return scored_assessment.get("best_practice_scores") or []

    def _get_overall_score(self, scored_assessment: ScoredAssessment) -> float:
        return float(scored_assessment.get("overall_score") or 0)

    def _get_maturity_level(self, scored_assessment: ScoredAssessment) -> str:
        return scored_assessment.get("maturity_level") or "Not Assessed"

    def _get_workspace_host(self, scored_assessment: ScoredAssessment) -> str:
        return scored_assessment.get("workspace_host") or "Unknown"

    def _get_assessment_date(self, scored_assessment: ScoredAssessment) -> str:
        return scored_assessment.get("assessment_date") or "Unknown"

    def _get_cloud_provider(self, scored_assessment: ScoredAssessment) -> str:
        return scored_assessment.get("cloud_provider") or "unknown"

    def _cloud_display_name(self, cloud: str) -> str:
        """Return human-friendly cloud name."""
        return {
            "aws": "Amazon Web Services (AWS)",
            "azure": "Microsoft Azure",
            "gcp": "Google Cloud Platform (GCP)",
        }.get(cloud, "Unknown Cloud")

    def _cloud_short_name(self, cloud: str) -> str:
        """Return short cloud name for tables."""
        return {"aws": "AWS", "azure": "Azure", "gcp": "GCP"}.get(cloud, "Unknown")

    def _format_score(self, score: Optional[Union[int, float]]) -> str:
        """Format score from 0-2 scale as a percentage (0-100%)."""
        if score is None:
            return "N/A"
        try:
            pct = (float(score) / 2.0) * 100
            return f"{pct:.0f}%"
        except (TypeError, ValueError):
            return "N/A"

    def _format_score_raw(self, score: Optional[Union[int, float]]) -> str:
        """Format score as raw 0-2 value."""
        if score is None:
            return "N/A"
        try:
            return f"{float(score):.1f}"
        except (TypeError, ValueError):
            return "N/A"

    def _score_to_pct(self, score: Optional[Union[int, float]]) -> float:
        """Convert 0-2 score to 0-100 percentage."""
        if score is None:
            return 0.0
        try:
            return (float(score) / 2.0) * 100
        except (TypeError, ValueError):
            return 0.0

    def _format_score_bar(self, score: float, width: int = 20) -> str:
        """Format score as a simple text bar (0-2 scale converted to bar)."""
        try:
            pct = min(100, max(0, (float(score) / 2.0) * 100))
        except (TypeError, ValueError):
            return "?" * width
        filled = int(width * pct / 100)
        return "█" * filled + "░" * (width - filled)

    def _maturity_color(self, maturity: str) -> str:
        m = (maturity or "").lower()
        if "optimized" in m or "established" in m:
            return "green"
        if "developing" in m:
            return "orange"
        return "red"

    def _score_badge_color(self, score_pct: float) -> str:
        """Return color for score badge (expects 0-100 percentage)."""
        if score_pct >= 70:
            return "green"
        if score_pct >= 40:
            return "orange"
        return "red"

    def _pillar_display_name(self, pillar: str) -> str:
        """Get the user-friendly display name for a pillar."""
        return PILLAR_DISPLAY_NAMES.get(pillar, pillar)

    def _get_bps_for_pillar(
        self,
        best_practice_scores: List[BestPracticeScore],
        pillar: str,
    ) -> List[BestPracticeScore]:
        """Get all scored best practices for a given pillar."""
        return [
            bp for bp in best_practice_scores
            if (bp.get("pillar") or "").strip() == pillar
        ]

    def _ensure_output_dir(self, output_dir: Union[str, Path]) -> Path:
        path = Path(output_dir)
        path.mkdir(parents=True, exist_ok=True)
        return path
