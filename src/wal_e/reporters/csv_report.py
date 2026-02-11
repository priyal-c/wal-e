"""
CSV reporter - Generates WAL_Assessment_Scores.csv with all 99 best practices.
"""

import csv
from pathlib import Path
from typing import Any, Dict, List, Union

from .base import (
    BEST_PRACTICES,
    AuditEntry,
    BaseReporter,
    BestPracticeScore,
    ScoredAssessment,
)


class CSVReporter(BaseReporter):
    """Generates WAL_Assessment_Scores.csv with columns: Pillar, Principle, Best Practice, Relevant (Y/N), Score (0-2), Finding/Notes."""

    def __init__(self):
        super().__init__("WAL_Assessment_Scores.csv")

    def generate(
        self,
        scored_assessment: ScoredAssessment,
        collected_data: Dict[str, Any],
        audit_entries: List[AuditEntry],
        output_dir: Union[str, Path],
    ) -> Path:
        output_path = self._ensure_output_dir(output_dir) / self.output_filename
        best_practice_scores = self._get_best_practice_scores(scored_assessment)

        rows: List[Dict[str, str]] = []
        for bp in BEST_PRACTICES:
            match = self._lookup_best_practice_score(
                best_practice_scores,
                bp["pillar"],
                bp["best_practice"],
            )
            if match:
                score_val = match.get("score", 0)
                try:
                    score_0_2 = min(2, max(0, float(score_val)))
                except (TypeError, ValueError):
                    score_0_2 = ""
                relevant = "Y"
                notes = str(match.get("finding_notes") or "")
            else:
                score_0_2 = ""
                relevant = "N"
                notes = ""

            rows.append({
                "Pillar": bp["pillar"],
                "Principle": bp["principle"],
                "Best Practice": bp["best_practice"],
                "Relevant": relevant,
                "Score (0-2)": str(score_0_2) if score_0_2 != "" else "",
                "Finding/Notes": notes,
            })

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "Pillar",
                    "Principle",
                    "Best Practice",
                    "Relevant",
                    "Score (0-2)",
                    "Finding/Notes",
                ],
            )
            writer.writeheader()
            writer.writerows(rows)

        return output_path
