"""
CSV reporter - Generates WAL_Assessment_Scores.csv with all scored best practices.
"""

import csv
from pathlib import Path
from typing import Any, Dict, List, Union

from .base import (
    PILLAR_ORDER,
    AuditEntry,
    BaseReporter,
    BestPracticeScore,
    ScoredAssessment,
)


class CSVReporter(BaseReporter):
    """Generates WAL_Assessment_Scores.csv with columns:
    Pillar, Principle, Best Practice, Relevant (Y/N), Score (0-2), Finding/Notes."""

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
        cloud = self._get_cloud_provider(scored_assessment)
        cloud_short = self._cloud_short_name(cloud)

        rows: List[Dict[str, str]] = []

        # Group by pillar using PILLAR_ORDER, then emit BPs
        for pillar in PILLAR_ORDER:
            display = self._pillar_display_name(pillar)
            pillar_bps = self._get_bps_for_pillar(best_practice_scores, pillar)
            if not pillar_bps:
                continue

            # Group by principle within each pillar
            principles_seen: List[str] = []
            for bp in pillar_bps:
                p = bp.get("principle", "")
                if p not in principles_seen:
                    principles_seen.append(p)

            for principle in principles_seen:
                # Header row for this principle group
                rows.append({
                    "Pillar": display,
                    "Principle": principle,
                    "Best Practice": "",
                    "Relevant (Y/N)": "",
                    "Score (0-2)": "",
                    "Finding/Notes": "",
                })

                for bp in pillar_bps:
                    if bp.get("principle", "") != principle:
                        continue
                    score_val = bp.get("score", 0)
                    notes = str(bp.get("finding_notes") or "")
                    verified = bp.get("verified", True)
                    rows.append({
                        "Pillar": "",
                        "Principle": "",
                        "Best Practice": bp.get("name", "Unknown"),
                        "Relevant (Y/N)": "Y",
                        "Score (0-2)": str(int(score_val)) if score_val is not None else "",
                        "Finding/Notes": notes,
                        "Verified": "Y" if verified else "N",
                    })

            # Empty separator between pillars
            rows.append({
                "Pillar": "",
                "Principle": "",
                "Best Practice": "",
                "Relevant (Y/N)": "",
                "Score (0-2)": "",
                "Finding/Notes": "",
            })

        # Also emit any BPs with pillars not in PILLAR_ORDER
        remaining = [bp for bp in best_practice_scores
                     if bp.get("pillar", "") not in PILLAR_ORDER]
        for bp in remaining:
            rows.append({
                "Pillar": self._pillar_display_name(bp.get("pillar", "")),
                "Principle": bp.get("principle", ""),
                "Best Practice": bp.get("name", "Unknown"),
                "Relevant (Y/N)": "Y",
                "Score (0-2)": str(int(bp.get("score", 0))),
                "Finding/Notes": str(bp.get("finding_notes") or ""),
            })

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "Pillar",
                    "Principle",
                    "Best Practice",
                    "Relevant (Y/N)",
                    "Score (0-2)",
                    "Finding/Notes",
                    "Verified",
                    "Cloud",
                ],
            )
            writer.writeheader()
            # Write cloud-tagged rows
            for row in rows:
                row["Cloud"] = cloud_short
                writer.writerow(row)

        return output_path
