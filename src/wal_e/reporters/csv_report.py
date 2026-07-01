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

    @staticmethod
    def _unverifiable_reason(notes: str) -> str:
        """Return a plain-language explanation of why an item could not be verified.

        Categorized from the finding text so customers reading the spreadsheet
        understand an 'unverifiable' item is a scope/visibility limitation, not a
        confirmed gap.
        """
        n = (notes or "").lower()
        if "--deep" in n or "system table" in n or "system.access" in n:
            return (
                "Why unverifiable: this requires the optional --deep scan "
                "(Databricks system tables), which was not run. It is a scope "
                "limitation of the standard scan, not a detected gap."
            )
        if "account level" in n or "account-level" in n or "account console" in n:
            return (
                "Why unverifiable: this is configured at the Databricks account "
                "level, which the workspace REST API does not expose. Confirm it "
                "in the account console before treating it as a gap."
            )
        return (
            "Why unverifiable: the workspace REST API does not expose enough "
            "detail to confirm this. Treat as needs manual review, not a "
            "confirmed gap."
        )

    def _notes_with_reason(self, notes: str, verified: bool) -> str:
        if verified:
            return notes
        reason = self._unverifiable_reason(notes)
        if not notes:
            return reason
        return f"{notes} [{reason}]"

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
                        "Finding/Notes": self._notes_with_reason(notes, verified),
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
            verified = bp.get("verified", True)
            rows.append({
                "Pillar": self._pillar_display_name(bp.get("pillar", "")),
                "Principle": bp.get("principle", ""),
                "Best Practice": bp.get("name", "Unknown"),
                "Relevant (Y/N)": "Y",
                "Score (0-2)": str(int(bp.get("score", 0))),
                "Finding/Notes": self._notes_with_reason(str(bp.get("finding_notes") or ""), verified),
                "Verified": "Y" if verified else "N",
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
