"""
Test script for WAL-E reporters - verifies all reporters run with sample data.
"""

import tempfile
from pathlib import Path

# Sample data matching ScoredAssessment, collected_data, audit_entries
SAMPLE_SCORED_ASSESSMENT = {
    "pillar_scores": {
        "Data & AI Governance": 65.0,
        "Interoperability & Usability": 72.0,
        "Operational Excellence": 48.0,
        "Security, Compliance & Privacy": 55.0,
        "Reliability": 60.0,
        "Performance Efficiency": 58.0,
        "Cost Optimization": 52.0,
    },
    "best_practice_scores": [
        {
            "name": "Use Unity Catalog for metastore",
            "pillar": "Data & AI Governance",
            "principle": "Unified metadata",
            "score": 1.5,
            "finding_notes": "Unity Catalog is enabled.",
        },
        {
            "name": "Delta Lake for transactions",
            "pillar": "Reliability",
            "principle": "ACID",
            "score": 2.0,
            "finding_notes": "Delta tables in use.",
        },
        {
            "name": "Use serverless compute where applicable",
            "pillar": "Interoperability & Usability",
            "principle": "Serverless",
            "score": 0.5,
            "finding_notes": "Limited serverless adoption.",
        },
    ],
    "overall_score": 58.5,
    "maturity_level": "Defined",
    "workspace_host": "https://customer.cloud.databricks.com",
    "assessment_date": "2025-02-11",
}

SAMPLE_COLLECTED_DATA = {
    "clusters": 12,
    "warehouses": 4,
    "jobs": 28,
    "catalogs": 3,
}

SAMPLE_AUDIT_ENTRIES = [
    {
        "command": "databricks clusters list",
        "output": '{"clusters": [{"cluster_id": "abc123", "cluster_name": "prod-job"}]}',
        "timestamp": "2025-02-11T10:00:00Z",
        "duration": 1.2,
    },
    {
        "command": "databricks unity-catalog catalogs list",
        "output": '{"catalogs": [{"name": "main"}, {"name": "analytics"}]}',
        "timestamp": "2025-02-11T10:00:05Z",
        "duration": 0.8,
    },
]


def test_reporters():
    """Run all reporters with sample data and verify files are created."""
    from wal_e.reporters import (
        MarkdownReporter,
        CSVReporter,
        HTMLDeckReporter,
        PPTXDeckReporter,
        AuditLogReporter,
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)

        # Markdown
        md = MarkdownReporter()
        path_md = md.generate(
            SAMPLE_SCORED_ASSESSMENT,
            SAMPLE_COLLECTED_DATA,
            SAMPLE_AUDIT_ENTRIES,
            output_dir,
        )
        assert path_md.exists()
        assert "Executive Summary" in path_md.read_text()

        # CSV
        csv_r = CSVReporter()
        path_csv = csv_r.generate(
            SAMPLE_SCORED_ASSESSMENT,
            SAMPLE_COLLECTED_DATA,
            SAMPLE_AUDIT_ENTRIES,
            output_dir,
        )
        assert path_csv.exists()
        lines = path_csv.read_text().strip().split("\n")
        assert len(lines) >= 99  # header + 99 best practices

        # HTML
        html = HTMLDeckReporter()
        path_html = html.generate(
            SAMPLE_SCORED_ASSESSMENT,
            SAMPLE_COLLECTED_DATA,
            SAMPLE_AUDIT_ENTRIES,
            output_dir,
        )
        assert path_html.exists()
        assert "Well-Architected Lakehouse" in path_html.read_text()

        # Audit log
        audit = AuditLogReporter()
        path_audit = audit.generate(
            SAMPLE_SCORED_ASSESSMENT,
            SAMPLE_COLLECTED_DATA,
            SAMPLE_AUDIT_ENTRIES,
            output_dir,
        )
        assert path_audit.exists()
        assert "databricks clusters list" in path_audit.read_text()

        # PPTX (skip if python-pptx not installed)
        try:
            pptx = PPTXDeckReporter()
            path_pptx = pptx.generate(
                SAMPLE_SCORED_ASSESSMENT,
                SAMPLE_COLLECTED_DATA,
                SAMPLE_AUDIT_ENTRIES,
                output_dir,
            )
            assert path_pptx.exists()
        except ImportError:
            pass  # Optional dependency

        print("All reporters passed.")


if __name__ == "__main__":
    test_reporters()
