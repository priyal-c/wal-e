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


class AuditEntry(TypedDict):
    """A single audit log entry from data collection."""

    command: str
    output: str
    timestamp: str
    duration: Optional[float]


# Canonical list of all 99 best practices (Pillar, Principle, Best Practice)
# Used by CSV reporter to ensure all rows are present
BEST_PRACTICES: List[Dict[str, str]] = [
    # Pillar 1: Data & AI Governance (12)
    {"pillar": "Data & AI Governance", "principle": "Unified metadata", "best_practice": "Use Unity Catalog for metastore"},
    {"pillar": "Data & AI Governance", "principle": "Centralized governance", "best_practice": "Single metastore per cloud region"},
    {"pillar": "Data & AI Governance", "principle": "Data organization", "best_practice": "Catalog → Schema → Table hierarchy"},
    {"pillar": "Data & AI Governance", "principle": "Data lineage", "best_practice": "Enable lineage tracking for tables"},
    {"pillar": "Data & AI Governance", "principle": "Data quality", "best_practice": "Define data quality rules"},
    {"pillar": "Data & AI Governance", "principle": "Access control", "best_practice": "Use UC grants for table access"},
    {"pillar": "Data & AI Governance", "principle": "Data discovery", "best_practice": "Document tables with descriptions"},
    {"pillar": "Data & AI Governance", "principle": "AI governance", "best_practice": "Register models in Model Registry"},
    {"pillar": "Data & AI Governance", "principle": "Data classification", "best_practice": "Tag sensitive data"},
    {"pillar": "Data & AI Governance", "principle": "Compliance", "best_practice": "Audit logs enabled"},
    {"pillar": "Data & AI Governance", "principle": "Data sharing", "best_practice": "Delta Sharing for external access"},
    {"pillar": "Data & AI Governance", "principle": "Data retention", "best_practice": "Define retention policies"},
    # Pillar 2: Interoperability & Usability (14)
    {"pillar": "Interoperability & Usability", "principle": "Open formats", "best_practice": "Use Delta Lake format"},
    {"pillar": "Interoperability & Usability", "principle": "Open formats", "best_practice": "Parquet for analytics"},
    {"pillar": "Interoperability & Usability", "principle": "API compatibility", "best_practice": "Use SQL for data access"},
    {"pillar": "Interoperability & Usability", "principle": "Multi-cloud", "best_practice": "Consistent config across clouds"},
    {"pillar": "Interoperability & Usability", "principle": "Infrastructure as Code", "best_practice": "Terraform/IaC for workspace"},
    {"pillar": "Interoperability & Usability", "principle": "Serverless", "best_practice": "Use serverless compute where applicable"},
    {"pillar": "Interoperability & Usability", "principle": "Self-service", "best_practice": "SQL Warehouses for analysts"},
    {"pillar": "Interoperability & Usability", "principle": "Self-service", "best_practice": "Notebook templates available"},
    {"pillar": "Interoperability & Usability", "principle": "Connectivity", "best_practice": "Partner connect integrations"},
    {"pillar": "Interoperability & Usability", "principle": "Connectivity", "best_practice": "JDBC/ODBC for BI tools"},
    {"pillar": "Interoperability & Usability", "principle": "User experience", "best_practice": "Consistent workspace layout"},
    {"pillar": "Interoperability & Usability", "principle": "User experience", "best_practice": "Documentation and runbooks"},
    {"pillar": "Interoperability & Usability", "principle": "Collaboration", "best_practice": "Shared repos and notebooks"},
    {"pillar": "Interoperability & Usability", "principle": "Collaboration", "best_practice": "Unity Catalog for cross-team sharing"},
    # Pillar 3: Operational Excellence (18)
    {"pillar": "Operational Excellence", "principle": "CI/CD", "best_practice": "Automated notebook deployment"},
    {"pillar": "Operational Excellence", "principle": "CI/CD", "best_practice": "Repos for version control"},
    {"pillar": "Operational Excellence", "principle": "CI/CD", "best_practice": "Job deployment automation"},
    {"pillar": "Operational Excellence", "principle": "MLOps", "best_practice": "Model Registry for lifecycle"},
    {"pillar": "Operational Excellence", "principle": "MLOps", "best_practice": "Feature store usage"},
    {"pillar": "Operational Excellence", "principle": "MLOps", "best_practice": "MLflow tracking enabled"},
    {"pillar": "Operational Excellence", "principle": "Monitoring", "best_practice": "Cluster monitoring alerts"},
    {"pillar": "Operational Excellence", "principle": "Monitoring", "best_practice": "Job run monitoring"},
    {"pillar": "Operational Excellence", "principle": "Monitoring", "best_practice": "Pipeline health checks"},
    {"pillar": "Operational Excellence", "principle": "Monitoring", "best_practice": "Dashboard for key metrics"},
    {"pillar": "Operational Excellence", "principle": "Capacity", "best_practice": "Environment isolation (dev/staging/prod)"},
    {"pillar": "Operational Excellence", "principle": "Capacity", "best_practice": "Cluster policies for consistency"},
    {"pillar": "Operational Excellence", "principle": "Capacity", "best_practice": "Instance pool usage"},
    {"pillar": "Operational Excellence", "principle": "Automation", "best_practice": "Scheduled jobs for workloads"},
    {"pillar": "Operational Excellence", "principle": "Automation", "best_practice": "DLT for streaming pipelines"},
    {"pillar": "Operational Excellence", "principle": "Automation", "best_practice": "Workflow orchestration"},
    {"pillar": "Operational Excellence", "principle": "Incident response", "best_practice": "Runbook documentation"},
    {"pillar": "Operational Excellence", "principle": "Incident response", "best_practice": "On-call and escalation paths"},
    # Pillar 4: Security, Compliance & Privacy (7)
    {"pillar": "Security, Compliance & Privacy", "principle": "Identity", "best_practice": "SCIM for user provisioning"},
    {"pillar": "Security, Compliance & Privacy", "principle": "Identity", "best_practice": "Service principals for automation"},
    {"pillar": "Security, Compliance & Privacy", "principle": "Encryption", "best_practice": "Encryption at rest"},
    {"pillar": "Security, Compliance & Privacy", "principle": "Network", "best_practice": "IP access lists configured"},
    {"pillar": "Security, Compliance & Privacy", "principle": "Network", "best_practice": "Private link / VPC"},
    {"pillar": "Security, Compliance & Privacy", "principle": "Secrets", "best_practice": "Secrets in secret scopes"},
    {"pillar": "Security, Compliance & Privacy", "principle": "Compliance", "best_practice": "Audit logging to external SIEM"},
    # Pillar 5: Reliability (14)
    {"pillar": "Reliability", "principle": "ACID", "best_practice": "Delta Lake for transactions"},
    {"pillar": "Reliability", "principle": "ACID", "best_practice": "Time travel enabled"},
    {"pillar": "Reliability", "principle": "Resilience", "best_practice": "Auto-scaling clusters"},
    {"pillar": "Reliability", "principle": "Resilience", "best_practice": "Job retry policies"},
    {"pillar": "Reliability", "principle": "Resilience", "best_practice": "Multi-AZ for critical workloads"},
    {"pillar": "Reliability", "principle": "Disaster recovery", "best_practice": "Backup strategy for tables"},
    {"pillar": "Reliability", "principle": "Disaster recovery", "best_practice": "Cross-region replication for DR"},
    {"pillar": "Reliability", "principle": "Disaster recovery", "best_practice": "Recovery runbooks"},
    {"pillar": "Reliability", "principle": "Data integrity", "best_practice": "Schema evolution handling"},
    {"pillar": "Reliability", "principle": "Data integrity", "best_practice": "Constraints for validation"},
    {"pillar": "Reliability", "principle": "Availability", "best_practice": "SQL Warehouse high availability"},
    {"pillar": "Reliability", "principle": "Availability", "best_practice": "Job concurrency limits"},
    {"pillar": "Reliability", "principle": "Change management", "best_practice": "Controlled schema changes"},
    {"pillar": "Reliability", "principle": "Change management", "best_practice": "Pipeline versioning"},
    # Pillar 6: Performance Efficiency (19)
    {"pillar": "Performance Efficiency", "principle": "Compute", "best_practice": "Serverless SQL Warehouses"},
    {"pillar": "Performance Efficiency", "principle": "Compute", "best_practice": "Photon acceleration enabled"},
    {"pillar": "Performance Efficiency", "principle": "Compute", "best_practice": "Right-sized cluster instances"},
    {"pillar": "Performance Efficiency", "principle": "Compute", "best_practice": "Autoscaling for variable load"},
    {"pillar": "Performance Efficiency", "principle": "Data layout", "best_practice": "Z-ordering on filter columns"},
    {"pillar": "Performance Efficiency", "principle": "Data layout", "best_practice": "Liquid clustering for tables"},
    {"pillar": "Performance Efficiency", "principle": "Data layout", "best_practice": "Partition pruning"},
    {"pillar": "Performance Efficiency", "principle": "Data layout", "best_practice": "Table optimization schedules"},
    {"pillar": "Performance Efficiency", "principle": "Caching", "best_practice": "Delta cache for repeated reads"},
    {"pillar": "Performance Efficiency", "principle": "Caching", "best_practice": "Result caching for dashboards"},
    {"pillar": "Performance Efficiency", "principle": "Query optimization", "best_practice": "Query history analysis"},
    {"pillar": "Performance Efficiency", "principle": "Query optimization", "best_practice": "Query recommendations enabled"},
    {"pillar": "Performance Efficiency", "principle": "Streaming", "best_practice": "Structured Streaming for real-time"},
    {"pillar": "Performance Efficiency", "principle": "Streaming", "best_practice": "Checkpointing for durability"},
    {"pillar": "Performance Efficiency", "principle": "Monitoring", "best_practice": "Performance dashboards"},
    {"pillar": "Performance Efficiency", "principle": "Monitoring", "best_practice": "Resource utilization tracking"},
    {"pillar": "Performance Efficiency", "principle": "Monitoring", "best_practice": "Query performance alerts"},
    {"pillar": "Performance Efficiency", "principle": "Monitoring", "best_practice": "System table usage for tuning"},
    # Pillar 7: Cost Optimization (15)
    {"pillar": "Cost Optimization", "principle": "Resource selection", "best_practice": "Spot instances for workloads"},
    {"pillar": "Cost Optimization", "principle": "Resource selection", "best_practice": "Cluster policies for cost"},
    {"pillar": "Cost Optimization", "principle": "Resource selection", "best_practice": "Termination policies"},
    {"pillar": "Cost Optimization", "principle": "Autoscaling", "best_practice": "Scale-to-zero for idle"},
    {"pillar": "Cost Optimization", "principle": "Autoscaling", "best_practice": "Warehouse auto-stop"},
    {"pillar": "Cost Optimization", "principle": "Autoscaling", "best_practice": "Cluster auto-termination"},
    {"pillar": "Cost Optimization", "principle": "Tagging", "best_practice": "Cost allocation tags"},
    {"pillar": "Cost Optimization", "principle": "Tagging", "best_practice": "Department/project tags"},
    {"pillar": "Cost Optimization", "principle": "Storage", "best_practice": "Lifecycle rules for old data"},
    {"pillar": "Cost Optimization", "principle": "Storage", "best_practice": "Vacuum policies"},
    {"pillar": "Cost Optimization", "principle": "Storage", "best_practice": "Compact small files"},
    {"pillar": "Cost Optimization", "principle": "Monitoring", "best_practice": "Cost dashboards"},
    {"pillar": "Cost Optimization", "principle": "Monitoring", "best_practice": "Budget alerts"},
    {"pillar": "Cost Optimization", "principle": "Monitoring", "best_practice": "Usage reporting"},
    {"pillar": "Cost Optimization", "principle": "Right-sizing", "best_practice": "Review cluster utilization"},
    {"pillar": "Performance Efficiency", "principle": "Data layout", "best_practice": "Appropriate file sizes"},
]

# Pillar display order
PILLAR_ORDER: List[str] = [
    "Data & AI Governance",
    "Interoperability & Usability",
    "Operational Excellence",
    "Security, Compliance & Privacy",
    "Reliability",
    "Performance Efficiency",
    "Cost Optimization",
]


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
        """
        Generate the report and write to output_dir.

        Args:
            scored_assessment: Scored assessment with pillar_scores, best_practice_scores, etc.
            collected_data: Raw data from collectors
            audit_entries: List of API/CLI commands executed with output
            output_dir: Directory to write the report file

        Returns:
            Path to the generated file

        Raises:
            NotImplementedError: Subclasses must implement
        """
        raise NotImplementedError("Subclasses must implement generate()")

    # -----------------------------------------------------------------------
    # Helper methods for formatting
    # -----------------------------------------------------------------------

    def _get_pillar_scores(self, scored_assessment: ScoredAssessment) -> Dict[str, float]:
        """Get pillar scores, defaulting to empty dict."""
        return scored_assessment.get("pillar_scores") or {}

    def _get_best_practice_scores(
        self, scored_assessment: ScoredAssessment
    ) -> List[BestPracticeScore]:
        """Get best practice scores, defaulting to empty list."""
        return scored_assessment.get("best_practice_scores") or []

    def _get_overall_score(self, scored_assessment: ScoredAssessment) -> float:
        """Get overall score (0-100), default 0."""
        return float(scored_assessment.get("overall_score") or 0)

    def _get_maturity_level(self, scored_assessment: ScoredAssessment) -> str:
        """Get maturity level string, default 'Not Assessed'."""
        return scored_assessment.get("maturity_level") or "Not Assessed"

    def _get_workspace_host(self, scored_assessment: ScoredAssessment) -> str:
        """Get workspace host, default 'Unknown'."""
        return scored_assessment.get("workspace_host") or "Unknown"

    def _get_assessment_date(self, scored_assessment: ScoredAssessment) -> str:
        """Get assessment date, default 'Unknown'."""
        return scored_assessment.get("assessment_date") or "Unknown"

    def _format_score(self, score: Optional[Union[int, float]]) -> str:
        """Format score as percentage (0-100) or N/A."""
        if score is None:
            return "N/A"
        try:
            return f"{float(score):.1f}%"
        except (TypeError, ValueError):
            return "N/A"

    def _format_score_bar(self, score: float, width: int = 20) -> str:
        """Format score as a simple text bar (0-100)."""
        try:
            pct = min(100, max(0, float(score)))
        except (TypeError, ValueError):
            return "?" * width
        filled = int(width * pct / 100)
        return "█" * filled + "░" * (width - filled)

    def _maturity_color(self, maturity: str) -> str:
        """Return CSS color name for maturity level."""
        m = (maturity or "").lower()
        if "optimized" in m or "advanced" in m:
            return "green"
        if "defined" in m or "intermediate" in m:
            return "orange"
        if "initial" in m or "basic" in m:
            return "orange"
        return "red"  # Not assessed, etc.

    def _score_badge_color(self, score: float) -> str:
        """Return color for score badge (0-100)."""
        if score >= 70:
            return "green"
        if score >= 40:
            return "orange"
        return "red"

    def _lookup_best_practice_score(
        self,
        best_practice_scores: List[BestPracticeScore],
        pillar: str,
        best_practice: str,
    ) -> Optional[BestPracticeScore]:
        """Find matching best practice score by pillar and name."""
        for bp in best_practice_scores:
            if (bp.get("pillar") or "").strip() == (pillar or "").strip():
                if (bp.get("name") or "").strip() == (best_practice or "").strip():
                    return bp
        return None

    def _ensure_output_dir(self, output_dir: Union[str, Path]) -> Path:
        """Ensure output directory exists and return Path."""
        path = Path(output_dir)
        path.mkdir(parents=True, exist_ok=True)
        return path
