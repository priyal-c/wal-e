"""
WAL-E Framework: Well-Architected Lakehouse pillars and best practices.

Defines all 7 pillars with 140 best practices from the Databricks
Well-Architected Lakehouse Framework plus Databricks published cheat sheets
and best-practice articles (docs.databricks.com/aws/en/getting-started/best-practices).
"""

from dataclasses import dataclass, field
from typing import Callable, Optional

# Scoring function type: (collected_data: dict) -> tuple[int, str]
# Returns (score 0-2, finding_notes)
ScoringFn = Callable[[dict], tuple[int, str]]


@dataclass
class BestPractice:
    """A single best practice within a pillar."""

    id: str
    name: str
    pillar: str
    principle: str
    scoring_fn: Optional[ScoringFn] = None  # Attached by scoring module
    domains: list[str] = field(default_factory=list)


@dataclass
class Pillar:
    """A WAL pillar with principles and best practices."""

    name: str
    principles: list[str]
    best_practices: list[BestPractice]


def _get_all_pillars() -> list[Pillar]:
    """Return all 7 WAL pillars with their best practices."""
    # Pillar 1: Data & AI Governance
    governance = Pillar(
        name="Data & AI Governance",
        principles=[
            "Establish governance processes",
            "Manage metadata centrally",
            "Track lineage and discovery",
            "Govern AI assets",
            "Define data quality standards",
            "Enforce standardized formats",
        ],
        best_practices=[
            BestPractice(
                id="gov-001",
                name="Establish governance process",
                pillar="Data & AI Governance",
                principle="Establish governance processes",
                domains=["Governance", "Metadata Management"],
            ),
            BestPractice(
                id="gov-002",
                name="Manage metadata in one place",
                pillar="Data & AI Governance",
                principle="Manage metadata centrally",
                domains=["Unity Catalog", "Metadata Management"],
            ),
            BestPractice(
                id="gov-003",
                name="Track lineage",
                pillar="Data & AI Governance",
                principle="Track lineage and discovery",
                domains=["Lineage", "Data Discovery"],
            ),
            BestPractice(
                id="gov-004",
                name="Add descriptions",
                pillar="Data & AI Governance",
                principle="Manage metadata centrally",
                domains=["Metadata Management", "Documentation"],
            ),
            BestPractice(
                id="gov-005",
                name="Allow discovery",
                pillar="Data & AI Governance",
                principle="Track lineage and discovery",
                domains=["Data Discovery", "Catalog"],
            ),
            BestPractice(
                id="gov-006",
                name="Govern AI assets",
                pillar="Data & AI Governance",
                principle="Govern AI assets",
                domains=["MLOps", "Model Registry", "Feature Store"],
            ),
            BestPractice(
                id="gov-007",
                name="Centralize access control",
                pillar="Data & AI Governance",
                principle="Establish governance processes",
                domains=["Unity Catalog", "IAM", "Security"],
            ),
            BestPractice(
                id="gov-008",
                name="Configure audit logging",
                pillar="Data & AI Governance",
                principle="Establish governance processes",
                domains=["Audit", "Compliance", "Security"],
            ),
            BestPractice(
                id="gov-009",
                name="Audit events",
                pillar="Data & AI Governance",
                principle="Establish governance processes",
                domains=["Audit", "Compliance"],
            ),
            BestPractice(
                id="gov-010",
                name="Define DQ standards",
                pillar="Data & AI Governance",
                principle="Define data quality standards",
                domains=["Data Quality", "Governance"],
            ),
            BestPractice(
                id="gov-011",
                name="Use DQ tools",
                pillar="Data & AI Governance",
                principle="Define data quality standards",
                domains=["Data Quality", "Expectations"],
            ),
            BestPractice(
                id="gov-012",
                name="Enforce standardized formats",
                pillar="Data & AI Governance",
                principle="Enforce standardized formats",
                domains=["Data Format", "Delta Lake", "Ingest"],
            ),
            # --- NEW: from UC Best Practices & Admin Cheat Sheet ---
            BestPractice(
                id="gov-013",
                name="Account-level group management",
                pillar="Data & AI Governance",
                principle="Establish governance processes",
                domains=["IAM", "Groups", "SCIM", "Identity"],
            ),
            BestPractice(
                id="gov-014",
                name="Prefer managed tables",
                pillar="Data & AI Governance",
                principle="Manage metadata centrally",
                domains=["Unity Catalog", "Managed Tables", "Storage"],
            ),
            BestPractice(
                id="gov-015",
                name="BROWSE privilege for discovery",
                pillar="Data & AI Governance",
                principle="Track lineage and discovery",
                domains=["Unity Catalog", "Data Discovery", "Privileges"],
            ),
        ],
    )

    # Pillar 2: Interoperability & Usability
    interoperability = Pillar(
        name="Interoperability & Usability",
        principles=[
            "Standard integration patterns",
            "Open formats and standards",
            "Self-service and productivity",
            "Reusable data products",
        ],
        best_practices=[
            BestPractice(
                id="int-001",
                name="Standard integration patterns",
                pillar="Interoperability & Usability",
                principle="Standard integration patterns",
                domains=["Integration", "ETL", "Connectors"],
            ),
            BestPractice(
                id="int-002",
                name="Optimized connectors",
                pillar="Interoperability & Usability",
                principle="Standard integration patterns",
                domains=["Connectors", "Ingest", "Performance"],
            ),
            BestPractice(
                id="int-003",
                name="Certified partner tools",
                pillar="Interoperability & Usability",
                principle="Standard integration patterns",
                domains=["Partner Ecosystem", "Integration"],
            ),
            BestPractice(
                id="int-004",
                name="Reduce pipeline complexity",
                pillar="Interoperability & Usability",
                principle="Standard integration patterns",
                domains=["Workflow Management", "DLT", "ETL"],
            ),
            BestPractice(
                id="int-005",
                name="Use IaC",
                pillar="Interoperability & Usability",
                principle="Standard integration patterns",
                domains=["Infrastructure", "Terraform", "Deployments"],
            ),
            BestPractice(
                id="int-006",
                name="Open data formats",
                pillar="Interoperability & Usability",
                principle="Open formats and standards",
                domains=["Delta Lake", "Parquet", "Data Format"],
            ),
            BestPractice(
                id="int-007",
                name="Secure sharing",
                pillar="Interoperability & Usability",
                principle="Open formats and standards",
                domains=["Delta Sharing", "Security", "Data Sharing"],
            ),
            BestPractice(
                id="int-008",
                name="Open ML standards",
                pillar="Interoperability & Usability",
                principle="Open formats and standards",
                domains=["MLflow", "MLOps", "Model Format"],
            ),
            BestPractice(
                id="int-009",
                name="Self-service",
                pillar="Interoperability & Usability",
                principle="Self-service and productivity",
                domains=["SQL Warehouses", "Notebooks", "Workspace"],
            ),
            BestPractice(
                id="int-010",
                name="Serverless compute",
                pillar="Interoperability & Usability",
                principle="Self-service and productivity",
                domains=["Compute", "SQL Warehouses", "Serverless"],
            ),
            BestPractice(
                id="int-011",
                name="Predefined compute templates",
                pillar="Interoperability & Usability",
                principle="Self-service and productivity",
                domains=["Cluster Policies", "Compute", "Standardization"],
            ),
            BestPractice(
                id="int-012",
                name="AI productivity",
                pillar="Interoperability & Usability",
                principle="Self-service and productivity",
                domains=["AI", "Assistant", "Productivity"],
            ),
            BestPractice(
                id="int-013",
                name="Reusable data products",
                pillar="Interoperability & Usability",
                principle="Reusable data products",
                domains=["Data Products", "Delta Sharing", "Catalog"],
            ),
            BestPractice(
                id="int-014",
                name="Semantic consistency",
                pillar="Interoperability & Usability",
                principle="Reusable data products",
                domains=["Semantic Layer", "BI", "Consistency"],
            ),
            BestPractice(
                id="int-015",
                name="UC for discovery",
                pillar="Interoperability & Usability",
                principle="Track lineage and discovery",
                domains=["Unity Catalog", "Data Discovery", "Catalog"],
            ),
        ],
    )

    # Pillar 3: Operational Excellence
    ops = Pillar(
        name="Operational Excellence",
        principles=[
            "Dedicated operations",
            "Standardize CI/CD and MLOps",
            "Environment isolation",
            "Automated workflows",
            "Monitoring and capacity",
        ],
        best_practices=[
            BestPractice(
                id="ops-001",
                name="Dedicated ops team",
                pillar="Operational Excellence",
                principle="Dedicated operations",
                domains=["Organization", "Operations"],
            ),
            BestPractice(
                id="ops-002",
                name="Enterprise SCM",
                pillar="Operational Excellence",
                principle="Standardize CI/CD and MLOps",
                domains=["Git", "CI/CD", "Source Control"],
            ),
            BestPractice(
                id="ops-003",
                name="Standardize CI/CD",
                pillar="Operational Excellence",
                principle="Standardize CI/CD and MLOps",
                domains=["CI/CD", "Deployments", "Automation"],
            ),
            BestPractice(
                id="ops-004",
                name="MLOps processes",
                pillar="Operational Excellence",
                principle="Standardize CI/CD and MLOps",
                domains=["MLOps", "Model Registry", "ML Lifecycle"],
            ),
            BestPractice(
                id="ops-005",
                name="Environment isolation",
                pillar="Operational Excellence",
                principle="Environment isolation",
                domains=["Workspaces", "Environments", "Isolation"],
            ),
            BestPractice(
                id="ops-006",
                name="Catalog strategy",
                pillar="Operational Excellence",
                principle="Environment isolation",
                domains=["Unity Catalog", "Catalog", "Governance"],
            ),
            BestPractice(
                id="ops-007",
                name="IaC deployments",
                pillar="Operational Excellence",
                principle="Standardize CI/CD and MLOps",
                domains=["Terraform", "IaC", "Deployments"],
            ),
            BestPractice(
                id="ops-008",
                name="Standardize compute",
                pillar="Operational Excellence",
                principle="Standardize CI/CD and MLOps",
                domains=["Cluster Policies", "Compute", "Standardization"],
            ),
            BestPractice(
                id="ops-009",
                name="Automated workflows",
                pillar="Operational Excellence",
                principle="Automated workflows",
                domains=["Jobs", "Workflows", "Orchestration"],
            ),
            BestPractice(
                id="ops-010",
                name="Event-driven ingestion",
                pillar="Operational Excellence",
                principle="Automated workflows",
                domains=["Auto Loader", "Streaming", "Ingest"],
            ),
            BestPractice(
                id="ops-011",
                name="ETL frameworks",
                pillar="Operational Excellence",
                principle="Automated workflows",
                domains=["DLT", "ETL", "Delta Live Tables"],
            ),
            BestPractice(
                id="ops-012",
                name="Deploy-code ML",
                pillar="Operational Excellence",
                principle="Standardize CI/CD and MLOps",
                domains=["MLOps", "Model Serving", "Deployment"],
            ),
            BestPractice(
                id="ops-013",
                name="Model registry",
                pillar="Operational Excellence",
                principle="Standardize CI/CD and MLOps",
                domains=["MLflow", "Model Registry", "MLOps"],
            ),
            BestPractice(
                id="ops-014",
                name="Automate experiment tracking",
                pillar="Operational Excellence",
                principle="Standardize CI/CD and MLOps",
                domains=["MLflow", "Experiments", "MLOps"],
            ),
            BestPractice(
                id="ops-015",
                name="Reuse ML infra",
                pillar="Operational Excellence",
                principle="Standardize CI/CD and MLOps",
                domains=["ML", "Compute", "Infrastructure"],
            ),
            BestPractice(
                id="ops-016",
                name="Declarative management",
                pillar="Operational Excellence",
                principle="Automated workflows",
                domains=["DLT", "Pipelines", "Declarative"],
            ),
            BestPractice(
                id="ops-017",
                name="Service limits",
                pillar="Operational Excellence",
                principle="Monitoring and capacity",
                domains=["Limits", "Capacity", "Governance"],
            ),
            BestPractice(
                id="ops-018",
                name="Capacity planning",
                pillar="Operational Excellence",
                principle="Monitoring and capacity",
                domains=["Capacity", "Planning", "Scalability"],
            ),
            BestPractice(
                id="ops-019",
                name="Monitoring processes",
                pillar="Operational Excellence",
                principle="Monitoring and capacity",
                domains=["Monitoring", "Alerting", "Operations"],
            ),
            BestPractice(
                id="ops-020",
                name="Platform monitoring tools",
                pillar="Operational Excellence",
                principle="Monitoring and capacity",
                domains=["Databricks SQL", "Metrics", "Monitoring"],
            ),
            # --- NEW: from CI/CD Best Practices, Jobs Cheat Sheet ---
            BestPractice(
                id="ops-021",
                name="Automated rollbacks",
                pillar="Operational Excellence",
                principle="Standardize CI/CD and MLOps",
                domains=["CI/CD", "Rollback", "Deployment"],
            ),
            BestPractice(
                id="ops-022",
                name="Workload identity federation",
                pillar="Operational Excellence",
                principle="Standardize CI/CD and MLOps",
                domains=["CI/CD", "Authentication", "Identity Federation"],
            ),
            BestPractice(
                id="ops-023",
                name="Restart long-running clusters",
                pillar="Operational Excellence",
                principle="Monitoring and capacity",
                domains=["Clusters", "Security", "Patching"],
            ),
            # --- DEEP SCAN: system table-backed best practices ---
            BestPractice(
                id="ops-024",
                name="Cluster utilization efficiency (deep)",
                pillar="Operational Excellence",
                principle="Monitoring and capacity",
                domains=["System Tables", "Compute", "Utilization", "Deep"],
            ),
        ],
    )

    # Pillar 4: Security
    security = Pillar(
        name="Security",
        principles=[
            "Least privilege",
            "Data protection",
            "Network security",
            "Compliance",
        ],
        best_practices=[
            BestPractice(
                id="sec-001",
                name="Least privilege IAM",
                pillar="Security",
                principle="Least privilege",
                domains=["IAM", "Unity Catalog", "Access Control"],
            ),
            BestPractice(
                id="sec-002",
                name="Data protection transit/rest",
                pillar="Security",
                principle="Data protection",
                domains=["Encryption", "Security", "Data Protection"],
            ),
            BestPractice(
                id="sec-003",
                name="Network security",
                pillar="Security",
                principle="Network security",
                domains=["VPC", "Firewall", "Network"],
            ),
            BestPractice(
                id="sec-004",
                name="Shared responsibility",
                pillar="Security",
                principle="Compliance",
                domains=["Security", "Cloud", "Responsibility"],
            ),
            BestPractice(
                id="sec-005",
                name="Compliance requirements",
                pillar="Security",
                principle="Compliance",
                domains=["Compliance", "Audit", "Regulatory"],
            ),
            BestPractice(
                id="sec-006",
                name="System security monitoring",
                pillar="Security",
                principle="Compliance",
                domains=["Monitoring", "Security", "Detection"],
            ),
            BestPractice(
                id="sec-007",
                name="Generic controls",
                pillar="Security",
                principle="Compliance",
                domains=["Security", "Policies", "Controls"],
            ),
            # --- NEW: from Admin Cheat Sheet, Jobs Cheat Sheet, UC Best Practices ---
            BestPractice(
                id="sec-008",
                name="SSO configuration",
                pillar="Security",
                principle="Data protection",
                domains=["SSO", "Identity", "Authentication"],
            ),
            BestPractice(
                id="sec-009",
                name="SCIM provisioning",
                pillar="Security",
                principle="Least privilege",
                domains=["SCIM", "Identity", "Provisioning"],
            ),
            BestPractice(
                id="sec-010",
                name="Service principals for automation",
                pillar="Security",
                principle="Least privilege",
                domains=["Service Principals", "Automation", "Identity"],
            ),
            BestPractice(
                id="sec-011",
                name="Customer-managed VPC",
                pillar="Security",
                principle="Network security",
                domains=["VPC", "Private Link", "Network"],
            ),
            BestPractice(
                id="sec-012",
                name="Restrict DBFS root data storage",
                pillar="Security",
                principle="Data protection",
                domains=["DBFS", "Storage", "Data Protection"],
            ),
            # --- DEEP SCAN: system table-backed best practices ---
            BestPractice(
                id="sec-013",
                name="Failed login monitoring (deep)",
                pillar="Security",
                principle="Monitoring and compliance",
                domains=["System Tables", "Audit", "Auth Failures", "Deep"],
            ),
            BestPractice(
                id="sec-014",
                name="Permission change audit (deep)",
                pillar="Security",
                principle="Monitoring and compliance",
                domains=["System Tables", "Audit", "Permissions", "Deep"],
            ),
        ],
    )

    # Pillar 5: Reliability
    reliability = Pillar(
        name="Reliability",
        principles=[
            "ACID and resilient storage",
            "Managed services",
            "Schema management",
            "Backup and recovery",
        ],
        best_practices=[
            BestPractice(
                id="rel-001",
                name="ACID format",
                pillar="Reliability",
                principle="ACID and resilient storage",
                domains=["Delta Lake", "ACID", "Storage"],
            ),
            BestPractice(
                id="rel-002",
                name="Resilient engine",
                pillar="Reliability",
                principle="ACID and resilient storage",
                domains=["Photon", "Spark", "Compute"],
            ),
            BestPractice(
                id="rel-003",
                name="Rescue invalid data",
                pillar="Reliability",
                principle="ACID and resilient storage",
                domains=["Auto Loader", "Rescued Data", "Ingest"],
            ),
            BestPractice(
                id="rel-004",
                name="Auto retries",
                pillar="Reliability",
                principle="Managed services",
                domains=["Jobs", "Retries", "Resilience"],
            ),
            BestPractice(
                id="rel-005",
                name="Scalable serving",
                pillar="Reliability",
                principle="Managed services",
                domains=["Model Serving", "ML", "Scalability"],
            ),
            BestPractice(
                id="rel-006",
                name="Managed services",
                pillar="Reliability",
                principle="Managed services",
                domains=["Jobs", "Pipelines", "Serverless"],
            ),
            BestPractice(
                id="rel-007",
                name="Layered storage",
                pillar="Reliability",
                principle="ACID and resilient storage",
                domains=["Storage", "Delta Lake", "Architecture"],
            ),
            BestPractice(
                id="rel-008",
                name="Reduce redundancy",
                pillar="Reliability",
                principle="ACID and resilient storage",
                domains=["Architecture", "Data Duplication"],
            ),
            BestPractice(
                id="rel-009",
                name="Active schema mgmt",
                pillar="Reliability",
                principle="Schema management",
                domains=["Schema Evolution", "Delta Lake"],
            ),
            BestPractice(
                id="rel-010",
                name="Constraints/expectations",
                pillar="Reliability",
                principle="Schema management",
                domains=["Constraints", "Data Quality", "Expectations"],
            ),
            BestPractice(
                id="rel-011",
                name="Data-centric ML",
                pillar="Reliability",
                principle="Schema management",
                domains=["ML", "Data", "Feature Store"],
            ),
            BestPractice(
                id="rel-012",
                name="ETL autoscaling",
                pillar="Reliability",
                principle="Managed services",
                domains=["DLT", "Autoscaling", "Pipelines"],
            ),
            BestPractice(
                id="rel-013",
                name="SQL warehouse autoscaling",
                pillar="Reliability",
                principle="Managed services",
                domains=["SQL Warehouses", "Autoscaling"],
            ),
            BestPractice(
                id="rel-014",
                name="Regular backups",
                pillar="Reliability",
                principle="Backup and recovery",
                domains=["Backup", "Delta Lake", "DR"],
            ),
            BestPractice(
                id="rel-015",
                name="Streaming recovery",
                pillar="Reliability",
                principle="Backup and recovery",
                domains=["Streaming", "Checkpoints", "Recovery"],
            ),
            BestPractice(
                id="rel-016",
                name="Time travel recovery",
                pillar="Reliability",
                principle="Backup and recovery",
                domains=["Delta Lake", "Time Travel", "Recovery"],
            ),
            BestPractice(
                id="rel-017",
                name="Job automation recovery",
                pillar="Reliability",
                principle="Backup and recovery",
                domains=["Jobs", "Retries", "Recovery"],
            ),
            BestPractice(
                id="rel-018",
                name="DR pattern",
                pillar="Reliability",
                principle="Backup and recovery",
                domains=["DR", "Replication", "Availability"],
            ),
            # --- NEW: from Jobs Cheat Sheet, UC Best Practices ---
            BestPractice(
                id="rel-019",
                name="Service principal job ownership",
                pillar="Reliability",
                principle="Managed services",
                domains=["Service Principals", "Jobs", "Reliability"],
            ),
            # --- DEEP SCAN: system table-backed best practices ---
            BestPractice(
                id="rel-020",
                name="Job success rate (deep)",
                pillar="Reliability",
                principle="Resilient workloads",
                domains=["System Tables", "Jobs", "Success Rate", "Deep"],
            ),
            BestPractice(
                id="rel-021",
                name="Recurring job failures (deep)",
                pillar="Reliability",
                principle="Resilient workloads",
                domains=["System Tables", "Jobs", "Recurring Failures", "Deep"],
            ),
        ],
    )

    # Pillar 6: Performance
    performance = Pillar(
        name="Performance",
        principles=[
            "Scaling and serverless",
            "Data patterns",
            "Query optimization",
            "Monitoring",
        ],
        best_practices=[
            BestPractice(
                id="perf-001",
                name="Scaling",
                pillar="Performance",
                principle="Scaling and serverless",
                domains=["Autoscaling", "Compute", "Clusters"],
            ),
            BestPractice(
                id="perf-002",
                name="Serverless",
                pillar="Performance",
                principle="Scaling and serverless",
                domains=["Serverless", "SQL Warehouses", "Compute"],
            ),
            BestPractice(
                id="perf-003",
                name="Data patterns",
                pillar="Performance",
                principle="Data patterns",
                domains=["Data Layout", "Partitioning", "Z-Ordering"],
            ),
            BestPractice(
                id="perf-004",
                name="Parallel computation",
                pillar="Performance",
                principle="Data patterns",
                domains=["Spark", "Parallelism", "Compute"],
            ),
            BestPractice(
                id="perf-005",
                name="Execution chain",
                pillar="Performance",
                principle="Data patterns",
                domains=["DLT", "Pipelines", "Execution"],
            ),
            BestPractice(
                id="perf-006",
                name="Larger clusters",
                pillar="Performance",
                principle="Scaling and serverless",
                domains=["Clusters", "Compute", "Sizing"],
            ),
            BestPractice(
                id="perf-007",
                name="Native Spark",
                pillar="Performance",
                principle="Data patterns",
                domains=["Spark", "Photon", "Compute"],
            ),
            BestPractice(
                id="perf-008",
                name="Native engines",
                pillar="Performance",
                principle="Data patterns",
                domains=["Photon", "Compute", "Performance"],
            ),
            BestPractice(
                id="perf-009",
                name="Hardware awareness",
                pillar="Performance",
                principle="Data patterns",
                domains=["Compute", "Instance Types", "Hardware"],
            ),
            BestPractice(
                id="perf-010",
                name="Caching",
                pillar="Performance",
                principle="Query optimization",
                domains=["Delta Cache", "Caching", "Performance"],
            ),
            BestPractice(
                id="perf-011",
                name="Compaction",
                pillar="Performance",
                principle="Data patterns",
                domains=["Optimize", "VACUUM", "Delta Lake"],
            ),
            BestPractice(
                id="perf-012",
                name="Data skipping",
                pillar="Performance",
                principle="Query optimization",
                domains=["Data Skipping", "Z-Ordering", "Performance"],
            ),
            BestPractice(
                id="perf-013",
                name="Avoid over-partition",
                pillar="Performance",
                principle="Data patterns",
                domains=["Partitioning", "Delta Lake", "Data Layout"],
            ),
            BestPractice(
                id="perf-014",
                name="Join optimization",
                pillar="Performance",
                principle="Query optimization",
                domains=["Joins", "AQE", "Query"],
            ),
            BestPractice(
                id="perf-015",
                name="Table statistics",
                pillar="Performance",
                principle="Query optimization",
                domains=["Statistics", "ANALYZE", "Query"],
            ),
            BestPractice(
                id="perf-016",
                name="Test on production data",
                pillar="Performance",
                principle="Query optimization",
                domains=["Testing", "Validation", "Performance"],
            ),
            BestPractice(
                id="perf-017",
                name="Prewarming",
                pillar="Performance",
                principle="Query optimization",
                domains=["SQL Warehouses", "Prewarm", "Performance"],
            ),
            BestPractice(
                id="perf-018",
                name="Identify bottlenecks",
                pillar="Performance",
                principle="Monitoring",
                domains=["Profiling", "Diagnostics", "Performance"],
            ),
            BestPractice(
                id="perf-019",
                name="Monitor queries",
                pillar="Performance",
                principle="Monitoring",
                domains=["SQL", "Query History", "Monitoring"],
            ),
            BestPractice(
                id="perf-020",
                name="Monitor streaming",
                pillar="Performance",
                principle="Monitoring",
                domains=["Streaming", "DLT", "Monitoring"],
            ),
            BestPractice(
                id="perf-021",
                name="Monitor jobs",
                pillar="Performance",
                principle="Monitoring",
                domains=["Jobs", "Cluster", "Monitoring"],
            ),
            # --- NEW: from Delta Lake Best Practices, Compute Cheat Sheet ---
            BestPractice(
                id="perf-022",
                name="Predictive optimization",
                pillar="Performance",
                principle="Query optimization",
                domains=["Predictive Optimization", "OPTIMIZE", "VACUUM"],
            ),
            BestPractice(
                id="perf-023",
                name="Liquid clustering",
                pillar="Performance",
                principle="Data patterns",
                domains=["Liquid Clustering", "Delta Lake", "Data Layout"],
            ),
            BestPractice(
                id="perf-024",
                name="Graviton instance types",
                pillar="Performance",
                principle="Scaling and serverless",
                domains=["Graviton", "Instance Types", "AWS"],
            ),
            BestPractice(
                id="perf-025",
                name="Standard access mode",
                pillar="Performance",
                principle="Scaling and serverless",
                domains=["Access Mode", "Compute", "UC"],
            ),
            # --- DEEP SCAN: system table-backed best practices ---
            BestPractice(
                id="perf-026",
                name="Query failure rate (deep)",
                pillar="Performance",
                principle="Monitoring and tuning",
                domains=["System Tables", "Query", "Failure Rate", "Deep"],
            ),
            BestPractice(
                id="perf-027",
                name="Slow query prevalence (deep)",
                pillar="Performance",
                principle="Monitoring and tuning",
                domains=["System Tables", "Query", "Slow Queries", "Deep"],
            ),
            BestPractice(
                id="perf-028",
                name="Warehouse utilization balance (deep)",
                pillar="Performance",
                principle="Scaling and serverless",
                domains=["System Tables", "Warehouse", "Utilization", "Deep"],
            ),
        ],
    )

    # Pillar 7: Cost
    cost = Pillar(
        name="Cost",
        principles=[
            "Optimized formats and compute",
            "Right-sizing",
            "Auto-scaling and termination",
            "Cost monitoring",
        ],
        best_practices=[
            BestPractice(
                id="cost-001",
                name="Optimized formats",
                pillar="Cost",
                principle="Optimized formats and compute",
                domains=["Delta Lake", "Compression", "Storage"],
            ),
            BestPractice(
                id="cost-002",
                name="Job clusters",
                pillar="Cost",
                principle="Optimized formats and compute",
                domains=["Jobs", "Clusters", "Compute"],
            ),
            BestPractice(
                id="cost-003",
                name="SQL for SQL",
                pillar="Cost",
                principle="Right-sizing",
                domains=["SQL Warehouses", "Compute", "Workload"],
            ),
            BestPractice(
                id="cost-004",
                name="Up-to-date runtimes",
                pillar="Cost",
                principle="Optimized formats and compute",
                domains=["DBR", "Runtimes", "Maintenance"],
            ),
            BestPractice(
                id="cost-005",
                name="GPU right workloads",
                pillar="Cost",
                principle="Right-sizing",
                domains=["GPU", "ML", "Compute"],
            ),
            BestPractice(
                id="cost-006",
                name="Serverless",
                pillar="Cost",
                principle="Optimized formats and compute",
                domains=["Serverless", "SQL Warehouses", "Compute"],
            ),
            BestPractice(
                id="cost-007",
                name="Right instance type",
                pillar="Cost",
                principle="Right-sizing",
                domains=["Instance Types", "Compute", "Clusters"],
            ),
            BestPractice(
                id="cost-008",
                name="Efficient compute size",
                pillar="Cost",
                principle="Right-sizing",
                domains=["Cluster Size", "Workers", "Compute"],
            ),
            BestPractice(
                id="cost-009",
                name="Performance engines",
                pillar="Cost",
                principle="Optimized formats and compute",
                domains=["Photon", "Performance", "Compute"],
            ),
            BestPractice(
                id="cost-010",
                name="Auto-scaling",
                pillar="Cost",
                principle="Auto-scaling and termination",
                domains=["Autoscaling", "Clusters", "SQL Warehouses"],
            ),
            BestPractice(
                id="cost-011",
                name="Auto-termination",
                pillar="Cost",
                principle="Auto-scaling and termination",
                domains=["Auto-stop", "SQL Warehouses", "Clusters"],
            ),
            BestPractice(
                id="cost-012",
                name="Cluster policies costs",
                pillar="Cost",
                principle="Right-sizing",
                domains=["Cluster Policies", "Governance", "Cost"],
            ),
            BestPractice(
                id="cost-013",
                name="Monitor costs",
                pillar="Cost",
                principle="Cost monitoring",
                domains=["Cost", "Tagging", "Monitoring"],
            ),
            BestPractice(
                id="cost-014",
                name="Tag clusters",
                pillar="Cost",
                principle="Cost monitoring",
                domains=["Tags", "Clusters", "Chargeback"],
            ),
            BestPractice(
                id="cost-015",
                name="Chargeback",
                pillar="Cost",
                principle="Cost monitoring",
                domains=["Chargeback", "Tags", "Cost Allocation"],
            ),
            BestPractice(
                id="cost-016",
                name="Cost reports",
                pillar="Cost",
                principle="Cost monitoring",
                domains=["Reporting", "Cost", "Analytics"],
            ),
            BestPractice(
                id="cost-017",
                name="Streaming balance",
                pillar="Cost",
                principle="Right-sizing",
                domains=["Streaming", "Cost", "Throughput"],
            ),
            BestPractice(
                id="cost-018",
                name="On-demand vs reserved",
                pillar="Cost",
                principle="Optimized formats and compute",
                domains=["Reserved", "Spot", "Instance Savings"],
            ),
            # --- NEW: from Compute Cheat Sheet, Admin Cheat Sheet ---
            BestPractice(
                id="cost-019",
                name="Spot instance strategy",
                pillar="Cost",
                principle="Right-sizing",
                domains=["Spot", "AWS", "Compute", "Cost"],
            ),
            BestPractice(
                id="cost-020",
                name="Budget alerts",
                pillar="Cost",
                principle="Cost monitoring",
                domains=["Budget", "Alerts", "Cost Monitoring"],
            ),
            # --- DEEP SCAN: system table-backed best practices ---
            BestPractice(
                id="cost-021",
                name="Idle cluster waste (deep)",
                pillar="Cost",
                principle="Eliminate waste",
                domains=["System Tables", "Compute", "Idle", "Deep"],
            ),
            BestPractice(
                id="cost-022",
                name="Cost trend analysis (deep)",
                pillar="Cost",
                principle="Cost monitoring",
                domains=["System Tables", "Billing", "Trend", "Deep"],
            ),
            BestPractice(
                id="cost-023",
                name="DBU concentration risk (deep)",
                pillar="Cost",
                principle="Cost monitoring",
                domains=["System Tables", "Billing", "Concentration", "Deep"],
            ),
        ],
    )

    return [governance, interoperability, ops, security, reliability, performance, cost]


# Export all pillars
ALL_PILLARS = _get_all_pillars()

# Flatten all best practices for lookup
ALL_BEST_PRACTICES: dict[str, BestPractice] = {
    bp.id: bp for pillar in ALL_PILLARS for bp in pillar.best_practices
}
