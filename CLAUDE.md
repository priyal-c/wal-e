# CLAUDE.md - WAL-E Project Rules

## Overview

WAL-E (Well-Architected Lakehouse Evaluator) is an automated assessment tool for Databricks workspaces. It evaluates workspaces against the [Well-Architected Lakehouse Framework](https://docs.databricks.com/lakehouse-architecture/well-architected) by querying Databricks APIs, scoring **129 best practices** across **7 pillars**, and generating assessment reports.

WAL-E auto-detects the cloud provider (AWS / Azure / GCP) from the workspace URL and fine-tunes all scoring and recommendations to be cloud-specific.

## How to Run

```bash
# Full assessment (collect data, score, and generate reports)
wal-e assess

# With specific profile and output
wal-e assess --profile customer-workspace --output ./assessment-results --format all

# Validate workspace access before running assessment
wal-e validate --profile customer-workspace

# Interactive mode (recommended for customer sessions)
wal-e assess --interactive --profile customer-workspace

# Re-generate reports from cached data
wal-e report --input ./assessment-results --format pptx html csv

# Show access setup guide for customer sessions
wal-e setup --guide
```

## Architecture

| Component | Path | Description |
|-----------|------|-------------|
| **Collectors** | `src/wal_e/collectors/` | Data collection from Databricks APIs (23+ endpoints) |
| **Framework** | `src/wal_e/framework/` | WAL scoring engine (129 best practices, 7 pillars) |
| **Reporters** | `src/wal_e/reporters/` | Report generators (MD, CSV, HTML, PPTX, Audit) |
| **Core** | `src/wal_e/core/` | Orchestration engine, config, cloud detection |
| **MCP** | `mcp/` | MCP server for AI Dev Kit integration |

## Key Rules

1. **READ-ONLY ACCESS** - All workspace access is read-only. Never modify the customer workspace. WAL-E only queries APIs and system tables.

2. **Validate First** - Always validate access with `wal-e validate` before running a full assessment.

3. **Scoring Scale** - Best practices use a 0-2 scale:
   - **0** = Not Implemented
   - **1** = Partial
   - **2** = Full

4. **Cloud-Aware** - WAL-E auto-detects the cloud from the workspace URL and tailors scoring (e.g., Graviton on AWS, Dv5 on Azure, T2D on GCP).

5. **Audit Trail** - All reports must include or reference an audit trail of API calls made.

6. **Configuration** - WAL-E uses `~/.databrickscfg` for host and token. Set `--profile` for different workspaces.

## 7 Pillars (129 Best Practices)

1. **Data & AI Governance** (15) - Unity Catalog, metadata, lineage, data quality, group management
2. **Interoperability & Usability** (14) - Open formats, IaC, serverless, self-service
3. **Operational Excellence** (23) - CI/CD, MLOps, monitoring, environment isolation, rollbacks
4. **Security** (12) - IAM, SSO/SCIM, encryption, network, VPC/VNET, compliance
5. **Reliability** (19) - ACID, auto-scaling, DR, backups, service principal ownership
6. **Performance** (25) - Serverless, data layout, liquid clustering, predictive optimization
7. **Cost** (20) - Spot/preemptible, reserved instances, tagging, budget alerts

## Adding New Code

- **New collectors**: Extend `BaseCollector` in `src/wal_e/collectors/base.py`, implement `collect()`, register in `AssessmentEngine`
- **New best practices**: Add to `pillars.py`, add scoring function to `scoring.py`, register in `SCORING_REGISTRY`
- **New reporters**: Extend `BaseReporter` in `src/wal_e/reporters/base.py`, implement `generate()`

## Output Deliverables

| File | Description |
|------|-------------|
| `WAL_Assessment_Readout.md` | Full detailed report (all 7 pillars) |
| `WAL_Assessment_Scores.csv` | 129 best practices with scores and notes |
| `WAL_Assessment_Presentation.pptx` | Executive readout deck (17 slides) |
| `WAL_Assessment_Presentation.html` | Browser-based presentation |
| `WAL_Assessment_Audit_Report.md` | Complete API call evidence trail |
