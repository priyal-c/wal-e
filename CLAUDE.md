# CLAUDE.md - WAL-E Project Rules

## Overview

WAL-E (Well-Architected Lakehouse Evaluator) is an automated assessment tool for Databricks workspaces. It evaluates workspaces against the [Well-Architected Lakehouse Framework](https://docs.databricks.com/lakehouse-architecture/well-architected) by querying Databricks APIs, scoring **145 best practices** (134 standard + 11 deep scan) across **7 pillars**, and generating assessment reports.

**Operating Model:** WAL-E is designed to be **run by the customer on their own machine**, with a Databricks SA guiding them through every step. No tokens, credentials, or data ever leave the customer's environment. The SA joins via screen share and guides the process.

WAL-E auto-detects the cloud provider (AWS / Azure / GCP) from the workspace URL and fine-tunes all scoring and recommendations to be cloud-specific.

## How to Run

```bash
# Standard assessment (30 API call types, 134 best practices)
wal-e assess --profile wal-assessment --output ./my-assessment --format all

# Deep scan (adds system tables: billing, compute, query, audit, lakeflow jobs)
# Also quantifies auto-termination $ savings for interactive clusters (10/30/60-min policies)
wal-e assess --profile wal-assessment --deep --warehouse-id <ID> --format all

# Validate workspace access before running
wal-e validate --profile wal-assessment

# Interactive mode (SA guides customer through each step)
wal-e assess --interactive --profile wal-assessment

# Re-generate reports from cached data
wal-e report --input ./my-assessment --format pptx html csv

# Show customer-facing setup guide
wal-e setup --guide
```

## Architecture

| Component | Path | Description |
|-----------|------|-------------|
| **Collectors** | `src/wal_e/collectors/` | Data collection from Databricks APIs + system tables (incl. `ai.py` for Mosaic AI / GenAI assets) |
| **Framework** | `src/wal_e/framework/` | WAL scoring engine (145 best practices, 7 pillars) |
| **Reporters** | `src/wal_e/reporters/` | Report generators (MD, CSV, HTML, PPTX, Audit) |
| **Core** | `src/wal_e/core/` | Orchestration engine, config, cloud detection |
| **MCP** | `mcp/` | MCP server for AI Dev Kit integration |

## Key Rules

1. **CUSTOMER-RUN** - WAL-E runs on the customer's machine. The SA guides via screen share. No tokens leave the customer's environment.

2. **READ-ONLY ACCESS** - All workspace access is read-only. Never modify the workspace. WAL-E only queries APIs and system tables.

3. **Validate First** - Always validate access with `wal-e validate` before running a full assessment.

4. **Scoring Scale** - Best practices use a 0-2 scale:
   - **0** = Not Implemented
   - **1** = Partial
   - **2** = Full

5. **Cloud-Aware** - WAL-E auto-detects the cloud from the workspace URL and tailors scoring (e.g., Graviton on AWS, Dv5 on Azure, T2D on GCP).

6. **Audit Trail** - All reports must include or reference an audit trail of API calls made.

7. **Configuration** - WAL-E uses `~/.databrickscfg` for host and token. Set `--profile` for different workspaces.

8. **Documentation Revision** - Every proposed change must account for its documentation impact. Before finishing a change, check whether it affects best-practice counts, API endpoints/call counts, the `setup --guide` output, `README.md`, `ACCESS_GUIDE.md`, `CLAUDE.md`, skills, or Cursor rules — and update them in the same change. If a change has no documentation impact, state that explicitly with the reason.

## 7 Pillars (145 Best Practices)

1. **Data & AI Governance** (17) - Unity Catalog, metadata, lineage, data quality, group management, UC-registered models, inference tables
2. **Interoperability & Usability** (15) - Open formats, IaC, serverless, self-service
3. **Operational Excellence** (24) - CI/CD, MLOps, monitoring, environment isolation, rollbacks
4. **Security** (16) - IAM, SSO/SCIM, encryption, network, VPC/VNET, compliance, LLM guardrails, external-model secrets
5. **Reliability** (22) - ACID, auto-scaling, DR, backups, service principal ownership, provisioned throughput for LLM serving
6. **Performance** (28) - Serverless, data layout, liquid clustering, predictive optimization
7. **Cost** (23) - Spot/preemptible, reserved instances, tagging, budget alerts

## Adding New Code

- **New collectors**: Extend `BaseCollector` in `src/wal_e/collectors/base.py`, implement `collect()`, register in `AssessmentEngine`
- **New best practices**: Add to `pillars.py`, add scoring function to `scoring.py`, register in `SCORING_REGISTRY`
- **New reporters**: Extend `BaseReporter` in `src/wal_e/reporters/base.py`, implement `generate()`

## Output Deliverables

| File | Description |
|------|-------------|
| `WAL_Assessment_Readout.md` | Full detailed report (all 7 pillars) |
| `WAL_Assessment_Scores.csv` | 145 best practices with scores and notes |
| `WAL_Assessment_Presentation.pptx` | Executive readout deck (17 slides) |
| `WAL_Assessment_Presentation.html` | Browser-based presentation |
| `WAL_Assessment_Audit_Report.md` | Complete API call evidence trail |
