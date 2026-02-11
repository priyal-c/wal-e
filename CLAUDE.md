# CLAUDE.md - WAL-E Project Rules

## Overview

WAL-E (Well-Architected Lakehouse Evaluator) is an automated assessment tool for Databricks workspaces. It evaluates workspaces against the [Well-Architected Lakehouse Framework](https://docs.databricks.com/lakehouse-architecture/well-architected) by querying Databricks APIs, scoring 99 best practices across 7 pillars, and generating assessment reports.

## How to Run

```bash
# Full assessment (collect data, score, and generate reports)
python -m wal_e assess

# Validate workspace access before running assessment
python -m wal_e validate

# Show access setup guide for customer sessions
python -m wal_e setup --guide
```

## Architecture

| Component | Path | Description |
|-----------|------|-------------|
| **Collectors** | `src/wal_e/collectors/` | Data collection from Databricks APIs (23+ endpoints) |
| **Framework** | `src/wal_e/framework/` | WAL scoring engine (99 best practices, 7 pillars) |
| **Reporters** | `src/wal_e/reporters/` | Report generators (MD, CSV, HTML, PPTX, Audit) |
| **Core** | `src/wal_e/core/` | Orchestration engine and config |
| **Skills** | `src/wal_e/skills/` | AI assistant skill definitions |
| **MCP** | `mcp/` | MCP server for AI Dev Kit integration |

## Key Rules

1. **READ-ONLY ACCESS** - All workspace access is read-only. Never modify the customer workspace. WAL-E only queries APIs and system tables.

2. **Validate First** - Always validate access with `python -m wal_e validate` before running a full assessment.

3. **Scoring Scale** - Best practices use a 0-2 scale:
   - **0** = Not Implemented
   - **1** = Partial
   - **2** = Full

4. **Audit Trail** - All reports must include or reference an audit trail of API calls made. Use the audit reporter for compliance.

5. **Configuration** - WAL-E uses `~/.databrickscfg` for host and token. Set `profile_name` for different workspaces.

## 7 Pillars (99 Best Practices)

1. **Data & AI Governance** (12) - Unity Catalog, metadata, lineage, data quality
2. **Interoperability & Usability** (15) - Open formats, IaC, serverless, self-service
3. **Operational Excellence** (20) - CI/CD, MLOps, monitoring, environment isolation
4. **Security** (7) - IAM, encryption, network, compliance
5. **Reliability** (18) - ACID, auto-scaling, DR, backups
6. **Performance** (21) - Serverless, data layout, caching, monitoring
7. **Cost** (18) - Resource selection, auto-scaling, tagging, monitoring

## Adding New Code

- **New collectors**: Extend `BaseCollector` in `src/wal_e/collectors/base.py`, implement `collect()`, register in `AssessmentEngine`
- **New best practices**: Add to `pillars.py`, add scoring function to `scoring.py`, register in `SCORING_REGISTRY`
- **New reporters**: Extend `BaseReporter` in `src/wal_e/reporters/base.py`, implement `generate()`
