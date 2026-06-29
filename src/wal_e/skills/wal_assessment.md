---
name: wal-assessment
description: Run WAL-E (Well-Architected Lakehouse Evaluator) to assess Databricks workspaces against the Well-Architected Lakehouse Framework. Use when the user wants to evaluate a Databricks workspace, generate architecture assessment reports, score 145 best practices across 7 pillars, or present findings to customers. Supports Cursor, Claude Code, Windsurf, and other AI assistants.
---

# WAL-E Assessment Skill

## Overview

WAL-E is an agentic assessment tool that evaluates Databricks workspaces against the [Well-Architected Lakehouse Framework](https://docs.databricks.com/lakehouse-architecture/well-architected). It queries 27+ Databricks APIs (including Mosaic AI / GenAI assets), scores 145 best practices across 7 pillars, and generates reports ready for customer presentations.

---

## Well-Architected Lakehouse Framework (7 Pillars, 145 Best Practices)

| # | Pillar | Best Practices | Focus |
|---|--------|:--------------:|-------|
| 1 | Data & AI Governance | 17 | Unity Catalog, metadata, lineage, data quality, UC models, inference tables |
| 2 | Interoperability & Usability | 15 | Open formats, IaC, serverless, self-service, Delta Sharing |
| 3 | Operational Excellence | 24 | CI/CD, MLOps, monitoring, environment isolation, DLT |
| 4 | Security | 16 | IAM, encryption, network, compliance, LLM guardrails, external-model secrets |
| 5 | Reliability | 22 | ACID, auto-scaling, DR, backups, provisioned throughput for LLM serving |
| 6 | Performance | 28 | Serverless, data layout, caching, Photon, monitoring |
| 7 | Cost | 23 | Right-sizing, auto-scaling, tagging, chargeback |

**Total:** 134 standard + 11 deep scan (system tables) = 145. **Scoring scale:** 0 = Not Implemented, 1 = Partial, 2 = Full

---

## Step-by-Step Workflow

### 1. Verify Prerequisites

- Python 3.10+
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) v0.200+ installed and configured
- Read-only workspace access (admin recommended for full assessment)

### 2. Validate Access

```bash
python -m wal_e validate
```

If validation fails, guide the user to:
- Run `databricks configure --profile <name>`
- Ensure token has read access to clusters, SQL warehouses, Unity Catalog, jobs, pipelines, etc.

### 3. Run Assessment

```bash
python -m wal_e assess
```

Options:
- `--profile <name>` – Databricks CLI profile
- `--output <dir>` – Output directory (default: `./assessment-results`)
- `--format md,csv,html,pptx,audit` – Report formats
- `--deep --warehouse-id <ID>` – Add 11 system-table best practices (cost trends, idle clusters, query/job failure rates, audit events). Requires a running SQL warehouse and `SELECT` on `system.*` schemas (`billing`, `compute`, `query`, `access`, `lakeflow`).

### 4. Interpret Results

- **Overall score:** Average of the scored best practices (0–2 scale)
- **Maturity levels:** Beginning (<0.5), Developing (0.5–1.25), Established (1.25–1.75), Optimized (≥1.75)
- **Pillar scores:** Per-pillar averages; identify weak areas
- **Finding notes:** Each best practice includes remediation guidance

### 5. Present Findings

- Use `WAL_Assessment_Readout.md` for detailed review
- Use `WAL_Assessment_Presentation.pptx` or `.html` for executive decks
- Use `WAL_Assessment_Audit_Report.md` for compliance and evidence

---

## Access Requirements

| Permission | Purpose |
|------------|---------|
| Cluster list | Compute configuration |
| SQL Warehouse list | Self-service analytics |
| Unity Catalog read | Governance, metadata |
| Jobs, pipelines list | Operational maturity |
| Cluster policies | Standardization |
| Workspace conf, IP access lists | Security |
| Secret scope list | Security assessment |

---

## Remediation Guidance

When a best practice scores 0 or 1, provide guidance from the finding notes. Common remediations:

- **Unity Catalog:** Migrate to UC, enable lineage, add descriptions
- **Delta Lake:** Migrate tables to Delta format
- **DLT:** Adopt Delta Live Tables for pipelines
- **Serverless:** Use Pro/serverless SQL warehouses
- **Cluster policies:** Define policies for standardized compute
- **Auto-termination:** Enable auto-stop on warehouses and clusters
- **Tags:** Add cost allocation tags for chargeback

---

## Example Conversations

**User:** "Assess my Databricks workspace against the Well-Architected Framework."

**Assistant:** I'll run WAL-E to assess your workspace. First, let me validate access:
```bash
python -m wal_e validate
```
If that succeeds, I'll run the full assessment:
```bash
python -m wal_e assess --output ./assessment-results --format md csv audit
```
You'll get pillar scores, 145 best-practice scores, and an audit trail.

---

**User:** "What pillars need the most work?"

**Assistant:** I'll run WAL-E and then analyze the pillar scores. The report will show per-pillar averages. Pillars with scores &lt; 1.0 typically need the most remediation. I can also filter the CSV for best practices scored 0 or 1.

---

**User:** "Generate a readout deck for my customer."

**Assistant:** Running WAL-E with PPTX format for an executive deck:
```bash
python -m wal_e assess --format pptx md audit
```
Output: `WAL_Assessment_Presentation.pptx` and `WAL_Assessment_Readout.md`. Include the audit report for compliance.

---

## Critical Rules

1. **Read-only only** – WAL-E never modifies the workspace
2. **Validate first** – Always run `validate` before `assess`
3. **Audit trail** – Include audit report in compliance-sensitive deliverables
4. **No hardcoded credentials** – Use Databricks CLI profiles only
