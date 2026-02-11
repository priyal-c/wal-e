---
name: wal-e-skill
description: Run WAL-E (Well-Architected Lakehouse Evaluator) for Databricks workspace assessments. Use when the user wants to assess a Databricks workspace, score against the WAL framework, generate assessment reports, or answer questions about assessment results. Teaches Claude to run WAL-E, interpret scores, and produce custom reports.
---

# WAL-E Skill for Claude Code

## Purpose

This skill enables Claude to run the WAL-E assessment tool against Databricks workspaces, interpret results, generate reports, and answer questions about the Well-Architected Lakehouse Framework assessment.

---

## When to Use

- User asks to **assess** or **evaluate** a Databricks workspace
- User wants a **Well-Architected Lakehouse** or **WAL** assessment
- User needs **pillar scores**, **best practice scores**, or **maturity level**
- User requests **assessment reports**, **readout decks**, or **audit trails**
- User asks how to **remediate** low-scoring areas
- User wants **custom reports** or filtered views of assessment data

---

## Running WAL-E

### Validate Access First

```bash
python -m wal_e validate
```

Interpret: Success = ready for assessment. Failure = help user configure Databricks CLI and permissions.

### Full Assessment

```bash
python -m wal_e assess
```

Common options:
- `--profile <name>` – Databricks CLI profile
- `--output <path>` – Output directory
- `--format md,csv,html,pptx,audit` – Report formats

### Setup Guide (Customer Sessions)

```bash
python -m wal_e setup --guide
```

Use when walking a customer through access setup before assessment.

---

## Interpreting Results

### Scoring

| Score | Meaning |
|-------|---------|
| 0 | Not Implemented – prioritize |
| 1 | Partial – expand coverage |
| 2 | Full – maintain |

### Maturity Levels

- **Beginning** (avg < 0.5): Significant gaps
- **Developing** (0.5–1.25): Early adoption
- **Established** (1.25–1.75): Good coverage
- **Optimized** (≥ 1.75): Best practices widely adopted

### Output Files

| File | Use |
|------|-----|
| `WAL_Assessment_Readout.md` | Full report, all pillars |
| `WAL_Assessment_Scores.csv` | 99 best practices, scores, notes |
| `WAL_Assessment_Presentation.pptx` | Executive deck |
| `WAL_Assessment_Audit_Report.md` | API call evidence |

---

## Generating Custom Reports

1. Run assessment: `python -m wal_e assess --format csv`
2. Parse `WAL_Assessment_Scores.csv` for filtering
3. Filter by: pillar, score range (0, 1, or 2), specific best practice IDs
4. Summarize: pillar averages, top-variance pillars, remediation priorities
5. Format as: markdown summary, JSON, or custom template

### Example: Focus on Low Scores

```python
import csv
with open("assessment-results/WAL_Assessment_Scores.csv") as f:
    reader = csv.DictReader(f)
    low = [r for r in reader if r.get("score", "0") in ("0", "1")]
    # Present low-scoring items with finding_notes
```

---

## Answering Assessment Questions

| Question Type | Response Approach |
|---------------|-------------------|
| "What did we score?" | Read report, cite overall_score, maturity_level, pillar_scores |
| "Where are we weak?" | List pillars with score < 1.0, cite best practices with score 0 |
| "How do we fix X?" | Use finding_notes from report; add Databricks docs references |
| "Is this compliant?" | Point to audit report; note read-only nature of assessment |
| "Compare runs" | Parse multiple CSVs, diff scores, highlight changes |

---

## 7 Pillars Reference

1. **Data & AI Governance** – Unity Catalog, metadata, lineage, DQ
2. **Interoperability & Usability** – Open formats, serverless, self-service
3. **Operational Excellence** – CI/CD, MLOps, monitoring, DLT
4. **Security** – IAM, encryption, network, compliance
5. **Reliability** – ACID, auto-scaling, backups, DR
6. **Performance** – Serverless, Photon, caching, monitoring
7. **Cost** – Right-sizing, auto-scaling, tagging

---

## Constraints

- **Read-only:** WAL-E never modifies the workspace
- **Validate first:** Always run validate before assess
- **Audit trail:** Include audit report for compliance deliverables
- **Profiles only:** Use Databricks CLI profiles; no hardcoded credentials
