# WAL-E User Guide for Solutions Architects

**Well-Architected Lakehouse Evaluator — Internal SA Guide**

_Use this guide to run and present WAL-E with customers. You can copy this entire document into a new Google Doc (File → New → Document, then paste) to share with your team._

---

## 1. What is WAL-E?

WAL-E is an open-source assessment tool that automatically evaluates a Databricks workspace against the **Well-Architected Lakehouse Framework**. It turns a week-long manual assessment into a **15-minute automated scan**.

**Operating model**

- The **customer** runs WAL-E on their own machine.
- The **SA** guides via screen share — no token or data ever leaves the customer environment.

**Key numbers**

- **140 best practices** across 7 pillars (129 standard + 11 deep scan).
- **21+ read-only API calls** — zero writes, zero data reads.
- **5 output formats**: Markdown readout, PPTX deck, CSV scores, DOCX remediation guide, audit report.

---

## 2. For SAs: How to Use WAL-E with Customers

### Before the Call

**Send the customer:**

1. **Prerequisites**
   - Python 3.10+
   - Databricks CLI v0.200+ (`pip install databricks-cli` or `brew install databricks`)
   - Workspace admin (and metastore admin if possible)
   - Their workspace URL

2. **Short pre-call message (copy-paste)**

```
Hi [Customer Name],

We'll run a Well-Architected Lakehouse assessment on your workspace. You'll run everything on YOUR machine; I'll guide you on the call.

Before the call, please:
• Install Python 3.10+ and Databricks CLI
• Have your workspace URL and workspace admin access ready

On the call we'll: install WAL-E (2 min), create a temporary token (1 min), run the assessment (~10 min), and review results. No data leaves your machine. Total time: ~30 minutes.
```

### During the Call

1. **Customer shares screen** (they run all commands).
2. **Clone & install** (use public repo, no auth):
   ```
   git clone https://github.com/priyal-c/wal-e.git
   cd wal-e
   pip install -e .
   ```
3. **Customer creates a 1-day PAT** in workspace: Settings → Developer → Access tokens → Generate.
4. **Configure CLI:**
   ```
   databricks configure --profile wal-assessment --host https://<their-workspace>.cloud.databricks.com --token
   ```
5. **Validate:** `wal-e validate --profile wal-assessment`
6. **Run assessment:** `wal-e assess --profile wal-assessment --interactive` (or `--output ./my-assessment --format all` for non-interactive).
7. **Review results** together (readout, PPTX, or HTML).
8. **Customer revokes the PAT** and removes the CLI profile after the call.

### After the Call

- Customer can share the output folder or specific reports at their discretion.
- Use the generated PPTX for executive readouts.
- Schedule follow-up to discuss remediation using the DOCX remediation guide.

---

## 3. Quick Reference: Commands

| Step                 | Command                                                                              |
| -------------------- | ------------------------------------------------------------------------------------ |
| Clone                | `git clone https://github.com/priyal-c/wal-e.git && cd wal-e`                        |
| Install              | `pip install -e .`                                                                   |
| Configure            | `databricks configure --profile wal-assessment --host <WORKSPACE_URL> --token`       |
| Validate             | `wal-e validate --profile wal-assessment`                                            |
| Assess (interactive) | `wal-e assess --profile wal-assessment --interactive`                                |
| Assess (all reports) | `wal-e assess --profile wal-assessment --output ./my-assessment --format all`        |
| Deep scan            | `wal-e assess --profile wal-assessment --deep --output ./my-assessment --format all` |

---

## 4. Output Files (What the Customer Gets)

| File                                  | Description                             |
| ------------------------------------- | --------------------------------------- |
| WAL_Assessment_Readout.md             | Full report, all 7 pillars              |
| WAL_Assessment_Scores.csv             | 140 best practices with scores          |
| WAL_Assessment_Presentation.pptx      | Executive deck (use for readouts)       |
| WAL_Assessment_Remediation_Guide.docx | Step-by-step remediation with doc links |
| WAL_Assessment_Audit_Report.md        | Evidence trail of every API call        |

---

## 5. Understanding Scores (For You and the Customer)

- **Verified score** — Based only on best practices where WAL-E had enough data to decide (0, 1, or 2). More reliable.
- **Coverage** — % of BPs with real evidence. Use `--deep` to increase coverage (system tables).
- **Maturity:** 88–100% = Optimized; 63–87% = Established; 25–62% = Developing; 0–24% = Beginning.

---

## 6. Security & Privacy (Customer Talking Points)

- **Who runs it?** Customer, on their machine.
- **Token sharing?** None — token stays in their `~/.databrickscfg`.
- **Where do results go?** Only their machine.
- **What does the SA see?** Only what the customer shares (screen or files).
- **What does WAL-E read?** Metadata only — no table data, no secret values, no file contents.
- **Audit:** `WAL_Assessment_Audit_Report.md` lists every API call for security review.

---

## 7. Troubleshooting

| Symptom               | Likely cause             | Action                          |
| --------------------- | ------------------------ | ------------------------------- |
| 401 Unauthorized      | Token expired/invalid    | Regenerate PAT                  |
| 403 on workspace-conf | Not workspace admin      | Use admin account for token     |
| 403 on catalogs       | Not metastore admin      | Use metastore admin             |
| Empty cluster list    | Insufficient permissions | Use admin account               |
| Connection/SSL errors | Network/proxy            | Check URL, VPN, corporate proxy |

**Coverage by role:** Regular user ~40%; Workspace admin ~80%; + Metastore admin ~95%; + System tables 100%.

---

## 8. Deep Scan (Optional)

For cost, query performance, and audit insights, the customer can grant system table access and run:

```bash
wal-e assess --profile wal-assessment --deep --output ./my-assessment --format all
```

Their admin runs in SQL warehouse:

```sql
GRANT SELECT ON SCHEMA system.billing TO `user@company.com`;
GRANT SELECT ON SCHEMA system.compute TO `user@company.com`;
GRANT SELECT ON SCHEMA system.query TO `user@company.com`;
GRANT SELECT ON SCHEMA system.access TO `user@company.com`;
```

---

## 9. Presenting WAL-E Internally (To Other SAs)

**Suggested flow for your internal SA presentation:**

1. **What & why** — WAL-E automates Well-Architected Lakehouse assessments; customer-run, SA-guided, no data/token sharing.
2. **Demo** — Short screen share: clone → install → configure → validate → assess → show readout/PPTX.
3. **Security one-pager** — Use Section 6 above; emphasize metadata-only, audit trail, customer control.
4. **Handoff** — Share this User Guide (or a Google Doc version) and the public repo: https://github.com/priyal-c/wal-e
5. **Q&A** — Deep scan, permissions, troubleshooting (Section 7), and when to use interactive vs batch.

**Repo link to share:** https://github.com/priyal-c/wal-e

---

## 10. Links & References

- **Public repo:** https://github.com/priyal-c/wal-e
- **Well-Architected Lakehouse:** https://docs.databricks.com/aws/en/lakehouse-architecture/well-architected
- **Databricks CLI:** https://docs.databricks.com/dev-tools/cli/install.html

---

_WAL-E User Guide for SAs | Internal use | Share with teams via Google Doc or this file._
