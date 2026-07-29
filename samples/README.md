# Sample WAL-E Output

This folder contains a complete example of the reports WAL-E generates from a
`--deep` assessment, so you can see the deliverables before running the tool
against your own workspace.

| File | Description |
|------|-------------|
| [`WAL_Assessment_Readout.md`](./WAL_Assessment_Readout.md) | Detailed findings across all 7 pillars (renders in your browser) |
| [`WAL_Assessment_Scores.csv`](./WAL_Assessment_Scores.csv) | Every best practice with its 0–2 score and notes |
| [`WAL_Assessment_Presentation.pptx`](./WAL_Assessment_Presentation.pptx) | Executive readout deck |
| [`WAL_Assessment_Remediation_Guide.docx`](./WAL_Assessment_Remediation_Guide.docx) | Prioritized remediation instructions |

## About this data

Everything here is **anonymized and illustrative**:

- Workspace host, metastore, catalog/cluster/warehouse/endpoint/job names, users,
  and emails are replaced with fake values (e.g. `acme_metastore`, `Sample User 01`).
- All cost, usage, and count figures are obfuscated — they do **not** reflect any
  real workspace.
- The API evidence trail (`WAL_Assessment_Audit_Report.md`) is omitted, since it
  captures raw responses.

Scores and maturity levels are shown so the sample stays realistic, but they are
derived from the anonymized data and are not a real assessment result.
