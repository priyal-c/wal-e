# FEINFRA Public Repo Creation – Required Inputs for WAL-E

Use this when creating a FEINFRA ticket for `databricks-solutions/wal-e`.

## Created Ticket

**FEINFRA-1927** – https://databricks.atlassian.net/browse/FEINFRA-1927

Values used: Primary owner `priyal-chindarkar`, Secondary owner `calreynolds`. Update in Jira if either needs to change.

## Pre-requisites (do these first)

1. **Request databricks-solutions org access in Opal**
   - Search: `app.github-databricks-solutions`
   - Justification: _"I need access to the databricks-solutions GitHub organization to contribute and maintain field engineering tools and demos for customer engagements."_

2. **Video guide**: https://www.loom.com/share/e112bc94873b497da25403ef7bffb3fc

---

## Required Fields for FEINFRA Public Repo Creation

| Field                                                                                   | Value                                        | Notes                                                |
| --------------------------------------------------------------------------------------- | -------------------------------------------- | ---------------------------------------------------- |
| **Summary**                                                                             | Create wal-e repo in databricks-solutions    | Short title                                          |
| **Repo Name** (customfield_20002)                                                       | `wal-e`                                      | Only `-` allowed as special character                |
| **External Code Share Category** (customfield_22794)                                    | Completely Public → **Databricks Solutions** | Cascading select                                     |
| **Github username of primary owner** (customfield_20004)                                | _[YOUR_GITHUB_USERNAME]_                     | Must have databricks-solutions access                |
| **Github username of secondary owner** (customfield_18768)                              | _[SECONDARY_OWNER_GITHUB_USERNAME]_          | Must be different from primary; must have org access |
| **I Acknowledge the following Legal and Security Responsibilities** (customfield_19185) | All 7 checkboxes                             | See below                                            |
| **Description**                                                                         | _[See template below]_                       | Optional but recommended                             |
| **SFDC link** (customfield_17056)                                                       | _[Optional]_                                 | Only if linked to customer/opportunity               |

---

## Legal & Security Acknowledgments (all required)

You must acknowledge all of these:

1. There will be no non-public information in this repo (customer data, PII, proprietary info)
2. There will be no access tokens, PAT, passwords or credentials
3. Repository only contains synthetically generated data (Faker, dbldatagen, LLAMA-4) if applicable
4. All 3rd party code/assets acknowledged with license (Apache, BSD, MIT, DB)
5. All published content peer reviewed by at least one team member/SME
6. Repo will be reviewed annually and archived if no longer needed
7. Repo owners will take timely action on security/legal violations; admins may archive if no remediation

---

## Description Template (copy into ticket)

```
Create a new repository in databricks-solutions for WAL-E (Well-Architected Lakehouse Evaluator).

What this repo is for:
WAL-E is an open-source assessment tool that automatically evaluates a Databricks workspace against the Well-Architected Lakehouse Framework. It turns a week-long manual assessment into a 15-minute automated scan. Designed for customer-run, SA-guided engagements—no tokens or data leave the customer environment.

Why / Problem statement:
Manual Well-Architected assessments are time-consuming and inconsistent. WAL-E automates 140 best practices across 7 pillars with verified scoring, cloud-aware detection (AWS/Azure/GCP), and multiple output formats (Markdown, PPTX, CSV, DOCX remediation guide).

Scope:
- 129 standard + 11 deep-scan best practices
- Read-only API/CLI collection
- Customer-run, SA-guided model
- Zero workspace modification

Deliverables:
- CLI tool with --deep scan mode
- README, User Guide (DOCX), remediation guide (DOCX)
- Cursor/Claude/MCP skill support

Audience:
Databricks Solutions Architects for customer workspace assessments.
```

---

## After Creating the Ticket

1. **Notify** Cal Reynolds or Quentin Ambard on the ticket
2. **Workflow**: Repo is auto-created → Legal/EntSec review → Move to public when ready
3. **Push code** after repo exists:
   ```bash
   git remote add databricks-solutions https://github.com/databricks-solutions/wal-e.git
   git push databricks-solutions main
   ```

---

## Links

- FEINFRA board: https://databricks.atlassian.net/jira/software/projects/FEINFRA/boards/1339
- FEINFRA process doc: go/feinfra-jira
- GitHub GHEC org access: Follow doc linked in FEINFRA process
