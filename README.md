# WAL-E: Well-Architected Lakehouse Evaluator

```
 __        __     _   ||  ___  _____
 \ \      / /    / \   ||       | ____|
  \ \ /\ / /    / _ \  ||       | |__
   \ V  V /    / ___ \ ||       |  __|
    \_/\_/    /_/   \_\||____ ___ |_|____
    Well-Architected Lakehouse Evaluator
```

> _"It only takes a moment..."_ to assess your entire Databricks Lakehouse.

---

## What is WAL-E?

**WAL-E** is an open-source assessment tool that automatically evaluates a Databricks workspace against the [Well-Architected Lakehouse Framework](https://docs.databricks.com/aws/en/lakehouse-architecture/well-architected). It turns a week-long manual assessment into a **15-minute automated scan**.

**WAL-E is designed to be run by the customer on their own system**, with a Databricks Solutions Architect guiding them through every step. No tokens, credentials, or data ever leave the customer's environment.

### How It Works

```
                    +--------------------+
                    |   Customer (You)   |
                    |  SA guides via     |
                    |  screen share      |
                    +--------+-----------+
                             |
                    +--------v-----------+
                    |    WAL-E Agent      |
                    |  (runs on YOUR     |
                    |   machine only)    |
                    +--------+-----------+
                             |
              +--------------+--------------+
              |              |              |
     +--------v---+  +------v------+  +----v-------+
     | Collectors  |  |  Scoring    |  | Reporters  |
     | (21 APIs)   |  |  Engine     |  | (5 formats)|
     | read-only   |  | 129 checks  |  | stays local|
     +--------+---+  +------+------+  +----+-------+
              |              |              |
     +--------v--------------v--------------v-------+
     |         Your Databricks Workspace            |
     |  (Unity Catalog, Clusters, Jobs, Security)   |
     +----------------------------------------------+
```

### Key Features

- **Customer-Run** - Runs entirely on the customer's machine; no token sharing, no credential handoff
- **SA-Guided** - Your Databricks SA walks you through every step via screen share or call
- **Automated Data Collection** - 23+ read-only API/CLI queries across governance, security, compute, operations, and cost
- **Cloud-Aware** - Auto-detects AWS, Azure, or GCP and tailors all scoring and recommendations
- **140 Best Practices** - 129 standard + 11 deep scan, scored 0-2 across 7 pillars
- **Multiple Output Formats** - Markdown, executive deck (PPTX), scored CSV, HTML presentation, and full audit trail
- **Zero Workspace Modification** - 100% read-only; no writes, no side effects
- **AI-Native** - Works as a Cursor skill, Claude Code skill, or MCP tool

### Security Model

| Concern                     | How WAL-E Handles It                                                                       |
| --------------------------- | ------------------------------------------------------------------------------------------ |
| **Who runs it?**            | The **customer** runs WAL-E on their own machine                                           |
| **Token sharing?**          | **None** — the customer creates and uses their own token locally                           |
| **Where do results go?**    | **Stays on the customer's machine** — nothing is transmitted externally                    |
| **What does the SA see?**   | Only what the customer **chooses to share** (e.g., via screen share or sending the report) |
| **What does WAL-E access?** | **Metadata only** — never reads table data, file contents, or secret values                |
| **How to clean up?**        | Customer revokes their own token and deletes local results                                 |

---

## Quick Start (For Customers)

Your Databricks SA will guide you through these steps on a call or screen share.

### Prerequisites

- Python 3.10+
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) v0.200+ configured with workspace access
- Workspace admin access (recommended for full assessment)

### Step 1: Install WAL-E

```bash
# Clone the repo (public — no authentication required)
git clone https://github.com/priyal-c/wal-e.git
cd wal-e

# Install
pip install -e .

# Or use the quick installer
./install.sh --cli
```

### Step 2: Configure Workspace Access

```bash
# Configure your Databricks CLI (you'll need your workspace URL)
databricks configure --profile wal-assessment \
  --host https://YOUR-WORKSPACE.cloud.databricks.com \
  --token

# When prompted, paste a PAT token you created as workspace admin
# (Settings > Developer > Access tokens > Generate — set lifetime to 1 day)
```

### Step 3: Validate Access

```bash
# Verify connectivity before running the full assessment
wal-e validate --profile wal-assessment
```

### Step 4: Run the Assessment

```bash
# Interactive mode (recommended — your SA will walk you through each step)
wal-e assess --profile wal-assessment --interactive

# Or quick scan (generates all reports automatically)
wal-e assess --profile wal-assessment --output ./my-assessment --format all
```

### Step 5: Review Results with Your SA

Your SA will help you interpret the results. Share the output folder or screen share.

The assessment generates these files in the output directory:

| File                                    | Description                                                     |
| --------------------------------------- | --------------------------------------------------------------- |
| `WAL_Assessment_Readout.md`             | Detailed assessment report (all 7 pillars)                      |
| `WAL_Assessment_Scores.csv`             | 140 best practices with scores and notes                        |
| `WAL_Assessment_Presentation.pptx`      | Executive readout deck (importable to Google Slides)            |
| `WAL_Assessment_Remediation_Guide.docx` | Detailed remediation instructions with cloud-specific doc links |
| `WAL_Assessment_Audit_Report.md`        | Complete evidence trail of all API calls                        |

### Understanding the Scores

WAL-E reports two key metrics for each pillar and overall:

| Metric             | What It Means                                                                                                                                                                                                                                                                                                                                         |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Verified Score** | The assessment score calculated _only_ from best practices where WAL-E had enough data to make a real determination. A score of 0 (not implemented) or 2 (fully implemented) is always verified. A score of 1 is verified only when WAL-E found real evidence (e.g., "no cluster policies found"), not when it defaulted to "cannot verify from API." |
| **Coverage**       | The percentage of best practices where WAL-E had real evidence to score. Higher coverage means more confidence in the verified score. Use `--deep` mode to increase coverage by querying system tables.                                                                                                                                               |

**Example reading:**

```
Performance Efficiency     █████████████░░  89%    64%
                           ^verified score         ^coverage
```

This means: of the performance BPs that WAL-E could verify (64%), the workspace scores 89%. The remaining 36% need `--deep` scan or manual verification.

**Maturity level** is derived from the verified score:

| Verified Score | Maturity Level |
| :------------: | -------------- |
|    88-100%     | Optimized      |
|     63-87%     | Established    |
|     25-62%     | Developing     |
|     0-24%      | Beginning      |

### Step 6: Clean Up

```bash
# Revoke your PAT token immediately after the assessment:
# Workspace > Settings > Developer > Access tokens > Revoke

# Delete the CLI profile:
# Edit ~/.databrickscfg and remove the [wal-assessment] section

# Optionally delete local assessment files after sharing with your SA
```

---

## For SAs: Guiding the Customer

As the SA, you don't need access to the customer's workspace. Your role is to guide them through the process.

### SA Workflow

```
1. Pre-Call Setup
   - Send the customer the Quick Reference Card (see ACCESS_GUIDE.md)
   - Ask them to install Python 3.10+ and Databricks CLI before the call
   - Schedule a 30-minute screen share session

2. On the Call (Customer shares their screen)
   - Guide them through 'git clone https://github.com/priyal-c/wal-e.git' and pip install
   - Walk them through 'databricks configure' with their own workspace URL
   - Have them create a short-lived PAT token (1 day lifetime)
   - Run 'wal-e validate' to confirm access
   - Run 'wal-e assess --interactive' together

3. Post-Assessment
   - Ask the customer to share the output folder (or screen share the results)
   - Walk through the readout deck together
   - Discuss findings and remediation priorities
   - Customer revokes their PAT token
```

### Showing the Setup Guide to Customers

```bash
# Print the customer-facing setup guide (share your screen or send the output)
wal-e setup --guide
```

---

## Advanced CLI Options

```bash
# Specify output formats
wal-e assess --format pptx --format html --format csv

# Set a custom timeout (seconds, default: 600, use 0 for no limit)
wal-e assess --timeout 0

# Run in background (useful inside AI coding tools)
wal-e assess --run-in-background --output ./assessment-results

# Re-generate reports from cached assessment data
wal-e report --input ./my-assessment --format all
```

### Deep Scan (System Tables)

The standard assessment uses 21 read-only API calls. For a deeper analysis, WAL-E can also query **Databricks system tables** to assess operational reality — actual cost trends, cluster idle time, query failure rates, job success rates, and security audit events.

```bash
# Deep scan requires a running SQL warehouse and SELECT grants on system.* schemas
wal-e assess --profile wal-assessment --deep --warehouse-id <YOUR_WAREHOUSE_ID>
```

Deep scan adds **11 additional best practices** (140 total) covering:

| Area            | What it reveals                                                  | System Table                                      |
| --------------- | ---------------------------------------------------------------- | ------------------------------------------------- |
| **Cost**        | Idle cluster waste, DBU spend trends, concentration risk         | `system.billing.usage`, `system.compute.clusters` |
| **Performance** | Query failure rate, slow query prevalence, warehouse utilization | `system.query.history`                            |
| **Reliability** | Job success rate, recurring job failures                         | `system.lakeflow.job_run_timeline`                |
| **Security**    | Failed login monitoring, permission change audit                 | `system.access.audit`                             |
| **Operations**  | Cluster utilization efficiency (24/7 clusters)                   | `system.compute.clusters`                         |

**Prerequisites for deep scan:**

```sql
-- Customer's account admin runs these in a SQL warehouse:
GRANT SELECT ON SCHEMA system.billing TO `your-admin-user@company.com`;
GRANT SELECT ON SCHEMA system.compute TO `your-admin-user@company.com`;
GRANT SELECT ON SCHEMA system.query TO `your-admin-user@company.com`;
GRANT SELECT ON SCHEMA system.access TO `your-admin-user@company.com`;
```

Without `--deep`, the 11 system-table BPs score as "partial" with a note explaining that deep scan is needed. This way the standard assessment still works perfectly with just the API.

---

## Integration with AI Dev Kit

WAL-E integrates natively with the [Databricks AI Dev Kit](https://github.com/databricks-solutions/ai-dev-kit).

### As a Cursor Skill

```bash
./install.sh --cursor
```

Then in Cursor Agent, ask:

> "Run a Well-Architected Lakehouse assessment on my workspace"

### As a Claude Code Skill

```bash
./install.sh --claude
```

Then ask naturally in Claude Code (no slash command):

> "Run a WAL-E assessment on my Databricks workspace and generate a readout deck"

> **Claude Code Timeout:** Claude Code's Bash tool has a max timeout of 10 minutes.
> Use `wal-e assess --timeout 0` for no limit, or `--run-in-background` for async execution.

### As an MCP Server

```bash
# Use the installer
./install.sh --mcp

# Or register manually
claude mcp add-json wal-e '{"command": "python3", "args": ["'$(pwd)'/mcp/server.py"]}'
```

Available MCP tools: `wal_e_assess`, `wal_e_collect`, `wal_e_score`, `wal_e_report`, `wal_e_validate`

---

## Access Requirements

> **Full guide:** See [ACCESS_GUIDE.md](ACCESS_GUIDE.md) for the complete self-service setup guide, permissions reference, and customer-facing instructions.

WAL-E needs **read-only** access to the workspace. It makes **21 HTTP GET API calls** and **zero write calls**.

### Permissions by Assessment Depth

| Access Level                          | What You Get                                        | Coverage |
| ------------------------------------- | --------------------------------------------------- | :------: |
| Regular user                          | Own clusters, permitted catalogs, own jobs          |   ~40%   |
| **Workspace admin**                   | All clusters, warehouses, security config, all jobs | **~80%** |
| **Workspace admin + Metastore admin** | Above + all catalogs, credentials, locations        | **~95%** |
| Above + System tables                 | Full above + billing, audit, query history          | **100%** |

**Recommended:** Workspace admin + Metastore admin for a meaningful assessment.

### What WAL-E Will NEVER Do

- Read table data, file contents, or query results
- Execute notebooks, jobs, or pipelines
- Create, modify, or delete any resource
- Start or stop any cluster or warehouse
- Access secret values (only scope names)
- Transmit data to any external service

---

## Architecture

```
                    +------------------+
                    |     Customer     |
                    | (runs on their   |
                    |  own machine)    |
                    +--------+---------+
                             |
                    +--------v---------+
                    |    WAL-E Agent    |
                    |  (CLI / Skill /  |
                    |   MCP Server)    |
                    +--------+---------+
                             |
              +--------------+--------------+
              |              |              |
     +--------v---+  +------v------+  +----v-------+
     | Collectors  |  |  Scoring    |  | Reporters  |
     | (23+ APIs)  |  |  Engine     |  | (5 formats)|
     +--------+---+  +------+------+  +----+-------+
              |              |              |
     +--------v--------------v--------------v-------+
     |       Customer's Databricks Workspace        |
     |  (Unity Catalog, Clusters, Jobs, Security)   |
     +----------------------------------------------+
```

| Component               | Path                                                  | Description |
| ----------------------- | ----------------------------------------------------- | ----------- |
| `src/wal_e/collectors/` | Data collection modules for each assessment area      |
| `src/wal_e/framework/`  | WAL pillar definitions, best practices, scoring logic |
| `src/wal_e/reporters/`  | Report generators (MD, CSV, HTML, PPTX, Audit)        |
| `src/wal_e/core/`       | Orchestration engine, config, cloud detection         |
| `mcp/`                  | MCP server for AI Dev Kit integration                 |

---

## WAL Framework Pillars

| #   | Pillar                         | Best Practices | Focus Areas                                                    |
| --- | ------------------------------ | :------------: | -------------------------------------------------------------- |
| 1   | Data & AI Governance           |       15       | Unity Catalog, metadata, lineage, data quality                 |
| 2   | Interoperability & Usability   |       14       | Open formats, IaC, serverless, self-service                    |
| 3   | Operational Excellence         |       24       | CI/CD, MLOps, monitoring, cluster utilization\*                |
| 4   | Security, Compliance & Privacy |       14       | IAM, SSO/SCIM, encryption, login audit*, permissions*          |
| 5   | Reliability                    |       21       | ACID, auto-scaling, DR, job success rate*, recurring failures* |
| 6   | Performance Efficiency         |       28       | Serverless, data layout, query failure rate*, slow queries*    |
| 7   | Cost Optimization              |       23       | Spot/preemptible, idle waste*, cost trends*, concentration\*   |
|     | **Total**                      |    **140**     | _\* = deep scan (system tables)_                               |

---

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Areas for Contribution

- Additional collectors (e.g., Databricks Apps, Clean Rooms, Marketplace)
- Custom scoring profiles per industry vertical
- Additional output formats (PDF, Notion, Confluence)
- System table query templates

---

## License

(c) 2026 Databricks, Inc. All rights reserved.
See [LICENSE.md](LICENSE.md) for details.
