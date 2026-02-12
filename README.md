# WAL-E: Well-Architected Lakehouse Evaluator

```
 __        __    _       _____
 \ \      / /   / \     | ____|
  \ \ /\ / /   / _ \    | |__   
   \ V  V /   / ___ \   |  __|  
    \_/\_/   /_/   \_\  |_|____
    Well-Architected Lakehouse Evaluator
```

> *"It only takes a moment..."* to assess your entire Databricks Lakehouse.

---

## What is WAL-E?

**WAL-E** is an agentic assessment tool that automatically evaluates a Databricks workspace against the [Well-Architected Lakehouse Framework](https://docs.databricks.com/aws/en/lakehouse-architecture/well-architected). Built by Field Engineering for SAs, it turns a week-long manual assessment into a **15-minute automated scan**.

WAL-E connects to a customer's Databricks workspace via CLI, queries 23+ API endpoints and system tables, scores **99 best practices** across **7 pillars**, and generates a complete readout deck -- ready to present.

### Key Features

- **Automated Data Collection** - 23+ API/CLI queries across governance, security, compute, operations, and cost
- **Framework-Aligned Scoring** - Every best practice from the WAL Assessment Tool, scored 0-2
- **Multiple Output Formats** - Markdown report, executive deck, scored CSV, HTML presentation, PPTX (Google Slides), and full audit trail
- **AI-Native** - Works as a Cursor skill, Claude Code skill, or MCP tool via AI Dev Kit
- **Interactive SA Flow** - Guided walkthrough for customer collaboration
- **Zero Workspace Modification** - 100% read-only; no writes, no side effects

---

## Architecture

```
                    +------------------+
                    |   SA / Customer  |
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
     | (23+ APIs)  |  |  Engine     |  | (6 formats)|
     +--------+---+  +------+------+  +----+-------+
              |              |              |
     +--------v--------------v--------------v-------+
     |           Databricks Workspace               |
     |  (Unity Catalog, Clusters, Jobs, Security)   |
     +----------------------------------------------+
```

### Components

| Component | Description |
|-----------|-------------|
| `src/wal_e/collectors/` | Data collection modules for each assessment area |
| `src/wal_e/framework/` | WAL pillar definitions, best practices, scoring logic |
| `src/wal_e/reporters/` | Report generators (MD, CSV, HTML, PPTX, Audit) |
| `src/wal_e/core/` | Orchestration engine, config, CLI |
| `src/wal_e/skills/` | AI assistant skill definitions |
| `mcp/` | MCP server for AI Dev Kit integration |

---

## Quick Start

### Prerequisites

- Python 3.10+
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) v0.200+ configured with workspace access
- Read-only access to the target workspace (admin recommended for full assessment)

### Install

```bash
# Clone the repo
git clone https://github.com/databricks-solutions/wal-e.git
cd wal-e

# Install dependencies
pip install -e .

# Or use the quick installer
curl -sL https://raw.githubusercontent.com/databricks-solutions/wal-e/main/install.sh | bash
```

### Run Assessment

```bash
# Interactive mode (recommended for customer sessions)
wal-e assess --interactive

# Quick scan with default profile
wal-e assess

# Specify profile and output directory
wal-e assess --profile customer-workspace --output ./assessment-results

# Generate specific output format
wal-e assess --format pptx --format html --format csv
```

### Customer Collaboration Flow

```bash
# 1. SA walks customer through access setup
wal-e setup --guide

# 2. Validate access before full scan
wal-e validate

# 3. Run full assessment
wal-e assess --interactive

# 4. Generate readout deck
wal-e report --format all
```

---

## Integration with AI Dev Kit

WAL-E integrates natively with the [Databricks AI Dev Kit](https://github.com/databricks-solutions/ai-dev-kit).

### As a Cursor Skill

```bash
# Install WAL-E skill into your Cursor project
./install.sh --cursor
```

Then in Cursor Agent, simply ask:
> "Run a Well-Architected Lakehouse assessment on my workspace"

Cursor reads the `.cursor/rules/wal-e-assessment.md` rule file automatically.

### As a Claude Code Skill

```bash
# Install WAL-E skill for Claude Code
./install.sh --claude
```

Then open Claude Code inside the `wal-e/` directory and ask naturally (no slash command):
> "Run a WAL-E assessment on my Databricks workspace and generate a readout deck"

Claude Code automatically reads `CLAUDE.md` from the project root and knows how to use WAL-E.

### As an MCP Server (AI Dev Kit)

```bash
# Register WAL-E as an MCP tool
claude mcp add-json wal-e "{
  \"command\": \"python\",
  \"args\": [\"$(pwd)/mcp/server.py\"]
}"
```

Available MCP tools:
- `wal_e_assess` - Run full assessment
- `wal_e_collect` - Collect workspace data
- `wal_e_score` - Score against framework
- `wal_e_report` - Generate reports
- `wal_e_validate` - Validate workspace access

---

## Access Requirements

> **Full guide:** See [ACCESS_GUIDE.md](ACCESS_GUIDE.md) for the complete access reference including customer-facing instructions, troubleshooting, and security assurances.

WAL-E needs **read-only** access to the target workspace. It makes **21 HTTP GET API calls** and **zero write calls**. No data is read from tables -- only workspace metadata and configuration.

### Quick Setup (3 minutes)

```bash
# 1. Customer creates a short-lived PAT token (as workspace admin)
#    Workspace > Settings > Developer > Access tokens > Generate
#    Lifetime: 1 day | Description: "WAL-E Assessment"

# 2. SA configures the CLI profile
databricks configure --profile customer-assessment \
  --host https://customer-workspace.cloud.databricks.com \
  --token

# 3. Validate before running
wal-e validate --profile customer-assessment
```

### Permissions by Assessment Depth

| Access Level | What You Get | Coverage |
|-------------|-------------|:--------:|
| Regular user | Own clusters, permitted catalogs, own jobs | ~40% |
| **Workspace admin** | All clusters, warehouses, security config, all jobs | **~80%** |
| **Workspace admin + Metastore admin** | Above + all catalogs, credentials, locations | **~95%** |
| Above + System tables | Full above + billing, audit, query history | **100%** |

**Recommended:** Workspace admin + Metastore admin for a meaningful assessment.

### API Endpoints Called (All Read-Only)

| Category | Endpoints | Count | Admin Required? |
|----------|-----------|:-----:|:---------------:|
| **Authentication** | `auth describe`, `current-user me` | 2 | No |
| **Unity Catalog** | `metastore_summary`, `catalogs`, `external-locations`, `storage-credentials` | 4 | Metastore admin for full list |
| **Compute** | `clusters/list`, `sql/warehouses`, `cluster-policies/list`, `instance-pools/list` | 4 | Admin for all clusters |
| **Security** | `workspace-conf`, `ip-access-lists`, `token/list` | 3 | **Yes** (workspace admin) |
| **Operations** | `jobs/list`, `pipelines`, `serving-endpoints`, `repos`, `global-init-scripts`, `groups/list`, `secrets/list-scopes` | 7 | Admin for complete lists |
| **Workspace** | `workspace/list` (root only) | 1 | Admin for full listing |
| **Total** | | **21** | |

### What WAL-E Will NEVER Do

- Read table data, file contents, or query results
- Execute notebooks, jobs, or pipelines
- Create, modify, or delete any resource
- Start or stop any cluster or warehouse
- Access secret values (only scope names)
- Transmit data to any external service

### Security for the Customer

- All API calls use **HTTPS/TLS**
- Assessment results are stored **locally on the SA's machine only**
- A complete **audit trail** of every API call is provided as a deliverable
- The PAT token can be **revoked immediately** after the assessment
- Set token lifetime to **1 day** (assessment takes ~15 minutes)

### Optional: System Tables for Deeper Analysis

For cost, performance, and audit insights, customers can optionally grant SELECT on system tables:

```sql
GRANT SELECT ON SCHEMA system.billing TO `sa-user@databricks.com`;
GRANT SELECT ON SCHEMA system.compute TO `sa-user@databricks.com`;
GRANT SELECT ON SCHEMA system.query TO `sa-user@databricks.com`;
GRANT SELECT ON SCHEMA system.access TO `sa-user@databricks.com`;
```

See [ACCESS_GUIDE.md](ACCESS_GUIDE.md) for the full list of system tables used and customer-facing copy-paste instructions.

---

## Output Deliverables

WAL-E generates 6 deliverables in a single run:

| File | Description |
|------|-------------|
| `WAL_Assessment_Readout.md` | Detailed assessment report (all 7 pillars) |
| `WAL_Assessment_Executive_Deck.md` | 15-slide executive summary |
| `WAL_Assessment_Scores.csv` | Scored assessment tool (99 best practices) |
| `WAL_Assessment_Presentation.html` | Browser-based presentation |
| `WAL_Assessment_Presentation.pptx` | Google Slides / PowerPoint importable |
| `WAL_Assessment_Audit_Report.md` | Complete evidence trail of all API calls |

---

## WAL Framework Pillars

| # | Pillar | Best Practices | Focus Areas |
|---|--------|:--------------:|-------------|
| 1 | Data & AI Governance | 12 | Unity Catalog, metadata, lineage, data quality |
| 2 | Interoperability & Usability | 14 | Open formats, IaC, serverless, self-service |
| 3 | Operational Excellence | 18 | CI/CD, MLOps, monitoring, environment isolation |
| 4 | Security, Compliance & Privacy | 7 | IAM, encryption, network, compliance |
| 5 | Reliability | 14 | ACID, auto-scaling, DR, backups |
| 6 | Performance Efficiency | 19 | Serverless, data layout, caching, monitoring |
| 7 | Cost Optimization | 15 | Resource selection, auto-scaling, tagging, monitoring |
| | **Total** | **99** | |

---

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Areas for Contribution
- Additional collectors (e.g., Databricks Apps, Clean Rooms, Marketplace)
- Cloud-specific checks (AWS vs Azure vs GCP)
- Custom scoring profiles per industry vertical
- Additional output formats (PDF, Notion, Confluence)
- System table query templates

---

## License

(c) 2026 Databricks, Inc. All rights reserved.
See [LICENSE.md](LICENSE.md) for details.
