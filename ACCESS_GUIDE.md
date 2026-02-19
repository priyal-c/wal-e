# WAL-E Access & Setup Guide
## Customer Self-Service with SA Guidance

---

> **TL;DR:** You (the customer) install and run WAL-E on **your own machine**. Your Databricks SA guides you through the process via screen share. No tokens or data leave your environment.

---

## Table of Contents

1. [How This Works](#1-how-this-works)
2. [What WAL-E Collects (and Doesn't)](#2-what-wal-e-collects-and-doesnt)
3. [Prerequisites](#3-prerequisites)
4. [Step-by-Step Setup (Customer)](#4-step-by-step-setup-customer)
5. [Required Permissions by Collector](#5-required-permissions-by-collector)
6. [Complete API Endpoint Reference](#6-complete-api-endpoint-reference)
7. [Optional: System Tables for Deep Assessment](#7-optional-system-tables-for-deep-assessment)
8. [Security & Privacy Assurances](#8-security--privacy-assurances)
9. [Troubleshooting](#9-troubleshooting)
10. [For SAs: Guiding the Customer](#10-for-sas-guiding-the-customer)

---

## 1. How This Works

WAL-E is designed so the **customer runs everything on their own system**. The SA never needs access to your workspace, tokens, or data.

```
┌──────────────────────────────────────────────────────────────┐
│                    YOUR ENVIRONMENT (CUSTOMER)                │
│                                                              │
│  1. You install WAL-E on your laptop/VM                      │
│  2. You create a short-lived PAT token (expires in 1 day)    │
│  3. You run the assessment (15 minutes)                      │
│  4. Results are saved locally on YOUR machine                │
│  5. You share the report with your SA (your choice)          │
│  6. You revoke the token and delete local files              │
│                                                              │
│  Your SA joins via screen share and guides each step.        │
│  The SA never touches your workspace or credentials.         │
└──────────────────────────────────────────────────────────────┘
```

**Key facts:**
- All API calls are **read-only** (HTTP GET only)
- **No data is read** from tables, volumes, or storage — only metadata and configuration
- **No resources are created, modified, or deleted**
- Your token **never leaves your machine**
- A full **audit trail** of every API call is provided so you can verify exactly what was accessed

---

## 2. What WAL-E Collects (and Doesn't)

### What IS Collected (Metadata Only)

| Category | What's Read | Example |
|----------|------------|---------|
| **Identity** | Current user info, group memberships | User email, group names |
| **Unity Catalog** | Metastore config, catalog names & owners, external locations, storage credentials | Catalog count, isolation modes |
| **Compute** | Cluster names & states, SQL warehouse configs, cluster policies, instance pools | Running cluster count, warehouse sizes |
| **Security** | Workspace settings (DBFS browser, export, token lifetime), IP access lists | Config flags (true/false) |
| **Operations** | Job names, pipeline states, serving endpoints, git repos, init scripts, secret scope names | Job count, pipeline failure states |
| **Workspace** | Root-level directory listing (names & types only) | Folder names, notebook counts |

### What is NOT Collected

| NOT Collected | Why |
|---------------|-----|
| Table data / row content | WAL-E never reads actual data from tables |
| File/volume contents | No files or volumes are accessed |
| Secret values | Only scope names are listed, never secret values |
| Notebook code | No notebook content is read |
| Query text / SQL statements | No query history content is accessed |
| Credentials / tokens of other users | Only the assessment token's own metadata |

---

## 3. Prerequisites

You need these on **your** machine (the customer's machine):

| Requirement | Details | Install |
|-------------|---------|---------|
| **Python** | 3.10 or newer | [python.org](https://python.org) |
| **Databricks CLI** | v0.200+ | `pip install databricks-cli` or `brew install databricks` |
| **Git** | For cloning WAL-E | Usually pre-installed on Mac/Linux |
| **GitHub CLI** | Optional, for easy cloning | `brew install gh` or [cli.github.com](https://cli.github.com) |
| **Network** | Outbound HTTPS (443) to your Databricks workspace URL | Usually already available |

Your SA can help you verify these during the setup call.

---

## 4. Step-by-Step Setup (Customer)

Your SA will walk you through these steps on a screen share call.

### Step 1: Install WAL-E (2 minutes)

```bash
# Clone the repository
gh repo clone priyal-chindarkar_data/wal-e
cd wal-e

# Install WAL-E
pip install -e .

# Verify installation
wal-e --version
```

### Step 2: Create a PAT Token (1 minute)

1. Log in to your Databricks workspace as a **workspace admin**
2. Click your username (top-right) → **Settings**
3. Go to **Developer** → **Access tokens**
4. Click **Generate New Token**
5. Description: `WAL-E Assessment - [Date]`
6. Lifetime: **1 day** (the assessment takes ~15 minutes)
7. Click **Generate**
8. **Copy the token** immediately (it won't be shown again)

> **Important:** The token inherits the permissions of the user who creates it. For a complete assessment, create the token from an account with **workspace admin** and **metastore admin** access.

### Step 3: Configure the Databricks CLI (1 minute)

```bash
# Set up a CLI profile for this assessment
databricks configure --profile wal-assessment \
  --host https://YOUR-WORKSPACE.cloud.databricks.com \
  --token

# When prompted, paste the PAT token you just created
```

### Step 4: Validate Access (30 seconds)

```bash
# Verify everything is connected
wal-e validate --profile wal-assessment
```

You should see a green checkmark. If you see errors, your SA will help troubleshoot (see Section 9).

### Step 5: Run the Assessment (5-10 minutes)

```bash
# Interactive mode (recommended — your SA walks you through each step)
wal-e assess --profile wal-assessment --interactive

# Or quick scan with all report formats
wal-e assess --profile wal-assessment --output ./my-assessment --format all
```

WAL-E will:
1. Auto-detect your cloud provider (AWS / Azure / GCP)
2. Run 21 read-only API calls to collect workspace metadata
3. Score 129 best practices across 7 pillars
4. Generate reports in the output directory

### Step 6: Review Results with Your SA

The output directory contains:

| File | Description |
|------|-------------|
| `WAL_Assessment_Readout.md` | Detailed report (all 7 pillars) |
| `WAL_Assessment_Scores.csv` | 129 best practices with scores |
| `WAL_Assessment_Presentation.pptx` | Executive deck |
| `WAL_Assessment_Presentation.html` | Browser presentation |
| `WAL_Assessment_Audit_Report.md` | Evidence trail of all API calls |

**Share with your SA** by:
- Screen sharing the HTML presentation, or
- Sending the output folder to your SA (at your discretion)

### Step 7: Clean Up (1 minute)

```bash
# 1. Revoke your PAT token immediately:
#    Workspace > Settings > Developer > Access tokens > Revoke

# 2. Remove the CLI profile:
#    Edit ~/.databrickscfg and delete the [wal-assessment] section

# 3. Delete local assessment files (after you've saved what you need):
rm -rf ./my-assessment
```

---

## 5. Required Permissions by Collector

WAL-E runs 6 collectors. Here is exactly what each one needs:

### Collector 1: Authentication & Identity

| API Call | Permission | Admin Required? |
|----------|-----------|-----------------|
| `databricks auth describe` | Any authenticated user | No |
| `databricks current-user me` | Any authenticated user | No |

### Collector 2: Governance (Unity Catalog)

| API Call | Permission | Admin Required? |
|----------|-----------|-----------------|
| `GET /api/2.1/unity-catalog/metastore_summary` | Metastore user | No (admin for full details) |
| `GET /api/2.1/unity-catalog/catalogs` | USE CATALOG or metastore admin | Metastore admin for ALL catalogs |
| `GET /api/2.1/unity-catalog/external-locations` | Metastore admin | Yes |
| `GET /api/2.1/unity-catalog/storage-credentials` | Metastore admin | Yes |

### Collector 3: Compute

| API Call | Permission | Admin Required? |
|----------|-----------|-----------------|
| `GET /api/2.1/clusters/list` | CAN_ATTACH_TO or admin | Admin for ALL clusters |
| `GET /api/2.0/sql/warehouses` | CAN_USE or admin | Admin for ALL warehouses |
| `GET /api/2.0/cluster-policies/list` | Any workspace user | No |
| `GET /api/2.0/instance-pools/list` | CAN_ATTACH_TO or admin | Admin for ALL pools |

### Collector 4: Security

| API Call | Permission | Admin Required? |
|----------|-----------|-----------------|
| `GET /api/2.0/workspace-conf` | Workspace admin | **Yes** |
| `GET /api/2.0/ip-access-lists` | Workspace admin | **Yes** |
| `GET /api/2.0/token/list` | Any authenticated user | No |

### Collector 5: Operations

| API Call | Permission | Admin Required? |
|----------|-----------|-----------------|
| `GET /api/2.1/jobs/list` | CAN_VIEW or admin | Admin for ALL jobs |
| `GET /api/2.0/pipelines` | CAN_VIEW or admin | Admin for ALL pipelines |
| `GET /api/2.0/serving-endpoints` | CAN_QUERY or admin | Admin for ALL endpoints |
| `GET /api/2.0/repos` | CAN_READ or admin | Admin for ALL repos |
| `GET /api/2.0/global-init-scripts` | Workspace admin | **Yes** |
| `GET /api/2.0/groups/list` | Any workspace user | No |
| `GET /api/2.0/secrets/list-scopes` | Any workspace user | No |

### Collector 6: Workspace Structure

| API Call | Permission | Admin Required? |
|----------|-----------|-----------------|
| `GET /api/2.0/workspace/list?path=%2F` | CAN_READ or admin | Admin for complete listing |

---

## 6. Complete API Endpoint Reference

**All calls are GET (read-only). Total: 21 calls. Zero write calls.**

```
# Authentication (2 calls)
databricks auth describe
databricks current-user me

# Unity Catalog Governance (4 calls)
GET /api/2.1/unity-catalog/metastore_summary
GET /api/2.1/unity-catalog/catalogs
GET /api/2.1/unity-catalog/external-locations
GET /api/2.1/unity-catalog/storage-credentials

# Compute Infrastructure (4 calls)
GET /api/2.1/clusters/list
GET /api/2.0/sql/warehouses
GET /api/2.0/cluster-policies/list
GET /api/2.0/instance-pools/list

# Security Configuration (3 calls)
GET /api/2.0/workspace-conf?keys=enableResultsDownloading,enableDbfsFileBrowser,...
GET /api/2.0/ip-access-lists
GET /api/2.0/token/list

# Operations (7 calls)
GET /api/2.1/jobs/list
GET /api/2.0/pipelines
GET /api/2.0/serving-endpoints
GET /api/2.0/repos
GET /api/2.0/global-init-scripts
GET /api/2.0/groups/list
GET /api/2.0/secrets/list-scopes

# Workspace Structure (1 call)
GET /api/2.0/workspace/list?path=%2F
```

---

## 7. Optional: System Tables for Deep Assessment

For a deeper assessment (cost analysis, query performance, audit trail), you can optionally grant access to [Databricks System Tables](https://docs.databricks.com/en/administration-guide/system-tables/index.html).

```sql
-- Run these as an account admin in a SQL warehouse
GRANT SELECT ON SCHEMA system.billing TO `your-admin-user@company.com`;
GRANT SELECT ON SCHEMA system.compute TO `your-admin-user@company.com`;
GRANT SELECT ON SCHEMA system.query TO `your-admin-user@company.com`;
GRANT SELECT ON SCHEMA system.access TO `your-admin-user@company.com`;
```

**Note:** System table access is **optional**. WAL-E produces a complete assessment using only the 21 REST API calls above.

---

## 8. Security & Privacy Assurances

### Data Handling

| Concern | Assurance |
|---------|-----------|
| **Who runs it?** | **You** (the customer) run WAL-E on your own machine |
| **Token sharing?** | **None** — your token stays on your machine in `~/.databrickscfg` |
| **Where do results go?** | **Your machine only** — nothing is transmitted externally |
| **What does the SA see?** | Only what **you choose to share** (e.g., screen share, sending the report) |
| **Data in transit** | All API calls use HTTPS/TLS encryption to your own workspace |
| **PII exposure** | Only workspace user emails and group names (for governance scoring) |

### What WAL-E Will NEVER Do

- **Never** read table data, row content, or query results
- **Never** execute any notebook or job
- **Never** create, modify, or delete any workspace resource
- **Never** start or stop any cluster or warehouse
- **Never** modify any configuration or setting
- **Never** access secret values (only scope names)
- **Never** transmit data to any external service or third party
- **Never** send your token or credentials anywhere

### Audit Trail

WAL-E generates a `WAL_Assessment_Audit_Report.md` documenting:
- Every API call made (command, endpoint, timestamp, duration)
- Every piece of raw output received
- What evidence was used for which finding

You can review this audit report with your security team before sharing any results.

---

## 9. Troubleshooting

| Symptom | Cause | Solution |
|---------|-------|----------|
| `401 Unauthorized` | Token expired or invalid | Regenerate your PAT token |
| `403 Forbidden` on workspace-conf | User is not workspace admin | Create token from a workspace admin account |
| `403 Forbidden` on catalogs | User lacks metastore admin | Use a metastore admin account |
| Empty cluster list | User lacks CAN_ATTACH_TO | Use an admin account |
| `Connection refused` | Network/firewall blocking | Verify your workspace URL; check VPN/proxy |
| `SSL certificate error` | Corporate proxy intercepting | Set `REQUESTS_CA_BUNDLE` or ask your IT team |

### Permissions by Coverage

| Access Level | Coverage |
|-------------|:--------:|
| Regular user | ~40% of best practices |
| **Workspace admin** | **~80%** |
| **Workspace admin + Metastore admin** | **~95%** |
| Above + System tables | **100%** |

---

## 10. For SAs: Guiding the Customer

### Pre-Call Checklist (Send to Customer)

Send this to the customer **before** the assessment call:

```
Hi [Customer Name],

We'll be running a Well-Architected Lakehouse assessment on your workspace.
You'll run everything on YOUR machine — I'll guide you through it on the call.

Before the call, please:
  1. Install Python 3.10+ (python.org)
  2. Install Databricks CLI: pip install databricks-cli
  3. Install GitHub CLI: brew install gh (or cli.github.com)
  4. Have your Databricks workspace URL ready
  5. Have workspace admin access (and metastore admin if possible)

On the call, I'll guide you through:
  - Installing WAL-E (2 min)
  - Creating a temporary token (1 min)
  - Running the assessment (10 min)
  - Reviewing the results together

The tool makes 21 read-only API calls and generates a report.
No data leaves your machine. You revoke the token right after.

Total time: ~30 minutes
```

### During the Call

1. Share your screen (SA) and guide the customer through the README Quick Start
2. Have the customer screen share when running `wal-e assess --interactive`
3. Walk through the output together
4. Ensure the customer revokes their token at the end

### After the Call

- Ask the customer to share the output folder or specific reports at their discretion
- Prepare the readout presentation from the PPTX
- Schedule a follow-up to discuss remediation priorities

---

*Document version: 2.0 | WAL-E v0.1.0 | Customer self-service model | Last updated: February 2026*
