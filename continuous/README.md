# WAL-E Continuous Mode

Deploy WAL-E as an always-running guardian inside your Databricks workspace.
Instead of a one-time scan, WAL-E runs on a schedule, stores every assessment
in Delta tables, detects drift from your baseline, and alerts you when best
practices regress.

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│  Databricks Workflow Job (scheduled every N hours)       │
│                                                          │
│  ┌──────────┐   ┌──────────┐   ┌──────────────────┐    │
│  │ Collect   │ → │ Score    │ → │ Compare to last  │    │
│  │ (21 APIs) │   │ (129 BPs)│   │ baseline in Delta│    │
│  └──────────┘   └──────────┘   └────────┬─────────┘    │
│                                          │               │
│                    ┌─────────────────────┼──────────┐    │
│                    │                     │          │    │
│               ┌────▼─────┐    ┌─────────▼────┐     │    │
│               │wal_e_    │    │wal_e_        │     │    │
│               │assessments│   │violations    │     │    │
│               │(Delta)   │    │(Delta)       │     │    │
│               └──────────┘    └──────┬───────┘     │    │
│                                      │             │    │
│                              ┌───────▼──────┐      │    │
│                              │ Slack / Email │      │    │
│                              │ Alert         │      │    │
│                              └──────────────┘      │    │
│                                                     │    │
│               ┌─────────────────────────────────┐   │    │
│               │  Lakeview Dashboard             │   │    │
│               │  (score trends, violations,     │   │    │
│               │   pillar heatmap, run history)  │   │    │
│               └─────────────────────────────────┘   │    │
└──────────────────────────────────────────────────────────┘
```

## Quick Start

### 1. Deploy with Databricks Asset Bundles

```bash
cd continuous/

# Dev mode (your user namespace)
databricks bundle deploy -t dev

# Production (shared workspace)
databricks bundle deploy -t production \
  --var="catalog=wal_e" \
  --var="schema=continuous" \
  --var="slack_webhook_url=https://hooks.slack.com/services/..."
```

### 2. Run manually to test

```bash
# Trigger the scan job
databricks bundle run wal_e_continuous_scan -t dev
```

### 3. Check results

```sql
-- Latest scores
SELECT * FROM wal_e.continuous.wal_e_assessments
WHERE run_id = (SELECT run_id FROM wal_e.continuous.wal_e_assessments ORDER BY run_timestamp DESC LIMIT 1);

-- Open violations
SELECT * FROM wal_e.continuous.wal_e_violations
WHERE acknowledged = FALSE
ORDER BY severity, detected_at DESC;
```

## Configuration

| Parameter           | Default       | Description                                       |
| ------------------- | ------------- | ------------------------------------------------- |
| `catalog`           | `wal_e`       | Unity Catalog for WAL-E tables                    |
| `schema`            | `continuous`  | Schema within the catalog                         |
| `profile`           | `DEFAULT`     | Databricks CLI profile                            |
| `auth_type`         | `auto`        | `auto`, `pat`, or `oauth-u2m`                     |
| `slack_webhook_url` | _(empty)_     | Slack incoming webhook for alerts                 |
| `alert_email`       | _(empty)_     | Email for alert notifications                     |
| `scan_schedule`     | `0 0 8 * * ?` | Quartz cron (default: daily 8am UTC)              |
| `scan_mode`         | `standard`    | `standard` (API-only) or `deep` (+ system tables) |
| `warehouse_id`      | _(empty)_     | SQL warehouse for deep scan                       |

## Schedule Examples

| Frequency             | Quartz Cron         |
| --------------------- | ------------------- |
| Every 4 hours         | `0 0 */4 * * ?`     |
| Daily at 8am UTC      | `0 0 8 * * ?`       |
| Weekdays at 9am UTC   | `0 0 9 ? * MON-FRI` |
| Weekly Monday 6am UTC | `0 0 6 ? * MON`     |

## Delta Tables

### `wal_e_assessments`

One row per best practice per scan run. Full history of every score.

### `wal_e_violations`

Drift events — written only when a best practice score drops between runs.
Includes severity (critical/high/medium/low) and acknowledgment tracking.

## Dashboard

Import `src/dashboard_queries.sql` into a Lakeview dashboard for:

- Overall score trend over time
- Pillar-by-pillar health heatmap
- Open violations with severity
- Best practice drill-down
- Run history and coverage stats

## Authentication

Supports the same auth methods as WAL-E CLI:

- **OAuth U2M** — browser-based SSO, short-lived tokens
- **PAT** — personal access token from workspace settings
- **Auto** — detects from your CLI profile
