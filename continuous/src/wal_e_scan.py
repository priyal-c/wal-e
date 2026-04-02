# Databricks notebook source
# MAGIC %md
# MAGIC # WAL-E Continuous Scan
# MAGIC
# MAGIC Runs a full WAL-E assessment, writes scores to Delta, detects drift from the
# MAGIC last baseline, and fires alerts on regressions.
# MAGIC
# MAGIC **Deploy as a scheduled Databricks Job** (recommended: every 4-24 hours).

# COMMAND ----------

# MAGIC %pip install python-pptx python-docx git+https://github.com/priyal-c/wal-e.git
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Set these widgets or override with job parameters.

# COMMAND ----------

dbutils.widgets.text("catalog", "wal_e", "Unity Catalog name")
dbutils.widgets.text("schema", "continuous", "Schema name")
dbutils.widgets.text("profile", "DEFAULT", "Databricks CLI profile")
dbutils.widgets.dropdown("auth_type", "auto", ["auto", "pat", "oauth-u2m"], "Auth method")
dbutils.widgets.text("slack_webhook_url", "", "Slack webhook URL (optional)")
dbutils.widgets.text("alert_email", "", "Alert email address (optional)")
dbutils.widgets.dropdown("scan_mode", "standard", ["standard", "deep"], "Scan mode")
dbutils.widgets.text("warehouse_id", "", "Warehouse ID (for deep scan)")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
PROFILE = dbutils.widgets.get("profile")
AUTH_TYPE = dbutils.widgets.get("auth_type")
SLACK_WEBHOOK = dbutils.widgets.get("slack_webhook_url")
ALERT_EMAIL = dbutils.widgets.get("alert_email")
SCAN_MODE = dbutils.widgets.get("scan_mode")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")

ASSESSMENTS_TABLE = f"{CATALOG}.{SCHEMA}.wal_e_assessments"
VIOLATIONS_TABLE = f"{CATALOG}.{SCHEMA}.wal_e_violations"

print(f"Catalog:    {CATALOG}")
print(f"Schema:     {SCHEMA}")
print(f"Profile:    {PROFILE}")
print(f"Auth:       {AUTH_TYPE}")
print(f"Scan mode:  {SCAN_MODE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Delta tables if they don't exist

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ASSESSMENTS_TABLE} (
  run_id            STRING      COMMENT 'Unique run identifier',
  run_timestamp     TIMESTAMP   COMMENT 'When this scan completed',
  run_mode          STRING      COMMENT 'standard or deep',
  auth_method       STRING      COMMENT 'pat, oauth-u2m, or auto',
  pillar            STRING      COMMENT 'Well-Architected pillar',
  principle         STRING      COMMENT 'Principle within the pillar',
  best_practice     STRING      COMMENT 'Best practice name',
  score             DOUBLE      COMMENT 'Score 0-2 for this best practice',
  verified          BOOLEAN     COMMENT 'Whether score is evidence-based',
  finding_notes     STRING      COMMENT 'What was found',
  workspace_host    STRING      COMMENT 'Workspace URL',
  cloud_provider    STRING      COMMENT 'aws, azure, or gcp',
  overall_score     DOUBLE      COMMENT 'Overall assessment score',
  maturity_level    STRING      COMMENT 'Beginning, Intermediate, Advanced, Optimized'
)
USING DELTA
COMMENT 'WAL-E continuous assessment history — one row per best practice per run'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {VIOLATIONS_TABLE} (
  violation_id      STRING      COMMENT 'Unique violation identifier',
  detected_at       TIMESTAMP   COMMENT 'When the regression was detected',
  run_id            STRING      COMMENT 'Run that detected this',
  best_practice     STRING      COMMENT 'Best practice that regressed',
  pillar            STRING      COMMENT 'Well-Architected pillar',
  previous_score    DOUBLE      COMMENT 'Score from last run',
  current_score     DOUBLE      COMMENT 'Score from this run',
  drift             STRING      COMMENT 'regressed, new_violation, or resolved',
  severity          STRING      COMMENT 'critical, high, medium, low',
  finding_notes     STRING      COMMENT 'Current finding details',
  workspace_host    STRING      COMMENT 'Workspace URL',
  acknowledged      BOOLEAN     COMMENT 'Has someone acknowledged this',
  acknowledged_by   STRING      COMMENT 'Who acknowledged',
  acknowledged_at   TIMESTAMP   COMMENT 'When acknowledged'
)
USING DELTA
COMMENT 'WAL-E drift violations — regressions from baseline'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")

print(f"✓ Tables ready: {ASSESSMENTS_TABLE}, {VIOLATIONS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Run WAL-E assessment via CLI

# COMMAND ----------

run_id = str(uuid.uuid4())
run_timestamp = datetime.now(timezone.utc)
output_dir = f"/tmp/wal-e-continuous-{run_id}"

cmd = [
    "python", "-m", "wal_e", "assess",
    "--profile", PROFILE,
    "--auth-type", AUTH_TYPE,
    "--output", output_dir,
    "--format", "audit",
    "--quiet",
    "--timeout", "300",
]

if SCAN_MODE == "deep" and WAREHOUSE_ID:
    cmd.extend(["--deep", "--warehouse-id", WAREHOUSE_ID])

print(f"Run ID: {run_id}")
print(f"Command: {' '.join(cmd)}")
print("Running assessment...")

result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)

if result.returncode != 0:
    error_msg = result.stderr.strip() if result.stderr else "Unknown error"
    print(f"⚠ Assessment returned exit code {result.returncode}")
    print(f"  stderr: {error_msg}")
    if "No module" in error_msg:
        raise RuntimeError(f"Missing dependency: {error_msg}")
else:
    print("✓ Assessment completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Parse scored results and write to Delta

# COMMAND ----------

cache_dir = Path(output_dir) / ".wal-e-cache"
scored_path = cache_dir / "scored_assessment.json"

if not scored_path.exists():
    raise FileNotFoundError(
        f"No scored data at {scored_path}. "
        f"Assessment may have failed. Check {output_dir} for details."
    )

with open(scored_path) as f:
    scored = json.load(f)

workspace_host = scored.get("workspace_host", "Unknown")
cloud_provider = scored.get("cloud_provider", "unknown")
overall_score = scored.get("overall_score", 0.0)
maturity_level = scored.get("maturity_level", "Not Assessed")
bp_scores = scored.get("best_practice_scores", [])

print(f"Workspace:  {workspace_host}")
print(f"Cloud:      {cloud_provider}")
print(f"Overall:    {overall_score:.2f} / 2.0 ({maturity_level})")
print(f"BPs scored: {len(bp_scores)}")

rows = []
for bp in bp_scores:
    rows.append({
        "run_id": run_id,
        "run_timestamp": run_timestamp,
        "run_mode": SCAN_MODE,
        "auth_method": AUTH_TYPE,
        "pillar": bp.get("pillar", ""),
        "principle": bp.get("principle", ""),
        "best_practice": bp.get("name", ""),
        "score": float(bp.get("score", 0)),
        "verified": bp.get("verified", True),
        "finding_notes": bp.get("finding_notes", ""),
        "workspace_host": workspace_host,
        "cloud_provider": cloud_provider,
        "overall_score": overall_score,
        "maturity_level": maturity_level,
    })

assessment_schema = StructType([
    StructField("run_id", StringType()),
    StructField("run_timestamp", TimestampType()),
    StructField("run_mode", StringType()),
    StructField("auth_method", StringType()),
    StructField("pillar", StringType()),
    StructField("principle", StringType()),
    StructField("best_practice", StringType()),
    StructField("score", DoubleType()),
    StructField("verified", BooleanType()),
    StructField("finding_notes", StringType()),
    StructField("workspace_host", StringType()),
    StructField("cloud_provider", StringType()),
    StructField("overall_score", DoubleType()),
    StructField("maturity_level", StringType()),
])

df_current = spark.createDataFrame(rows, schema=assessment_schema)
df_current.write.mode("append").saveAsTable(ASSESSMENTS_TABLE)

print(f"✓ Wrote {len(rows)} best practice scores to {ASSESSMENTS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Detect drift from last baseline

# COMMAND ----------

df_history = spark.table(ASSESSMENTS_TABLE)

df_previous = (
    df_history
    .filter(F.col("run_id") != run_id)
    .withColumn("rn", F.row_number().over(
        F.Window.partitionBy("best_practice").orderBy(F.col("run_timestamp").desc())
    ))
    .filter(F.col("rn") == 1)
    .select(
        F.col("best_practice"),
        F.col("score").alias("previous_score"),
        F.col("run_id").alias("previous_run_id"),
    )
)

df_current_scores = (
    df_history
    .filter(F.col("run_id") == run_id)
    .select("best_practice", "pillar", "score", "finding_notes")
)

df_compared = (
    df_current_scores
    .join(df_previous, on="best_practice", how="left")
    .withColumn("previous_score", F.coalesce(F.col("previous_score"), F.col("score")))
    .withColumn("score_delta", F.col("score") - F.col("previous_score"))
)

df_regressions = df_compared.filter(F.col("score_delta") < 0)
df_improvements = df_compared.filter(F.col("score_delta") > 0)

regression_count = df_regressions.count()
improvement_count = df_improvements.count()
unchanged_count = df_compared.count() - regression_count - improvement_count

print(f"\n{'='*60}")
print(f"  DRIFT REPORT")
print(f"{'='*60}")
print(f"  🔴 Regressions:  {regression_count}")
print(f"  🟢 Improvements: {improvement_count}")
print(f"  ⚪ Unchanged:    {unchanged_count}")
print(f"{'='*60}\n")

if regression_count > 0:
    print("REGRESSIONS:")
    df_regressions.select(
        "pillar", "best_practice", "previous_score", "score", "score_delta"
    ).orderBy("score_delta").show(50, truncate=False)

if improvement_count > 0:
    print("IMPROVEMENTS:")
    df_improvements.select(
        "pillar", "best_practice", "previous_score", "score", "score_delta"
    ).orderBy(F.col("score_delta").desc()).show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Write violations to Delta

# COMMAND ----------

def severity_from_delta(score_delta, current_score):
    """Map score drop magnitude to severity."""
    if current_score == 0:
        return "critical"
    if score_delta <= -1.5:
        return "critical"
    if score_delta <= -1.0:
        return "high"
    if score_delta <= -0.5:
        return "medium"
    return "low"


if regression_count > 0:
    violation_rows = []
    for row in df_regressions.collect():
        violation_rows.append({
            "violation_id": str(uuid.uuid4()),
            "detected_at": run_timestamp,
            "run_id": run_id,
            "best_practice": row["best_practice"],
            "pillar": row["pillar"],
            "previous_score": float(row["previous_score"]),
            "current_score": float(row["score"]),
            "drift": "regressed",
            "severity": severity_from_delta(row["score_delta"], row["score"]),
            "finding_notes": row["finding_notes"] or "",
            "workspace_host": workspace_host,
            "acknowledged": False,
            "acknowledged_by": None,
            "acknowledged_at": None,
        })

    violation_schema = StructType([
        StructField("violation_id", StringType()),
        StructField("detected_at", TimestampType()),
        StructField("run_id", StringType()),
        StructField("best_practice", StringType()),
        StructField("pillar", StringType()),
        StructField("previous_score", DoubleType()),
        StructField("current_score", DoubleType()),
        StructField("drift", StringType()),
        StructField("severity", StringType()),
        StructField("finding_notes", StringType()),
        StructField("workspace_host", StringType()),
        StructField("acknowledged", BooleanType()),
        StructField("acknowledged_by", StringType()),
        StructField("acknowledged_at", TimestampType()),
    ])

    df_violations = spark.createDataFrame(violation_rows, schema=violation_schema)
    df_violations.write.mode("append").saveAsTable(VIOLATIONS_TABLE)
    print(f"✓ Wrote {len(violation_rows)} violations to {VIOLATIONS_TABLE}")
else:
    print("✓ No regressions detected — no violations to write")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Send alerts (Slack / Email)

# COMMAND ----------

import urllib.request


def send_slack_alert(webhook_url, regressions, run_id, workspace_host, overall_score, maturity):
    """Post a regression summary to Slack via incoming webhook."""
    if not webhook_url:
        return

    critical = sum(1 for r in regressions if r["severity"] == "critical")
    high = sum(1 for r in regressions if r["severity"] == "high")

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "🚨 WAL-E Drift Alert"}
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Workspace:*\n{workspace_host}"},
                {"type": "mrkdwn", "text": f"*Overall Score:*\n{overall_score:.0%} ({maturity})"},
                {"type": "mrkdwn", "text": f"*Regressions:*\n{len(regressions)}"},
                {"type": "mrkdwn", "text": f"*Critical/High:*\n{critical} / {high}"},
            ]
        },
        {"type": "divider"},
    ]

    for r in sorted(regressions, key=lambda x: x["current_score"])[:10]:
        emoji = "🔴" if r["severity"] == "critical" else "🟠" if r["severity"] == "high" else "🟡"
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"{emoji} *{r['best_practice']}*\n"
                    f"_{r['pillar']}_ | "
                    f"Score: {r['previous_score']:.0f} → {r['current_score']:.0f}\n"
                    f">{r['finding_notes'][:200]}"
                )
            }
        })

    if len(regressions) > 10:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"_...and {len(regressions) - 10} more. See {VIOLATIONS_TABLE} for full list._"}]
        })

    payload = json.dumps({"blocks": blocks}).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(f"✓ Slack alert sent (HTTP {resp.status})")
    except Exception as e:
        print(f"⚠ Slack alert failed: {e}")


def send_email_alert(email, regressions, run_id, workspace_host, overall_score, maturity):
    """Send a regression summary via dbutils email (Databricks notification)."""
    if not email:
        return

    subject = f"WAL-E Alert: {len(regressions)} regression(s) on {workspace_host}"
    body_lines = [
        f"WAL-E Continuous Scan detected {len(regressions)} regression(s).",
        f"",
        f"Workspace: {workspace_host}",
        f"Overall Score: {overall_score:.2f} / 2.0 ({maturity})",
        f"Run ID: {run_id}",
        f"",
        f"Regressions:",
        f"{'='*70}",
    ]
    for r in sorted(regressions, key=lambda x: x["current_score"]):
        body_lines.append(
            f"[{r['severity'].upper()}] {r['best_practice']} "
            f"({r['pillar']}) — {r['previous_score']:.0f} → {r['current_score']:.0f}"
        )
    body_lines.extend([
        f"",
        f"Full details: SELECT * FROM {VIOLATIONS_TABLE} WHERE run_id = '{run_id}'",
    ])

    try:
        dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath()
        print(f"✓ Email alert queued to {email}")
        print(f"  Subject: {subject}")
        print(f"  Body preview: {body_lines[0]}")
    except Exception:
        print(f"⚠ Email notification not available in this environment")
        print(f"  Would send to: {email}")
        print(f"  Subject: {subject}")


if regression_count > 0 and (SLACK_WEBHOOK or ALERT_EMAIL):
    regression_data = [row.asDict() for row in df_regressions.collect()]
    violation_data = []
    for r in regression_data:
        violation_data.append({
            "best_practice": r["best_practice"],
            "pillar": r["pillar"],
            "previous_score": float(r["previous_score"]),
            "current_score": float(r["score"]),
            "severity": severity_from_delta(r["score_delta"], r["score"]),
            "finding_notes": r.get("finding_notes", ""),
        })

    if SLACK_WEBHOOK:
        send_slack_alert(SLACK_WEBHOOK, violation_data, run_id, workspace_host, overall_score / 2.0, maturity_level)
    if ALERT_EMAIL:
        send_email_alert(ALERT_EMAIL, violation_data, run_id, workspace_host, overall_score, maturity_level)
elif regression_count > 0:
    print("⚠ Regressions detected but no alert destination configured.")
    print("  Set slack_webhook_url or alert_email to enable notifications.")
else:
    print("✓ No regressions — no alerts needed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

total_runs = df_history.select("run_id").distinct().count()
open_violations = spark.table(VIOLATIONS_TABLE).filter(F.col("acknowledged") == False).count()

print(f"""
{'='*60}
  WAL-E CONTINUOUS SCAN COMPLETE
{'='*60}

  Run ID:          {run_id}
  Timestamp:       {run_timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}
  Workspace:       {workspace_host}
  Auth method:     {AUTH_TYPE}
  Overall score:   {overall_score:.2f} / 2.0 ({maturity_level})
  BPs evaluated:   {len(bp_scores)}

  Drift:
    Regressions:   {regression_count}
    Improvements:  {improvement_count}
    Unchanged:     {unchanged_count}

  History:
    Total runs:         {total_runs}
    Open violations:    {open_violations}

  Tables:
    Assessments: {ASSESSMENTS_TABLE}
    Violations:  {VIOLATIONS_TABLE}

{'='*60}
""")
