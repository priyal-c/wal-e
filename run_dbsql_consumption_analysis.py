#!/usr/bin/env python3
"""
DBSQL Consumption Analysis Script

Analyzes DBSQL consumption patterns by consumer type (BI tools, Databricks Apps, etc.)
over the last 6 months using system.query.history table.

Usage:
    python run_dbsql_consumption_analysis.py --profile <profile-name> --warehouse-id <warehouse-id>

Example:
    python run_dbsql_consumption_analysis.py --profile wal-assessment --warehouse-id abc123def456
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path


def run_sql_query(sql: str, warehouse_id: str, profile_name: str) -> dict:
    """Execute SQL query via Databricks statement execution API."""
    payload = json.dumps({
        "warehouse_id": warehouse_id,
        "statement": sql,
        "wait_timeout": "120s",
        "disposition": "INLINE",
        "format": "JSON_ARRAY",
    })

    cmd = [
        "databricks", "api", "post",
        "/api/2.0/sql/statements",
        "--json", payload,
        "--profile", profile_name,
    ]

    try:
        print(f"🔄 Executing DBSQL consumption analysis query...")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=180)

        if result.returncode != 0:
            print(f"❌ Error executing query: {result.stderr}", file=sys.stderr)
            sys.exit(1)

        response = json.loads(result.stdout)
        return response

    except subprocess.TimeoutExpired:
        print("❌ Query timed out after 180 seconds", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Failed to parse response: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


def parse_results(response: dict) -> list[dict]:
    """Parse query results from API response."""
    try:
        status = response.get("status", {}).get("state")
        if status != "SUCCEEDED":
            error_msg = response.get("status", {}).get("error", {}).get("message", "Unknown error")
            print(f"❌ Query failed: {error_msg}", file=sys.stderr)
            sys.exit(1)

        manifest = response.get("manifest", {})
        schema = manifest.get("schema", {}).get("columns", [])
        data_array = response.get("result", {}).get("data_array", [])

        if not data_array:
            print("⚠️  No data returned from query", file=sys.stderr)
            return []

        # Parse column names
        column_names = [col["name"] for col in schema]

        # Convert rows to dictionaries
        rows = []
        for row in data_array:
            row_dict = dict(zip(column_names, row))
            rows.append(row_dict)

        return rows

    except Exception as e:
        print(f"❌ Failed to parse results: {e}", file=sys.stderr)
        sys.exit(1)


def display_results(rows: list[dict]):
    """Display results in a formatted table."""
    if not rows:
        print("\n📊 No DBSQL consumption data found for the last 6 months.\n")
        return

    print("\n" + "="*120)
    print("📊 DBSQL CONSUMPTION ANALYSIS - LAST 6 MONTHS")
    print("="*120)
    print()

    # Calculate totals
    total_queries = sum(int(row.get("total_queries", 0)) for row in rows)
    total_compute_hours = sum(float(row.get("total_compute_hours", 0)) for row in rows)

    # Print summary table
    print(f"{'Consumer Type':<35} {'Queries':>10} {'Query %':>10} {'Compute Hrs':>15} {'Consumption %':>15} {'Avg Time (s)':>15} {'Failures':>10}")
    print("-"*120)

    for row in rows:
        consumer_type = row.get("consumer_type", "Unknown")[:35]
        queries = int(row.get("total_queries", 0))
        query_pct = float(row.get("pct_of_total_queries", 0))
        compute_hrs = float(row.get("total_compute_hours", 0))
        consumption_pct = float(row.get("pct_of_total_consumption", 0))
        avg_time = float(row.get("avg_query_time_sec", 0))
        failures = int(row.get("total_failed", 0))

        print(f"{consumer_type:<35} {queries:>10,} {query_pct:>9.2f}% {compute_hrs:>15,.2f} {consumption_pct:>14.2f}% {avg_time:>15,.2f} {failures:>10,}")

    print("-"*120)
    print(f"{'TOTAL':<35} {total_queries:>10,} {'100.00%':>10} {total_compute_hours:>15,.2f} {'100.00%':>15}")
    print("="*120)
    print()

    # Categorize by consumer type
    bi_tools = [r for r in rows if r.get("consumer_type", "").startswith("BI -")]
    databricks_native = [r for r in rows if r.get("consumer_type", "").startswith("Databricks")]
    programmatic = [r for r in rows if any(x in r.get("consumer_type", "") for x in ["Python", "JDBC", "ODBC"])]

    print("📈 SUMMARY BY CATEGORY:")
    print()

    if bi_tools:
        bi_consumption = sum(float(r.get("total_compute_hours", 0)) for r in bi_tools)
        bi_pct = (bi_consumption / total_compute_hours * 100) if total_compute_hours > 0 else 0
        print(f"  🎨 BI Tools:              {bi_pct:>6.2f}% ({len(bi_tools)} tool(s))")

    if databricks_native:
        db_consumption = sum(float(r.get("total_compute_hours", 0)) for r in databricks_native)
        db_pct = (db_consumption / total_compute_hours * 100) if total_compute_hours > 0 else 0
        print(f"  🟢 Databricks Native:     {db_pct:>6.2f}% ({len(databricks_native)} source(s))")

    if programmatic:
        prog_consumption = sum(float(r.get("total_compute_hours", 0)) for r in programmatic)
        prog_pct = (prog_consumption / total_compute_hours * 100) if total_compute_hours > 0 else 0
        print(f"  💻 Programmatic Access:   {prog_pct:>6.2f}% ({len(programmatic)} client(s))")

    print()
    print("="*120)
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Analyze DBSQL consumption by consumer type over the last 6 months"
    )
    parser.add_argument(
        "--profile",
        required=True,
        help="Databricks CLI profile name (e.g., wal-assessment)"
    )
    parser.add_argument(
        "--warehouse-id",
        required=True,
        help="SQL Warehouse ID to execute the query on"
    )
    parser.add_argument(
        "--output",
        help="Optional: Save results to JSON file"
    )

    args = parser.parse_args()

    # Read SQL query from file
    sql_file = Path(__file__).parent / "dbsql_consumption_analysis.sql"
    if not sql_file.exists():
        print(f"❌ SQL file not found: {sql_file}", file=sys.stderr)
        sys.exit(1)

    with open(sql_file, "r") as f:
        sql_query = f.read()

    # Extract main query (ignore commented sections)
    lines = [line for line in sql_query.split("\n") if not line.strip().startswith("/*")]
    sql_query = "\n".join(lines)

    # Execute query
    response = run_sql_query(sql_query, args.warehouse_id, args.profile)

    # Parse and display results
    rows = parse_results(response)
    display_results(rows)

    # Save to file if requested
    if args.output:
        output_path = Path(args.output)
        with open(output_path, "w") as f:
            json.dump(rows, f, indent=2)
        print(f"💾 Results saved to: {output_path}")
        print()


if __name__ == "__main__":
    main()
