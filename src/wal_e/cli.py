"""
WAL-E CLI - Well-Architected Lakehouse Evaluator

Command-line interface for running assessments, validation, and report generation.
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

# ANSI color codes
class C:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    WHITE = "\033[97m"


WAL_E_BANNER = """
\x1b[96m __        __    _       _____
 \\ \\      / /   / \\     | ____|
  \\ \\ /\\ / /   / _ \\    | |__
   \\ V  V /   / ___ \\   |  __|
    \\_/\\_/   /_/   \\_\\  |_|____
\x1b[0m    \x1b[2mWell-Architected Lakehouse Evaluator\x1b[0m
"""


def _print_banner(quiet: bool = False) -> None:
    if not quiet:
        print(WAL_E_BANNER)


def _progress_spinner(quiet: bool, stop_event: list) -> None:
    chars = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
    i = 0
    while not stop_event[0]:
        if not quiet:
            print(f"\r{C.CYAN}{chars[i % len(chars)]}{C.RESET} Collecting...", end="", flush=True)
        i += 1
        time.sleep(0.08)


def _print_summary_table(pillar_scores: dict[str, float], overall: float, maturity: str, quiet: bool) -> None:
    if quiet:
        return
    from wal_e.reporters.base import PILLAR_DISPLAY_NAMES, PILLAR_ORDER
    print(f"\n\n{C.BOLD}{C.GREEN}✓ Assessment Complete{C.RESET}\n")
    print(f"{C.BOLD}Pillar Scores{C.RESET}")
    print("─" * 58)
    for pillar in PILLAR_ORDER:
        score = pillar_scores.get(pillar, 0)
        pct = (score / 2.0) * 100 if score is not None else 0
        display = PILLAR_DISPLAY_NAMES.get(pillar, pillar)
        bar_len = 20
        filled = int(bar_len * pct / 100)
        bar = f"{C.GREEN}█{C.RESET}" * filled + f"{C.DIM}░{C.RESET}" * (bar_len - filled)
        print(f"  {display[:40]:<40} {bar} {pct:.0f}%")
    print("─" * 58)
    overall_pct = (overall / 2.0) * 100
    print(f"  {C.BOLD}Overall{C.RESET}{'':<35} {overall_pct:.0f}%  ({maturity})")
    print()


def _convert_audit_entries(raw_responses: dict[str, list]) -> list[dict]:
    from wal_e.collectors.base import AuditEntry as CollectorAuditEntry

    entries: list[dict] = []
    for _collector_name, audit_list in raw_responses.items():
        for ae in audit_list:
            if hasattr(ae, "command"):
                cmd_str = " ".join(ae.command) if isinstance(ae.command, list) else str(ae.command)
                entries.append({
                    "command": cmd_str,
                    "output": getattr(ae, "raw_output", ""),
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "duration": getattr(ae, "duration_seconds", 0),
                })
            elif isinstance(ae, dict):
                entries.append({
                    "command": ae.get("command", ""),
                    "output": ae.get("output", ae.get("raw_output", "")),
                    "timestamp": ae.get("timestamp", ""),
                    "duration": ae.get("duration", ae.get("duration_seconds")),
                })
    return entries


def _scored_to_reporter_format(scored: Any) -> dict:
    from wal_e.framework.scoring import ScoredBestPractice

    pillar_scores = dict(scored.pillar_scores) if scored.pillar_scores else {}

    best_practice_scores = []
    for bp in scored.best_practice_scores:
        if hasattr(bp, "name"):
            best_practice_scores.append({"name": bp.name, "pillar": bp.pillar, "principle": bp.principle, "score": float(bp.score), "finding_notes": bp.finding_notes})
        elif isinstance(bp, dict):
            best_practice_scores.append(bp)

    return {
        "pillar_scores": pillar_scores,
        "best_practice_scores": best_practice_scores,
        "overall_score": scored.overall_score,
        "maturity_level": scored.maturity_level,
        "assessment_date": scored.assessment_date,
        "workspace_host": scored.workspace_host or "Unknown",
        "cloud_provider": getattr(scored, "cloud_provider", "") or "unknown",
    }


def _save_cached_assessment(out_path: Path, result: Any, scored: Any) -> None:
    cache_dir = out_path / ".wal-e-cache"
    cache_dir.mkdir(exist_ok=True)
    with open(cache_dir / "collected_data.json", "w") as f:
        json.dump(result.collected_data, f, default=str, indent=2)
    audit_flat = _convert_audit_entries(result.raw_responses)
    with open(cache_dir / "audit_entries.json", "w") as f:
        json.dump(audit_flat, f, indent=2)
    with open(cache_dir / "scored_assessment.json", "w") as f:
        json.dump(asdict(scored), f, indent=2)


def _run_assess(args: argparse.Namespace) -> int:
    from wal_e.core.config import WalEConfig
    from wal_e.core.engine import AssessmentEngine
    from wal_e.framework.scoring import ScoringEngine
    from wal_e.reporters import AuditLogReporter, CSVReporter, HTMLDeckReporter, MarkdownReporter, PPTXDeckReporter

    config = WalEConfig(profile_name=args.profile, output_dir=args.output, formats=args.format or ["md", "csv", "html", "pptx", "audit"])
    engine = AssessmentEngine(config)
    stop_spinner: list[bool] = [False]

    def _on_sigint(*_args: Any) -> None:
        stop_spinner[0] = True
        print(f"\n\n{C.YELLOW}Interrupted. Exiting gracefully...{C.RESET}")
        sys.exit(130)

    signal.signal(signal.SIGINT, _on_sigint)

    if args.interactive:
        return _interactive_assess(args, config, engine)
    else:
        _print_banner(args.quiet)
        if not args.quiet:
            from wal_e.core.config import CLOUD_DISPLAY_NAMES
            cloud_label = CLOUD_DISPLAY_NAMES.get(config.cloud_provider, config.cloud_provider)
            cloud_color = {
                "aws": C.YELLOW, "azure": C.BLUE, "gcp": C.CYAN,
            }.get(config.cloud_provider, C.DIM)
            print(f"{C.BLUE}Profile:{C.RESET} {args.profile}  {C.BLUE}Output:{C.RESET} {args.output}")
            print(f"{C.BLUE}Cloud:{C.RESET}   {cloud_color}{cloud_label}{C.RESET}\n")

        import threading
        t = threading.Thread(target=_progress_spinner, args=(args.quiet, stop_spinner))
        t.daemon = True
        t.start()
        try:
            result = engine.run_assessment()
        finally:
            stop_spinner[0] = True
            t.join(timeout=0.5)

        if result.errors and not args.quiet:
            for err in result.errors:
                print(f"{C.YELLOW}Warning:{C.RESET} {err}")

        scoring_engine = ScoringEngine()
        scored = scoring_engine.score_all(result.collected_data, config.workspace_host or "Unknown")
        reporter_format = _scored_to_reporter_format(scored)
        audit_entries = _convert_audit_entries(result.raw_responses)

        out_path = Path(args.output)
        out_path.mkdir(parents=True, exist_ok=True)
        formats = args.format or ["md", "csv", "html", "pptx", "audit"]
        if "all" in formats:
            formats = ["md", "csv", "html", "pptx", "audit"]

        reporters_map = {"md": MarkdownReporter(), "csv": CSVReporter(), "html": HTMLDeckReporter(), "pptx": PPTXDeckReporter(), "audit": AuditLogReporter()}
        generated: list[str] = []
        for fmt in formats:
            r = reporters_map.get(fmt)
            if r:
                try:
                    p = r.generate(reporter_format, result.collected_data, audit_entries, out_path)
                    generated.append(str(p))
                except Exception as e:
                    if not args.quiet:
                        print(f"{C.RED}Failed to generate {fmt}:{C.RESET} {e}")

        _save_cached_assessment(out_path, result, scored)
        _print_summary_table(scored.pillar_scores, scored.overall_score, scored.maturity_level, args.quiet)
        if not args.quiet and generated:
            print(f"{C.BOLD}Reports written to:{C.RESET}")
            for g in generated:
                print(f"  {C.DIM}{g}{C.RESET}")
            print()
    return 0


def _interactive_assess(args: argparse.Namespace, config: Any, engine: Any) -> bool:
    from wal_e.framework.scoring import ScoringEngine
    from wal_e.reporters import AuditLogReporter, CSVReporter, HTMLDeckReporter, MarkdownReporter, PPTXDeckReporter

    _print_banner(False)
    print(f"{C.BOLD}Interactive WAL-E Assessment{C.RESET}\n")
    collectors = ["AuthCollector", "GovernanceCollector", "ComputeCollector", "SecurityCollector", "OperationsCollector", "WorkspaceCollector"]
    for i, name in enumerate(collectors):
        print(f"{C.CYAN}Step {i + 1}/{len(collectors)}:{C.RESET} Running {name}...")
        resp = input("  Press Enter to continue (or 'q' to quit): ").strip().lower()
        if resp == "q":
            print(f"{C.YELLOW}Aborted.{C.RESET}")
            return False

    print(f"\n{C.CYAN}Running full assessment...{C.RESET}\n")
    result = engine.run_assessment()
    if result.errors:
        for err in result.errors:
            print(f"{C.YELLOW}Warning:{C.RESET} {err}")

    print(f"{C.GREEN}Scoring assessment...{C.RESET}")
    scoring_engine = ScoringEngine()
    scored = scoring_engine.score_all(result.collected_data, config.workspace_host or "Unknown")
    reporter_format = _scored_to_reporter_format(scored)
    audit_entries = _convert_audit_entries(result.raw_responses)
    out_path = Path(args.output)
    out_path.mkdir(parents=True, exist_ok=True)
    formats = args.format or ["md", "csv", "html", "pptx", "audit"]
    if "all" in formats:
        formats = ["md", "csv", "html", "pptx", "audit"]
    reporters_map = {"md": MarkdownReporter(), "csv": CSVReporter(), "html": HTMLDeckReporter(), "pptx": PPTXDeckReporter(), "audit": AuditLogReporter()}
    for fmt in formats:
        r = reporters_map.get(fmt)
        if r:
            try:
                r.generate(reporter_format, result.collected_data, audit_entries, out_path)
            except Exception:
                pass
    _save_cached_assessment(out_path, result, scored)
    _print_summary_table(scored.pillar_scores, scored.overall_score, scored.maturity_level, False)
    return True


def _run_validate(args: argparse.Namespace) -> int:
    from wal_e.core.config import WalEConfig

    _print_banner(args.quiet)
    config = WalEConfig(profile_name=args.profile)
    ok, msg = config.validate()
    if ok:
        if not args.quiet:
            print(f"{C.GREEN}✓ {msg}{C.RESET}")
        return 0
    if not args.quiet:
        print(f"{C.RED}✗ {msg}{C.RESET}")
    return 1


def _print_access_guide() -> None:
    guide = f"""
{C.BOLD}WAL-E Access Requirements Guide{C.RESET}
{C.DIM}══════════════════════════════════════════════════════════════{C.RESET}

WAL-E makes {C.BOLD}21 read-only API calls{C.RESET} to assess a Databricks workspace.
{C.GREEN}Zero writes. Zero data access. Zero resource modifications.{C.RESET}

{C.BOLD}WHAT THE CUSTOMER NEEDS TO PROVIDE{C.RESET}
{C.DIM}──────────────────────────────────────────────────────────────{C.RESET}

  {C.BOLD}1. Personal Access Token (PAT){C.RESET}
     Created by a {C.YELLOW}workspace admin{C.RESET} (metastore admin recommended)
     - Workspace > Settings > Developer > Access tokens > Generate
     - Lifetime: {C.GREEN}1 day{C.RESET} (assessment takes ~15 minutes)
     - Description: "WAL-E Assessment - [date]"

  {C.BOLD}2. Workspace URL{C.RESET}
     e.g., https://adb-xxxxx.azuredatabricks.net
          https://customer.cloud.databricks.com

{C.BOLD}SA SETUP STEPS{C.RESET}
{C.DIM}──────────────────────────────────────────────────────────────{C.RESET}

  {C.CYAN}${C.RESET} databricks configure --profile customer-assessment \\
      --host https://WORKSPACE-URL --token
  {C.DIM}# Paste the customer's PAT when prompted{C.RESET}

  {C.CYAN}${C.RESET} wal-e validate --profile customer-assessment
  {C.DIM}# Verify connectivity and permissions{C.RESET}

  {C.CYAN}${C.RESET} wal-e assess --profile customer-assessment --interactive
  {C.DIM}# Run the assessment{C.RESET}

{C.BOLD}COVERAGE BY ACCESS LEVEL{C.RESET}
{C.DIM}──────────────────────────────────────────────────────────────{C.RESET}

  Regular user ................. ~40% of best practices scored
  {C.YELLOW}Workspace admin{C.RESET} .............. ~80% of best practices scored
  {C.GREEN}Workspace + Metastore admin{C.RESET} .. ~95% of best practices scored
  Above + System tables ........ 100% of best practices scored

{C.BOLD}API CALLS MADE (ALL READ-ONLY){C.RESET}
{C.DIM}──────────────────────────────────────────────────────────────{C.RESET}

  {C.BLUE}Authentication (2 calls){C.RESET}
    GET  auth describe
    GET  current-user me

  {C.BLUE}Unity Catalog (4 calls){C.RESET}          {C.DIM}[metastore admin recommended]{C.RESET}
    GET  /api/2.1/unity-catalog/metastore_summary
    GET  /api/2.1/unity-catalog/catalogs
    GET  /api/2.1/unity-catalog/external-locations
    GET  /api/2.1/unity-catalog/storage-credentials

  {C.BLUE}Compute (4 calls){C.RESET}               {C.DIM}[admin for all clusters]{C.RESET}
    GET  /api/2.1/clusters/list
    GET  /api/2.0/sql/warehouses
    GET  /api/2.0/cluster-policies/list
    GET  /api/2.0/instance-pools/list

  {C.BLUE}Security (3 calls){C.RESET}              {C.DIM}[workspace admin REQUIRED]{C.RESET}
    GET  /api/2.0/workspace-conf
    GET  /api/2.0/ip-access-lists
    GET  /api/2.0/token/list

  {C.BLUE}Operations (7 calls){C.RESET}            {C.DIM}[admin for complete lists]{C.RESET}
    GET  /api/2.1/jobs/list
    GET  /api/2.0/pipelines
    GET  /api/2.0/serving-endpoints
    GET  /api/2.0/repos
    GET  /api/2.0/global-init-scripts
    GET  /api/2.0/groups/list
    GET  /api/2.0/secrets/list-scopes

  {C.BLUE}Workspace (1 call){C.RESET}
    GET  /api/2.0/workspace/list (root only)

{C.BOLD}SECURITY ASSURANCES FOR THE CUSTOMER{C.RESET}
{C.DIM}──────────────────────────────────────────────────────────────{C.RESET}

  {C.GREEN}+{C.RESET} All calls are HTTPS/TLS encrypted
  {C.GREEN}+{C.RESET} Assessment results stored locally on SA machine only
  {C.GREEN}+{C.RESET} Complete audit trail provided as a deliverable
  {C.GREEN}+{C.RESET} Token can be revoked immediately after assessment

  {C.RED}-{C.RESET} NEVER reads table data, file contents, or query results
  {C.RED}-{C.RESET} NEVER executes notebooks, jobs, or pipelines
  {C.RED}-{C.RESET} NEVER creates, modifies, or deletes any resource
  {C.RED}-{C.RESET} NEVER accesses secret values (only scope names)
  {C.RED}-{C.RESET} NEVER transmits data to external services

{C.BOLD}OPTIONAL: SYSTEM TABLES (for deeper analysis){C.RESET}
{C.DIM}──────────────────────────────────────────────────────────────{C.RESET}

  Customer admin runs:
    GRANT SELECT ON SCHEMA system.billing TO `sa-user`;
    GRANT SELECT ON SCHEMA system.compute TO `sa-user`;
    GRANT SELECT ON SCHEMA system.query   TO `sa-user`;
    GRANT SELECT ON SCHEMA system.access  TO `sa-user`;

{C.BOLD}AFTER THE ASSESSMENT{C.RESET}
{C.DIM}──────────────────────────────────────────────────────────────{C.RESET}

  1. Customer revokes PAT: Settings > Developer > Access tokens > Revoke
  2. SA removes CLI profile: edit ~/.databrickscfg
  3. SA delivers assessment reports to customer
  4. SA deletes local assessment files

{C.DIM}Full documentation: ACCESS_GUIDE.md in the WAL-E repo{C.RESET}
"""
    print(guide)


def _run_setup(args: argparse.Namespace) -> int:
    _print_banner(args.quiet)
    if args.guide:
        _print_access_guide()
    else:
        if not args.quiet:
            print(f"{C.YELLOW}Use --guide to print access requirements.{C.RESET}")
    return 0


def _run_report(args: argparse.Namespace) -> int:
    from wal_e.reporters import AuditLogReporter, CSVReporter, HTMLDeckReporter, MarkdownReporter, PPTXDeckReporter

    _print_banner(args.quiet)
    inp = Path(args.input)
    cache_dir = inp / ".wal-e-cache"
    if not cache_dir.exists():
        if not args.quiet:
            print(f"{C.RED}No cached assessment data in {inp}{C.RESET}")
            print("Run 'wal-e assess' first to generate assessment data.")
        return 1

    collected_path = cache_dir / "collected_data.json"
    scored_path = cache_dir / "scored_assessment.json"
    audit_path = cache_dir / "audit_entries.json"

    if not collected_path.exists() or not scored_path.exists():
        if not args.quiet:
            print(f"{C.RED}Cached data incomplete. Re-run 'wal-e assess'.{C.RESET}")
        return 1

    with open(collected_path) as f:
        collected_data = json.load(f)
    with open(scored_path) as f:
        scored_dict = json.load(f)
    audit_entries = []
    if audit_path.exists():
        with open(audit_path) as f:
            audit_entries = json.load(f)

    reporter_format = {
        "pillar_scores": scored_dict.get("pillar_scores", {}),
        "best_practice_scores": scored_dict.get("best_practice_scores", []),
        "overall_score": scored_dict.get("overall_score", 0),
        "maturity_level": scored_dict.get("maturity_level", "Not Assessed"),
        "assessment_date": scored_dict.get("assessment_date", ""),
        "workspace_host": scored_dict.get("workspace_host", "Unknown"),
        "cloud_provider": scored_dict.get("cloud_provider", "unknown"),
    }

    formats = args.format or ["md", "csv", "html", "pptx", "audit"]
    if "all" in formats:
        formats = ["md", "csv", "html", "pptx", "audit"]
    reporters_map = {"md": MarkdownReporter(), "csv": CSVReporter(), "html": HTMLDeckReporter(), "pptx": PPTXDeckReporter(), "audit": AuditLogReporter()}

    for fmt in formats:
        r = reporters_map.get(fmt)
        if r:
            try:
                r.generate(reporter_format, collected_data, audit_entries, inp)
                if not args.quiet:
                    print(f"{C.GREEN}✓{C.RESET} Generated report ({fmt})")
            except Exception as e:
                if not args.quiet:
                    print(f"{C.RED}✗ {fmt}:{C.RESET} {e}")
                return 1

    if not args.quiet:
        print(f"\n{C.GREEN}Reports regenerated in {inp}{C.RESET}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="wal-e", description="Well-Architected Lakehouse Evaluator")
    parser.add_argument("--version", action="version", version="%(prog)s 0.1.0")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    assess_parser = subparsers.add_parser("assess", help="Run full WAL-E assessment")
    assess_parser.add_argument("--profile", default="DEFAULT", help="Databricks CLI profile (default: DEFAULT)")
    assess_parser.add_argument("-o", "--output", default="./wal-e-assessment", help="Output directory (default: ./wal-e-assessment)")
    assess_parser.add_argument("--format", action="append", choices=["md", "csv", "html", "pptx", "audit", "all"], default=None, help="Output formats (default: all)")
    assess_parser.add_argument("--interactive", action="store_true", help="Interactive mode with step-by-step prompts")
    assess_parser.add_argument("--quiet", action="store_true", help="Suppress progress output")
    assess_parser.set_defaults(func=_run_assess)

    validate_parser = subparsers.add_parser("validate", help="Validate workspace access")
    validate_parser.add_argument("--profile", default="DEFAULT", help="Databricks CLI profile")
    validate_parser.add_argument("--quiet", action="store_true", help="Suppress banner")
    validate_parser.set_defaults(func=_run_validate)

    setup_parser = subparsers.add_parser("setup", help="Print access requirements guide")
    setup_parser.add_argument("--guide", action="store_true", help="Print access requirements for customer")
    setup_parser.add_argument("--quiet", action="store_true", help="Suppress banner")
    setup_parser.set_defaults(func=_run_setup)

    report_parser = subparsers.add_parser("report", help="Re-generate reports from cached data")
    report_parser.add_argument("--input", "-i", default="./wal-e-assessment", help="Input directory with cached assessment data")
    report_parser.add_argument("--format", action="append", choices=["md", "csv", "html", "pptx", "audit", "all"], default=None, help="Output formats")
    report_parser.add_argument("--quiet", action="store_true", help="Suppress output")
    report_parser.set_defaults(func=_run_report)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return 0

    try:
        return args.func(args)
    except KeyboardInterrupt:
        print(f"\n{C.YELLOW}Interrupted.{C.RESET}")
        return 130
    except Exception as e:
        print(f"{C.RED}Error:{C.RESET} {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
