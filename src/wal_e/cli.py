"""
WAL-E CLI - Well-Architected Lakehouse Evaluator

Command-line interface for running assessments, validation, and report generation.
"""

from __future__ import annotations

import argparse
import json
import signal
import subprocess
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
\x1b[96m __        __           ||       |_____
 \\ \\      / /    / \\    ||        | ____|
  \\ \\ /\\ / /    / _ \\   ||        | |__
   \\ V  V /    / ___ \\  ||        |  __|
    \\_/\\_/    /_/   \\_\\ ||____    |_|____
\x1b[0m    \x1b[2mWell-Architected Lakehouse Evaluator\x1b[0m
    \x1b[2mRuns on YOUR machine • SA guides you\x1b[0m
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


def _print_summary_table(
    pillar_scores: dict[str, float],
    overall: float,
    maturity: str,
    quiet: bool,
    verified_score: float = 0.0,
    coverage_pct: float = 0.0,
    pillar_verified_scores: dict[str, float] | None = None,
    pillar_coverage: dict[str, float] | None = None,
) -> None:
    if quiet:
        return
    from wal_e.reporters.base import PILLAR_DISPLAY_NAMES, PILLAR_ORDER

    pv = pillar_verified_scores or {}
    pc = pillar_coverage or {}
    has_verified = bool(pv)

    print(f"\n\n{C.BOLD}{C.GREEN}✓ Assessment Complete{C.RESET}\n")

    if has_verified:
        header = f"  {'Pillar':<40} {'Verified Score':>15} {'Coverage':>10}"
        print(f"{C.BOLD}{header}{C.RESET}")
    else:
        print(f"{C.BOLD}Pillar Scores{C.RESET}")
    print("─" * 70)

    for pillar in PILLAR_ORDER:
        display = PILLAR_DISPLAY_NAMES.get(pillar, pillar)
        if has_verified:
            v_score = pv.get(pillar, 0)
            v_pct = (v_score / 2.0) * 100 if v_score else 0
            cov = pc.get(pillar, 0)
            bar_len = 15
            filled = int(bar_len * v_pct / 100)
            bar = f"{C.GREEN}█{C.RESET}" * filled + f"{C.DIM}░{C.RESET}" * (bar_len - filled)
            cov_color = C.GREEN if cov >= 60 else (C.YELLOW if cov >= 40 else C.RED)
            print(f"  {display[:40]:<40} {bar} {v_pct:>3.0f}%   {cov_color}{cov:>4.0f}%{C.RESET}")
        else:
            score = pillar_scores.get(pillar, 0)
            pct = (score / 2.0) * 100 if score is not None else 0
            bar_len = 20
            filled = int(bar_len * pct / 100)
            bar = f"{C.GREEN}█{C.RESET}" * filled + f"{C.DIM}░{C.RESET}" * (bar_len - filled)
            print(f"  {display[:40]:<40} {bar} {pct:.0f}%")

    print("─" * 70)

    if has_verified:
        v_pct = (verified_score / 2.0) * 100
        print(f"  {C.BOLD}Verified Score{C.RESET}{'':<28} {v_pct:.0f}%  ({maturity})")
        print(f"  {C.DIM}Coverage: {coverage_pct:.0f}% of best practices had enough data to verify{C.RESET}")
    else:
        overall_pct = (overall / 2.0) * 100
        print(f"  {C.BOLD}Overall{C.RESET}{'':<35} {overall_pct:.0f}%  ({maturity})")
    print()


def _print_unverified_bps(scored_bps: list[dict], quiet: bool) -> None:
    """Print a short footnote about unverified BPs, directing to reports for details."""
    if quiet:
        return

    count = sum(1 for bp in scored_bps if not (bp.get("verified", True) if isinstance(bp, dict) else getattr(bp, "verified", True)))
    if not count:
        return

    print(f"  {C.YELLOW}Note:{C.RESET} {count} best practices could not be automatically verified.")
    print(f"  {C.DIM}See the Remediation Guide (DOCX) or Readout (MD) for the full list and how to increase coverage.{C.RESET}")
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
            best_practice_scores.append({"name": bp.name, "pillar": bp.pillar, "principle": bp.principle, "score": float(bp.score), "finding_notes": bp.finding_notes, "verified": getattr(bp, "verified", True)})
        elif isinstance(bp, dict):
            best_practice_scores.append(bp)

    return {
        "pillar_scores": pillar_scores,
        "best_practice_scores": best_practice_scores,
        "overall_score": scored.overall_score,
        "verified_score": getattr(scored, "verified_score", scored.overall_score),
        "coverage_pct": getattr(scored, "coverage_pct", 100.0),
        "maturity_level": scored.maturity_level,
        "assessment_date": scored.assessment_date,
        "workspace_host": scored.workspace_host or "Unknown",
        "cloud_provider": getattr(scored, "cloud_provider", "") or "unknown",
        "pillar_verified_scores": getattr(scored, "pillar_verified_scores", pillar_scores),
        "pillar_coverage": getattr(scored, "pillar_coverage", {}),
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


def _run_assess_foreground(args: argparse.Namespace, config: Any, engine: Any) -> int:
    """Run the assessment in the foreground (default mode)."""
    from wal_e.framework.scoring import ScoringEngine
    from wal_e.reporters import AuditLogReporter, CSVReporter, DocxRemediationReporter, MarkdownReporter, PPTXDeckReporter

    stop_spinner: list[bool] = [False]

    _print_banner(args.quiet)
    if not args.quiet:
        from wal_e.core.config import CLOUD_DISPLAY_NAMES
        cloud_label = CLOUD_DISPLAY_NAMES.get(config.cloud_provider, config.cloud_provider)
        cloud_color = {
            "aws": C.YELLOW, "azure": C.BLUE, "gcp": C.CYAN,
        }.get(config.cloud_provider, C.DIM)
        print(f"{C.BLUE}Profile:{C.RESET} {args.profile}  {C.BLUE}Output:{C.RESET} {args.output}")
        print(f"{C.BLUE}Cloud:{C.RESET}   {cloud_color}{cloud_label}{C.RESET}")
        if config.deep_scan:
            print(f"{C.BLUE}Mode:{C.RESET}    {C.GREEN}Deep Scan{C.RESET} (system tables via warehouse {config.warehouse_id})")
        else:
            print(f"{C.BLUE}Mode:{C.RESET}    Standard (API-only)")
        timeout_val = getattr(args, "timeout", 600)
        if timeout_val and timeout_val > 0:
            print(f"{C.BLUE}Timeout:{C.RESET} {timeout_val}s")
        else:
            print(f"{C.BLUE}Timeout:{C.RESET} none")
        print()

    import threading
    t = threading.Thread(target=_progress_spinner, args=(args.quiet, stop_spinner))
    t.daemon = True
    t.start()

    # Apply timeout if set
    timeout_val = getattr(args, "timeout", 600)
    timed_out = [False]

    def _run_with_timeout():
        try:
            return engine.run_assessment()
        except Exception as e:
            return e

    if timeout_val and timeout_val > 0:
        result_holder: list = [None]
        def _target():
            result_holder[0] = _run_with_timeout()
        worker = threading.Thread(target=_target)
        worker.daemon = True
        worker.start()
        worker.join(timeout=timeout_val)
        if worker.is_alive():
            timed_out[0] = True
            stop_spinner[0] = True
            t.join(timeout=0.5)
            print(f"\n{C.RED}Assessment timed out after {timeout_val}s.{C.RESET}")
            print(f"{C.YELLOW}Tip:{C.RESET} Use {C.BOLD}--timeout 0{C.RESET} for no timeout, or run in a separate terminal.")
            return 1
        result = result_holder[0]
        if isinstance(result, Exception):
            raise result
    else:
        result = engine.run_assessment()

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
    formats = args.format or ["md", "csv", "pptx", "audit", "docx"]
    if "all" in formats:
        formats = ["md", "csv", "pptx", "audit", "docx"]

    reporters_map = {"md": MarkdownReporter(), "csv": CSVReporter(), "pptx": PPTXDeckReporter(), "audit": AuditLogReporter(), "docx": DocxRemediationReporter()}
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
    _print_summary_table(
        scored.pillar_scores, scored.overall_score, scored.maturity_level, args.quiet,
        verified_score=scored.verified_score, coverage_pct=scored.coverage_pct,
        pillar_verified_scores=scored.pillar_verified_scores, pillar_coverage=scored.pillar_coverage,
    )
    bp_dicts = [{"pillar": bp.pillar, "name": bp.name, "score": bp.score, "finding_notes": bp.finding_notes, "verified": bp.verified} for bp in scored.best_practice_scores]
    _print_unverified_bps(bp_dicts, args.quiet)
    if not args.quiet and generated:
        print(f"{C.BOLD}Reports written to:{C.RESET}")
        for g in generated:
            print(f"  {C.DIM}{g}{C.RESET}")
        print()
    return 0


def _run_assess_background(args: argparse.Namespace, config: Any, engine: Any) -> int:
    """Fork the assessment into a background process and return immediately."""
    import multiprocessing
    import os

    out_path = Path(args.output)
    out_path.mkdir(parents=True, exist_ok=True)
    pid_file = out_path / ".wal-e-cache" / "bg.pid"
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    status_file = out_path / ".wal-e-cache" / "bg.status"

    def _background_worker(profile: str, output: str, formats_list: list, host: str) -> None:
        """Runs in a child process — no TTY output."""
        import json as _json
        from wal_e.core.config import WalEConfig as _Cfg
        from wal_e.core.engine import AssessmentEngine as _Eng
        from wal_e.framework.scoring import ScoringEngine as _Sc
        from wal_e.reporters import AuditLogReporter, CSVReporter, DocxRemediationReporter, MarkdownReporter, PPTXDeckReporter

        _out = Path(output)
        _out.mkdir(parents=True, exist_ok=True)
        _status = _out / ".wal-e-cache" / "bg.status"

        try:
            _status.write_text("running")
            _cfg = _Cfg(profile_name=profile, output_dir=output)
            _eng = _Eng(_cfg)
            result = _eng.run_assessment()

            sc_engine = _Sc()
            scored = sc_engine.score_all(result.collected_data, _cfg.workspace_host or host)

            reporter_format = {
                "pillar_scores": dict(scored.pillar_scores) if scored.pillar_scores else {},
                "best_practice_scores": [
                    {"name": bp.name, "pillar": bp.pillar, "principle": bp.principle,
                     "score": float(bp.score), "finding_notes": bp.finding_notes,
                     "verified": getattr(bp, "verified", True)}
                    for bp in scored.best_practice_scores
                ],
                "overall_score": scored.overall_score,
                "maturity_level": scored.maturity_level,
                "assessment_date": scored.assessment_date,
                "workspace_host": scored.workspace_host or "Unknown",
                "cloud_provider": getattr(scored, "cloud_provider", "") or "unknown",
            }

            audit_entries = []
            for _cname, audit_list in result.raw_responses.items():
                for ae in audit_list:
                    if hasattr(ae, "command"):
                        cmd_str = " ".join(ae.command) if isinstance(ae.command, list) else str(ae.command)
                        audit_entries.append({"command": cmd_str, "output": getattr(ae, "raw_output", ""), "timestamp": "", "duration": getattr(ae, "duration_seconds", 0)})

            fmts = formats_list or ["md", "csv", "pptx", "audit", "docx"]
            if "all" in fmts:
                fmts = ["md", "csv", "pptx", "audit", "docx"]
            reporters_map = {"md": MarkdownReporter(), "csv": CSVReporter(), "pptx": PPTXDeckReporter(), "audit": AuditLogReporter(), "docx": DocxRemediationReporter()}
            for fmt in fmts:
                r = reporters_map.get(fmt)
                if r:
                    try:
                        r.generate(reporter_format, result.collected_data, audit_entries, _out)
                    except Exception:
                        pass

            # Save cache
            cache_dir = _out / ".wal-e-cache"
            cache_dir.mkdir(exist_ok=True)
            with open(cache_dir / "collected_data.json", "w") as f:
                _json.dump(result.collected_data, f, default=str, indent=2)
            with open(cache_dir / "scored_assessment.json", "w") as f:
                from dataclasses import asdict as _asdict
                _json.dump(_asdict(scored), f, indent=2)
            with open(cache_dir / "audit_entries.json", "w") as f:
                _json.dump(audit_entries, f, indent=2)

            _status.write_text("complete")
        except Exception as e:
            _status.write_text(f"error: {e}")

    _print_banner(args.quiet)
    if not args.quiet:
        from wal_e.core.config import CLOUD_DISPLAY_NAMES
        cloud_label = CLOUD_DISPLAY_NAMES.get(config.cloud_provider, config.cloud_provider)
        print(f"{C.BLUE}Profile:{C.RESET} {args.profile}  {C.BLUE}Cloud:{C.RESET} {cloud_label}")

    p = multiprocessing.Process(
        target=_background_worker,
        args=(args.profile, args.output, args.format, config.workspace_host or "Unknown"),
        daemon=False,
    )
    p.start()
    pid_file.write_text(str(p.pid))

    print(f"\n{C.GREEN}Assessment running in background (PID: {p.pid}){C.RESET}")
    print(f"  Output directory: {C.CYAN}{args.output}{C.RESET}")
    print(f"\n{C.BOLD}Check progress:{C.RESET}")
    print(f"  cat {args.output}/.wal-e-cache/bg.status")
    print(f"\n{C.BOLD}When complete, regenerate reports:{C.RESET}")
    print(f"  wal-e report --input {args.output} --format all")
    print()
    return 0


def _auto_discover_warehouse(profile: str) -> str:
    """Auto-discover the best SQL warehouse for deep scan.

    Priority: serverless + RUNNING > serverless + STOPPED (auto-starts) > any RUNNING > any STOPPED.
    """
    try:
        result = subprocess.run(
            ["databricks", "api", "get", "/api/2.0/sql/warehouses", "--profile", profile],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0 or not result.stdout:
            return ""
        data = json.loads(result.stdout)
        warehouses = data.get("warehouses", []) or []
        if not warehouses:
            return ""

        # Score each warehouse: higher = better candidate
        def _rank(w: dict) -> tuple[int, int, str]:
            is_serverless = 1 if w.get("enable_serverless_compute") else 0
            is_running = 1 if w.get("state") == "RUNNING" else 0
            return (is_serverless, is_running, w.get("id", ""))

        warehouses.sort(key=_rank, reverse=True)
        best = warehouses[0]
        wh_id = best.get("id", "")
        wh_name = best.get("name", "unknown")
        wh_type = "serverless" if best.get("enable_serverless_compute") else "classic"
        wh_state = best.get("state", "UNKNOWN")

        print(f"{C.BLUE}Deep scan:{C.RESET} Auto-selected warehouse {C.BOLD}{wh_name}{C.RESET} ({wh_type}, {wh_state})")
        if wh_state != "RUNNING":
            print(f"  {C.DIM}Warehouse will auto-start when the first query is sent.{C.RESET}")
        return wh_id
    except Exception:
        return ""


def _run_assess(args: argparse.Namespace) -> int:
    from wal_e.core.config import WalEConfig
    from wal_e.core.engine import AssessmentEngine

    deep = getattr(args, "deep", False)
    wh_id = getattr(args, "warehouse_id", "")

    if deep and not wh_id:
        wh_id = _auto_discover_warehouse(args.profile)
        if not wh_id:
            print(f"{C.RED}Error:{C.RESET} --deep requires a SQL warehouse but none could be auto-discovered.")
            print(f"  Either provide --warehouse-id <ID> or ensure at least one SQL warehouse exists.")
            print(f"  Find your warehouse ID: Workspace > SQL Warehouses > click warehouse > copy ID from URL")
            return 1

    config = WalEConfig(
        profile_name=args.profile,
        output_dir=args.output,
        formats=args.format or ["md", "csv", "pptx", "audit", "docx"],
        deep_scan=deep,
        warehouse_id=wh_id,
    )
    engine = AssessmentEngine(config)

    def _on_sigint(*_args: Any) -> None:
        print(f"\n\n{C.YELLOW}Interrupted. Exiting gracefully...{C.RESET}")
        sys.exit(130)

    signal.signal(signal.SIGINT, _on_sigint)

    if args.interactive:
        return _interactive_assess(args, config, engine)
    elif getattr(args, "run_in_background", False):
        return _run_assess_background(args, config, engine)
    else:
        return _run_assess_foreground(args, config, engine)


def _interactive_assess(args: argparse.Namespace, config: Any, engine: Any) -> bool:
    from wal_e.framework.scoring import ScoringEngine
    from wal_e.reporters import AuditLogReporter, CSVReporter, DocxRemediationReporter, MarkdownReporter, PPTXDeckReporter

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
    formats = args.format or ["md", "csv", "pptx", "audit", "docx"]
    if "all" in formats:
        formats = ["md", "csv", "pptx", "audit", "docx"]
    reporters_map = {"md": MarkdownReporter(), "csv": CSVReporter(), "pptx": PPTXDeckReporter(), "audit": AuditLogReporter(), "docx": DocxRemediationReporter()}
    for fmt in formats:
        r = reporters_map.get(fmt)
        if r:
            try:
                r.generate(reporter_format, result.collected_data, audit_entries, out_path)
            except Exception:
                pass
    _save_cached_assessment(out_path, result, scored)
    _print_summary_table(
        scored.pillar_scores, scored.overall_score, scored.maturity_level, False,
        verified_score=scored.verified_score, coverage_pct=scored.coverage_pct,
        pillar_verified_scores=scored.pillar_verified_scores, pillar_coverage=scored.pillar_coverage,
    )
    bp_dicts = [{"pillar": bp.pillar, "name": bp.name, "score": bp.score, "finding_notes": bp.finding_notes, "verified": bp.verified} for bp in scored.best_practice_scores]
    _print_unverified_bps(bp_dicts, False)
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
{C.BOLD}WAL-E Setup Guide — For You (the Customer){C.RESET}
{C.DIM}══════════════════════════════════════════════════════════════{C.RESET}

{C.GREEN}You run WAL-E on YOUR machine. Your SA guides you.{C.RESET}
{C.GREEN}No tokens or data leave your environment.{C.RESET}

WAL-E makes {C.BOLD}21 read-only API calls{C.RESET} to assess your workspace.
{C.GREEN}Zero writes. Zero data access. Zero resource modifications.{C.RESET}

{C.BOLD}STEP 1: CREATE A PAT TOKEN (1 minute){C.RESET}
{C.DIM}──────────────────────────────────────────────────────────────{C.RESET}

  Log into your workspace as a {C.YELLOW}workspace admin{C.RESET}:
  1. Click your username (top-right) > {C.BOLD}Settings{C.RESET}
  2. Go to {C.BOLD}Developer{C.RESET} > {C.BOLD}Access tokens{C.RESET} > {C.BOLD}Generate New Token{C.RESET}
  3. Description: {C.DIM}"WAL-E Assessment - [today's date]"{C.RESET}
  4. Lifetime: {C.GREEN}1 day{C.RESET} (assessment takes ~15 minutes)
  5. Click {C.BOLD}Generate{C.RESET} and copy the token

{C.BOLD}STEP 2: CONFIGURE THE DATABRICKS CLI (1 minute){C.RESET}
{C.DIM}──────────────────────────────────────────────────────────────{C.RESET}

  {C.CYAN}${C.RESET} databricks configure --profile wal-assessment \\
      --host https://YOUR-WORKSPACE-URL --token
  {C.DIM}# Paste the PAT token you just created{C.RESET}

{C.BOLD}STEP 3: VALIDATE ACCESS (30 seconds){C.RESET}
{C.DIM}──────────────────────────────────────────────────────────────{C.RESET}

  {C.CYAN}${C.RESET} wal-e validate --profile wal-assessment

{C.BOLD}STEP 4: RUN THE ASSESSMENT (5-10 minutes){C.RESET}
{C.DIM}──────────────────────────────────────────────────────────────{C.RESET}

  {C.CYAN}${C.RESET} wal-e assess --profile wal-assessment --interactive
  {C.DIM}# Your SA will walk you through each step{C.RESET}

  Or for a quick scan:
  {C.CYAN}${C.RESET} wal-e assess --profile wal-assessment --output ./my-assessment --format all

{C.BOLD}STEP 5: REVIEW RESULTS WITH YOUR SA{C.RESET}
{C.DIM}──────────────────────────────────────────────────────────────{C.RESET}

  Reports are saved in your output directory:
  - WAL_Assessment_Readout.md     (detailed report)
  - WAL_Assessment_Scores.csv     (129 scored best practices)
  - WAL_Assessment_Presentation.pptx (executive deck)
  - WAL_Assessment_Audit_Report.md   (full evidence trail)

  Share with your SA via screen share or by sending the files.

{C.BOLD}STEP 6: CLEAN UP (1 minute){C.RESET}
{C.DIM}──────────────────────────────────────────────────────────────{C.RESET}

  1. Revoke your token: Settings > Developer > Access tokens > Revoke
  2. Remove CLI profile: edit ~/.databrickscfg, delete [wal-assessment]
  3. Delete local files: rm -rf ./my-assessment

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

{C.BOLD}SECURITY ASSURANCES{C.RESET}
{C.DIM}──────────────────────────────────────────────────────────────{C.RESET}

  {C.GREEN}+{C.RESET} YOU run everything on YOUR machine
  {C.GREEN}+{C.RESET} Your token NEVER leaves your environment
  {C.GREEN}+{C.RESET} All calls are HTTPS/TLS encrypted to YOUR workspace
  {C.GREEN}+{C.RESET} Results are stored locally on YOUR machine only
  {C.GREEN}+{C.RESET} Complete audit trail so you can verify every API call
  {C.GREEN}+{C.RESET} Token auto-expires in 1 day (or revoke immediately)

  {C.RED}-{C.RESET} NEVER reads table data, file contents, or query results
  {C.RED}-{C.RESET} NEVER executes notebooks, jobs, or pipelines
  {C.RED}-{C.RESET} NEVER creates, modifies, or deletes any resource
  {C.RED}-{C.RESET} NEVER accesses secret values (only scope names)
  {C.RED}-{C.RESET} NEVER transmits data to any external service

{C.BOLD}OPTIONAL: DEEP SCAN (system tables for operational analysis){C.RESET}
{C.DIM}──────────────────────────────────────────────────────────────{C.RESET}

  The standard scan uses the 21 API calls above (129 best practices).
  For a {C.GREEN}deep scan{C.RESET} (+11 best practices), WAL-E also queries system tables
  to assess actual cost trends, cluster idle time, query failure rates,
  job success rates, and security audit events.

  {C.BOLD}Requires:{C.RESET}
    - A running SQL warehouse (note the warehouse ID)
    - SELECT grants on system schemas

  Your account admin runs these in a SQL warehouse:
    {C.CYAN}GRANT SELECT ON SCHEMA system.billing TO `your-user`;{C.RESET}
    {C.CYAN}GRANT SELECT ON SCHEMA system.compute TO `your-user`;{C.RESET}
    {C.CYAN}GRANT SELECT ON SCHEMA system.query   TO `your-user`;{C.RESET}
    {C.CYAN}GRANT SELECT ON SCHEMA system.access  TO `your-user`;{C.RESET}

  Then run:
    {C.CYAN}${C.RESET} wal-e assess --profile wal-assessment --deep --warehouse-id <ID>

  {C.DIM}Deep scan is optional. Standard mode works perfectly with just APIs.{C.RESET}

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
    from wal_e.reporters import AuditLogReporter, CSVReporter, DocxRemediationReporter, MarkdownReporter, PPTXDeckReporter

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

    formats = args.format or ["md", "csv", "pptx", "audit", "docx"]
    if "all" in formats:
        formats = ["md", "csv", "pptx", "audit", "docx"]
    reporters_map = {"md": MarkdownReporter(), "csv": CSVReporter(), "pptx": PPTXDeckReporter(), "audit": AuditLogReporter(), "docx": DocxRemediationReporter()}

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
    assess_parser.add_argument("--format", action="append", choices=["md", "csv", "pptx", "audit", "docx", "all"], default=None, help="Output formats (default: all). Use 'docx' for remediation guide.")
    assess_parser.add_argument("--interactive", action="store_true", help="Interactive mode with step-by-step prompts")
    assess_parser.add_argument("--quiet", action="store_true", help="Suppress progress output")
    assess_parser.add_argument("--timeout", type=int, default=600, metavar="SECONDS",
                               help="Maximum time in seconds for the assessment to complete (default: 600 = 10 min). "
                                    "Use 0 for no timeout. Useful when running inside Claude Code or other AI tools.")
    assess_parser.add_argument("--run-in-background", action="store_true",
                               help="Run assessment in background and return immediately. "
                                    "Results are written to the output directory when complete. "
                                    "Use 'wal-e report' to check/regenerate reports from cached data.")
    assess_parser.add_argument("--deep", action="store_true",
                               help="Deep scan: query system tables (billing, compute, query history, "
                                    "audit) via a SQL warehouse for operational reality analysis. "
                                    "Requires --warehouse-id and SELECT on system.* schemas.")
    assess_parser.add_argument("--warehouse-id", default="", metavar="ID",
                               help="SQL warehouse ID for --deep scan. If omitted, WAL-E auto-selects "
                                    "the best available warehouse (prefers serverless).")
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
    report_parser.add_argument("--format", action="append", choices=["md", "csv", "pptx", "audit", "docx", "all"], default=None, help="Output formats. Use 'docx' for remediation guide.")
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
