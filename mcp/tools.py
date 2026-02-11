"""
WAL-E MCP Tool Definitions

Tool implementations for running assessments, collection, scoring, and reporting
via the Model Context Protocol (MCP) for AI Dev Kit integration.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any


def wal_e_assess(profile: str = "DEFAULT", output_dir: str = "./wal-e-assessment") -> dict:
    """
    Run a full WAL-E assessment against a Databricks workspace.

    Collects workspace data, scores against Well-Architected Lakehouse best practices,
    and generates reports in the output directory.

    Args:
        profile: Databricks CLI profile name (default: DEFAULT)
        output_dir: Directory to write assessment reports (default: ./wal-e-assessment)

    Returns:
        JSON object with success status, pillar scores, overall score, and output path
    """
    from wal_e.core.config import WalEConfig
    from wal_e.core.engine import AssessmentEngine
    from wal_e.framework.scoring import ScoringEngine

    try:
        config = WalEConfig(profile_name=profile, output_dir=output_dir)
        engine = AssessmentEngine(config)
        result = engine.run_assessment()

        scoring_engine = ScoringEngine()
        scored = scoring_engine.score_all(result.collected_data, config.workspace_host or "Unknown")

        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)

        # Save cache for report regeneration
        from wal_e.cli import _convert_audit_entries, _scored_to_reporter_format
        from wal_e.reporters import AuditLogReporter, CSVReporter, HTMLDeckReporter, MarkdownReporter, PPTXDeckReporter

        audit_entries = _convert_audit_entries(result.raw_responses)
        reporter_format = _scored_to_reporter_format(scored)

        cache_dir = out_path / ".wal-e-cache"
        cache_dir.mkdir(exist_ok=True)
        with open(cache_dir / "collected_data.json", "w") as f:
            json.dump(result.collected_data, f, default=str, indent=2)
        with open(cache_dir / "scored_assessment.json", "w") as f:
            json.dump(asdict(scored), f, indent=2)
        with open(cache_dir / "audit_entries.json", "w") as f:
            json.dump(audit_entries, f, indent=2)

        # Generate reports
        reporters = [MarkdownReporter(), CSVReporter(), HTMLDeckReporter(), AuditLogReporter()]
        try:
            reporters.append(PPTXDeckReporter())
        except Exception:
            pass

        generated = []
        for r in reporters:
            try:
                p = r.generate(reporter_format, result.collected_data, audit_entries, out_path)
                generated.append(str(p))
            except Exception:
                pass

        return {
            "success": True,
            "profile": profile,
            "output_dir": output_dir,
            "pillar_scores": scored.pillar_scores,
            "overall_score": scored.overall_score,
            "maturity_level": scored.maturity_level,
            "errors": result.errors,
            "generated_reports": generated,
        }
    except Exception as e:
        return {"success": False, "error": str(e), "profile": profile, "output_dir": output_dir}


def wal_e_collect(profile: str = "DEFAULT") -> dict:
    """
    Collect workspace data only (no scoring or reporting).

    Runs all WAL-E collectors and returns the raw collected data.

    Args:
        profile: Databricks CLI profile name

    Returns:
        JSON object with collected_data and raw_responses
    """
    from wal_e.core.config import WalEConfig
    from wal_e.core.engine import AssessmentEngine

    try:
        config = WalEConfig(profile_name=profile)
        engine = AssessmentEngine(config)
        result = engine.run_assessment()

        return {
            "success": True,
            "collected_data": result.collected_data,
            "errors": result.errors,
            "timing": result.timing,
        }
    except Exception as e:
        return {"success": False, "error": str(e), "profile": profile}


def wal_e_score(collected_data_path: str) -> dict:
    """
    Score assessment from cached collected data.

    Args:
        collected_data_path: Path to assessment directory with .wal-e-cache/collected_data.json,
            or path to collected_data.json file directly

    Returns:
        JSON object with pillar_scores, overall_score, maturity_level
    """
    from wal_e.framework.scoring import ScoringEngine

    try:
        path = Path(collected_data_path)
        if path.is_file():
            data_file = path
        elif (path / ".wal-e-cache" / "collected_data.json").exists():
            data_file = path / ".wal-e-cache" / "collected_data.json"
        elif (path / "collected_data.json").exists():
            data_file = path / "collected_data.json"
        else:
            return {"success": False, "error": f"Collected data not found at {collected_data_path}"}

        with open(data_file) as f:
            collected_data = json.load(f)

        scoring_engine = ScoringEngine()
        scored = scoring_engine.score_all(collected_data)

        return {
            "success": True,
            "pillar_scores": scored.pillar_scores,
            "overall_score": scored.overall_score,
            "maturity_level": scored.maturity_level,
            "assessment_date": scored.assessment_date,
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


def wal_e_report(assessment_path: str, format: str = "md") -> dict:
    """
    Generate reports from cached assessment data.

    Args:
        assessment_path: Path to directory with .wal-e-cache (from assess or collect+score)
        format: Report format - md, csv, html, pptx, or audit

    Returns:
        JSON object with success status and generated file path
    """
    from wal_e.reporters import AuditLogReporter, CSVReporter, HTMLDeckReporter, MarkdownReporter, PPTXDeckReporter

    try:
        inp = Path(assessment_path)
        cache_dir = inp / ".wal-e-cache"
        if not cache_dir.exists():
            return {"success": False, "error": f"No cached data at {assessment_path}"}

        with open(cache_dir / "collected_data.json") as f:
            collected_data = json.load(f)
        with open(cache_dir / "scored_assessment.json") as f:
            scored_dict = json.load(f)
        audit_entries = []
        if (cache_dir / "audit_entries.json").exists():
            with open(cache_dir / "audit_entries.json") as f:
                audit_entries = json.load(f)

        reporter_format = {
            "pillar_scores": scored_dict.get("pillar_scores", {}),
            "best_practice_scores": scored_dict.get("best_practice_scores", []),
            "overall_score": scored_dict.get("overall_score", 0),
            "maturity_level": scored_dict.get("maturity_level", "Not Assessed"),
            "assessment_date": scored_dict.get("assessment_date", ""),
            "workspace_host": scored_dict.get("workspace_host", "Unknown"),
        }

        reporters = {"md": MarkdownReporter(), "csv": CSVReporter(), "html": HTMLDeckReporter(), "pptx": PPTXDeckReporter(), "audit": AuditLogReporter()}
        r = reporters.get(format)
        if not r:
            return {"success": False, "error": f"Unknown format: {format}. Use md, csv, html, pptx, or audit"}

        p = r.generate(reporter_format, collected_data, audit_entries, inp)
        return {"success": True, "format": format, "output_path": str(p)}
    except Exception as e:
        return {"success": False, "error": str(e)}


def wal_e_validate(profile: str = "DEFAULT") -> dict:
    """
    Validate workspace access and connectivity.

    Args:
        profile: Databricks CLI profile name

    Returns:
        JSON object with success status and message
    """
    from wal_e.core.config import WalEConfig

    try:
        config = WalEConfig(profile_name=profile)
        ok, msg = config.validate()
        return {"success": ok, "message": msg, "profile": profile}
    except Exception as e:
        return {"success": False, "error": str(e), "profile": profile}
