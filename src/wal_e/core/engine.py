"""Main orchestration engine for WAL-E assessments."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from wal_e.collectors import (
    AuthCollector,
    ComputeCollector,
    GovernanceCollector,
    OperationsCollector,
    SecurityCollector,
    WorkspaceCollector,
)
from wal_e.collectors.base import AuditEntry, BaseCollector
from wal_e.core.config import WalEConfig


@dataclass
class AssessmentResult:
    """Result of a full WAL-E assessment run."""

    collected_data: dict[str, Any] = field(default_factory=dict)
    raw_responses: dict[str, list[AuditEntry]] = field(default_factory=dict)
    timing: dict[str, float] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    total_duration_seconds: float = 0.0


class AssessmentEngine:
    """Orchestrates collection, scoring, and reporting for WAL-E assessments."""

    def __init__(self, config: WalEConfig) -> None:
        self.config = config
        self._collectors: list[BaseCollector] = [
            AuthCollector(config.profile_name),
            GovernanceCollector(config.profile_name),
            ComputeCollector(config.profile_name),
            SecurityCollector(config.profile_name),
            OperationsCollector(config.profile_name),
            WorkspaceCollector(config.profile_name),
        ]

    def run_assessment(self) -> AssessmentResult:
        """
        Main entry point: run all collectors, score, and report.

        Returns:
            AssessmentResult with collected data, raw responses, and timing.
        """
        result = AssessmentResult()
        start_total = time.perf_counter()

        for collector in self._collectors:
            collector_name = collector.__class__.__name__
            start = time.perf_counter()
            try:
                findings = collector.collect()
                result.collected_data[collector_name] = findings
                result.raw_responses[collector_name] = list(collector.audit_entries)
            except Exception as e:
                result.errors.append(f"{collector_name}: {e}")
                result.raw_responses[collector_name] = list(collector.audit_entries)
                result.collected_data[collector_name] = {}
            finally:
                result.timing[collector_name] = time.perf_counter() - start

        result.total_duration_seconds = time.perf_counter() - start_total

        return result
