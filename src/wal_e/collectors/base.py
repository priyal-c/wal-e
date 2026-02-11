"""Base collector class for Databricks workspace data collection."""

from __future__ import annotations

import json
import subprocess
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class AuditEntry:
    """Single audit trail entry for an API/CLI call."""

    command: list[str]
    raw_output: str
    duration_seconds: float
    success: bool
    error: str | None = None


class BaseCollector(ABC):
    """Base class for all WAL-E data collectors."""

    def __init__(self, profile_name: str = "DEFAULT") -> None:
        self.profile_name = profile_name
        self.audit_entries: list[AuditEntry] = []

    def run_cli_command(self, cmd: list[str]) -> tuple[str | None, bool]:
        """
        Execute a Databricks CLI command and capture output for audit trail.

        Args:
            cmd: Command as list, e.g. ["databricks", "auth", "describe", "--profile", "DEFAULT"]

        Returns:
            Tuple of (output string or None on failure, success flag)
        """
        start = time.perf_counter()
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120,
            )
            duration = time.perf_counter() - start
            output = result.stdout if result.stdout else ""
            stderr = result.stderr.strip() if result.stderr else ""
            success = result.returncode == 0

            self.audit_entries.append(
                AuditEntry(
                    command=cmd,
                    raw_output=output or stderr,
                    duration_seconds=duration,
                    success=success,
                    error=None if success else (stderr or f"Exit code {result.returncode}"),
                )
            )
            return (output if success else None, success)
        except subprocess.TimeoutExpired:
            duration = time.perf_counter() - start
            self.audit_entries.append(
                AuditEntry(
                    command=cmd,
                    raw_output="",
                    duration_seconds=duration,
                    success=False,
                    error="Command timed out after 120s",
                )
            )
            return (None, False)
        except Exception as e:
            duration = time.perf_counter() - start
            self.audit_entries.append(
                AuditEntry(
                    command=cmd,
                    raw_output="",
                    duration_seconds=duration,
                    success=False,
                    error=str(e),
                )
            )
            return (None, False)

    def run_api_call(self, endpoint: str) -> tuple[dict[str, Any] | None, bool]:
        """
        Call Databricks REST API via `databricks api get {endpoint}`.

        Args:
            endpoint: API path, e.g. "/api/2.0/clusters/list"

        Returns:
            Tuple of (parsed JSON dict or None, success flag)
        """
        cmd = [
            "databricks",
            "api",
            "get",
            endpoint,
            "--profile",
            self.profile_name,
        ]
        output, success = self.run_cli_command(cmd)
        if not success or not output:
            return (None, False)
        try:
            return (json.loads(output), True)
        except json.JSONDecodeError:
            return (None, False)

    @abstractmethod
    def collect(self) -> dict[str, Any]:
        """
        Collect data from the workspace.

        Returns:
            Dict of findings keyed by category/subject.
        """
        ...
