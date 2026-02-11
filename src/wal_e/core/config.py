"""Configuration management for WAL-E assessment tool."""

from __future__ import annotations

import configparser
import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal


@dataclass
class WalEConfig:
    """Configuration for WAL-E assessment runs."""

    profile_name: str = "DEFAULT"
    workspace_host: str = ""
    token: str = ""
    output_dir: str = "./assessment-results"
    formats: list[Literal["md", "csv", "html", "pptx", "audit"]] = field(
        default_factory=lambda: ["md", "audit"]
    )

    def __post_init__(self) -> None:
        """Load profile from ~/.databrickscfg if host/token not set."""
        if not self.workspace_host or not self.token:
            self._load_from_cli_config()

    def _get_config_path(self) -> Path:
        """Get path to Databricks CLI config file."""
        return Path(
            os.environ.get("DATABRICKS_CONFIG_FILE", Path.home() / ".databrickscfg")
        )

    def _load_from_cli_config(self) -> None:
        """Load workspace host and token from ~/.databrickscfg."""
        config_path = self._get_config_path()
        if not config_path.exists():
            return

        parser = configparser.ConfigParser()
        parser.read(config_path)

        profile = self.profile_name
        if profile not in parser:
            profile = "DEFAULT"
        if profile not in parser:
            return

        section = parser[profile]
        self.workspace_host = section.get("host", self.workspace_host)
        self.token = section.get("token", self.token)

    def validate(self) -> tuple[bool, str]:
        """
        Validate configuration and check connectivity to the workspace.

        Returns:
            Tuple of (success: bool, message: str)
        """
        if not self.workspace_host:
            return False, "Workspace host not configured. Run 'databricks configure' or set host in config."
        if not self.token:
            return False, "Authentication token not configured. Run 'databricks configure --token'."

        try:
            result = subprocess.run(
                ["databricks", "api", "get", "/api/2.1/clusters/list", "--profile", self.profile_name],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode == 0:
                return True, "Connection validated successfully."
            stderr = result.stderr.strip() if result.stderr else "Unknown error"
            return False, f"Connection failed: {stderr}"
        except FileNotFoundError:
            return False, "Databricks CLI not found. Install it: pip install databricks-cli"
        except subprocess.TimeoutExpired:
            return False, "Connection timed out. Check network and workspace URL."
        except Exception as e:
            return False, f"Validation error: {e}"
