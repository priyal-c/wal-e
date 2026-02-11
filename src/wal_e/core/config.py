"""Configuration management for WAL-E assessment tool."""

from __future__ import annotations

import configparser
import os
import re
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal


# Cloud provider detection patterns based on Databricks workspace URLs
_CLOUD_PATTERNS: list[tuple[str, str]] = [
    # Azure: *.azuredatabricks.net
    (r"\.azuredatabricks\.net", "azure"),
    # GCP: *.gcp.databricks.com
    (r"\.gcp\.databricks\.com", "gcp"),
    # AWS: *.cloud.databricks.com or any other *.databricks.com
    (r"\.cloud\.databricks\.com", "aws"),
    (r"\.databricks\.com", "aws"),  # fallback for AWS
]


def detect_cloud_provider(host: str) -> str:
    """
    Detect cloud provider from Databricks workspace URL.

    Returns:
        "aws", "azure", "gcp", or "unknown"
    """
    if not host:
        return "unknown"
    host_lower = host.lower().rstrip("/")
    for pattern, cloud in _CLOUD_PATTERNS:
        if re.search(pattern, host_lower):
            return cloud
    return "unknown"


# Human-friendly names
CLOUD_DISPLAY_NAMES: dict[str, str] = {
    "aws": "Amazon Web Services (AWS)",
    "azure": "Microsoft Azure",
    "gcp": "Google Cloud Platform (GCP)",
    "unknown": "Unknown Cloud",
}


@dataclass
class WalEConfig:
    """Configuration for WAL-E assessment runs."""

    profile_name: str = "DEFAULT"
    workspace_host: str = ""
    token: str = ""
    output_dir: str = "./assessment-results"
    cloud_provider: str = ""  # auto-detected: "aws", "azure", "gcp", "unknown"
    formats: list[Literal["md", "csv", "html", "pptx", "audit"]] = field(
        default_factory=lambda: ["md", "audit"]
    )

    def __post_init__(self) -> None:
        """Load profile from ~/.databrickscfg if host/token not set."""
        if not self.workspace_host or not self.token:
            self._load_from_cli_config()
        # Auto-detect cloud provider from workspace host
        if not self.cloud_provider:
            self.cloud_provider = detect_cloud_provider(self.workspace_host)

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
