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


AUTH_TYPE_PAT = "pat"
AUTH_TYPE_OAUTH_U2M = "oauth-u2m"
AUTH_TYPE_AUTO = "auto"
VALID_AUTH_TYPES = (AUTH_TYPE_PAT, AUTH_TYPE_OAUTH_U2M, AUTH_TYPE_AUTO)

AUTH_DISPLAY_NAMES: dict[str, str] = {
    AUTH_TYPE_PAT: "Personal Access Token (PAT)",
    AUTH_TYPE_OAUTH_U2M: "OAuth User-to-Machine (U2M)",
    AUTH_TYPE_AUTO: "Auto-detect",
}


@dataclass
class WalEConfig:
    """Configuration for WAL-E assessment runs."""

    profile_name: str = "DEFAULT"
    workspace_host: str = ""
    token: str = ""
    auth_type: str = AUTH_TYPE_AUTO
    output_dir: str = "./assessment-results"
    cloud_provider: str = ""  # auto-detected: "aws", "azure", "gcp", "unknown"
    deep_scan: bool = False  # --deep: include system tables queries
    warehouse_id: str = ""  # SQL warehouse ID for system table queries
    formats: list[Literal["md", "csv", "html", "pptx", "audit"]] = field(
        default_factory=lambda: ["md", "audit"]
    )

    def __post_init__(self) -> None:
        """Load profile from ~/.databrickscfg if host/token not set."""
        if not self.workspace_host or (self.auth_type != AUTH_TYPE_OAUTH_U2M and not self.token):
            self._load_from_cli_config()
        if not self.cloud_provider:
            self.cloud_provider = detect_cloud_provider(self.workspace_host)

    def _get_config_path(self) -> Path:
        """Get path to Databricks CLI config file."""
        return Path(
            os.environ.get("DATABRICKS_CONFIG_FILE", Path.home() / ".databrickscfg")
        )

    def _load_from_cli_config(self) -> None:
        """Load workspace host and auth details from ~/.databrickscfg."""
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

        if self.auth_type == AUTH_TYPE_AUTO:
            self.auth_type = self._detect_auth_type(section)

    def _detect_auth_type(self, section: configparser.SectionProxy) -> str:
        """Detect auth type from a profile's config section."""
        if section.get("token"):
            return AUTH_TYPE_PAT
        # OAuth U2M profiles are set up via `databricks auth login` and
        # may not have any credential keys in the file — the CLI stores
        # the OAuth session in its internal cache.  When `host` exists
        # but `token` doesn't, treat it as OAuth U2M.
        if section.get("host") and not section.get("token"):
            return AUTH_TYPE_OAUTH_U2M
        return AUTH_TYPE_AUTO

    def _validate_cli_auth(self) -> tuple[bool, str]:
        """Use `databricks auth describe` to verify the CLI can authenticate."""
        try:
            result = subprocess.run(
                ["databricks", "auth", "describe", "--profile", self.profile_name],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode == 0 and result.stdout:
                return True, result.stdout.strip()
            stderr = result.stderr.strip() if result.stderr else "Unknown error"
            return False, stderr
        except FileNotFoundError:
            return False, "Databricks CLI not found. Install it: pip install databricks-cli"
        except subprocess.TimeoutExpired:
            return False, "Auth check timed out."
        except Exception as e:
            return False, str(e)

    def validate(self) -> tuple[bool, str]:
        """
        Validate configuration and check connectivity to the workspace.
        Supports PAT profiles (token in config) and OAuth U2M (CLI-managed session).

        Returns:
            Tuple of (success: bool, message: str)
        """
        if not self.workspace_host:
            return False, "Workspace host not configured. Run 'databricks configure' or set host in config."

        if self.auth_type == AUTH_TYPE_PAT and not self.token:
            return False, (
                "Authentication token not configured.\n"
                "  Option A (PAT): databricks configure --token --profile <name>\n"
                "  Option B (OAuth): databricks auth login --host <workspace-url>"
            )

        if self.auth_type == AUTH_TYPE_OAUTH_U2M:
            ok, detail = self._validate_cli_auth()
            if not ok:
                return False, (
                    f"OAuth session not valid: {detail}\n"
                    "  Re-authenticate: databricks auth login --host <workspace-url>"
                )

        try:
            result = subprocess.run(
                ["databricks", "api", "get", "/api/2.1/clusters/list", "--profile", self.profile_name],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode == 0:
                auth_label = AUTH_DISPLAY_NAMES.get(self.auth_type, self.auth_type)
                return True, f"Connection validated successfully ({auth_label})."
            stderr = result.stderr.strip() if result.stderr else "Unknown error"
            return False, f"Connection failed: {stderr}"
        except FileNotFoundError:
            return False, "Databricks CLI not found. Install it: pip install databricks-cli"
        except subprocess.TimeoutExpired:
            return False, "Connection timed out. Check network and workspace URL."
        except Exception as e:
            return False, f"Validation error: {e}"
