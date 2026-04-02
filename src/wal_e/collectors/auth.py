"""Authentication and identity data collector."""

from __future__ import annotations

import json
from typing import Any

from wal_e.collectors.base import BaseCollector


class AuthCollector(BaseCollector):
    """Collects authentication and identity information."""

    def _parse_auth_type(self, auth_output: str) -> str:
        """Extract the authentication type from `databricks auth describe` output."""
        for line in auth_output.splitlines():
            stripped = line.strip().lower()
            if stripped.startswith("auth type:") or stripped.startswith("authentication type:"):
                value = line.split(":", 1)[-1].strip()
                if "oauth" in value.lower() or "u2m" in value.lower():
                    return "oauth-u2m"
                if "pat" in value.lower() or "token" in value.lower():
                    return "pat"
                return value
        if "oauth" in auth_output.lower():
            return "oauth-u2m"
        if "pat" in auth_output.lower() or "personal access token" in auth_output.lower():
            return "pat"
        return "unknown"

    def collect(self) -> dict[str, Any]:
        """Collect auth describe and current-user me."""
        findings: dict[str, Any] = {
            "user_identity": None,
            "groups": [],
            "roles": [],
            "instance_profiles": [],
            "auth_config": {},
            "auth_method": "unknown",
        }

        cmd_auth = [
            "databricks",
            "auth",
            "describe",
            "--profile",
            self.profile_name,
        ]
        output_auth, ok_auth = self.run_cli_command(cmd_auth)
        if ok_auth and output_auth:
            try:
                findings["auth_config"] = {"raw": output_auth}
                findings["auth_method"] = self._parse_auth_type(output_auth)
                if "User:" in output_auth:
                    for line in output_auth.splitlines():
                        if line.strip().startswith("User:"):
                            findings["user_identity"] = line.split(":", 1)[-1].strip()
                            break
            except Exception:
                pass

        # Current user me - returns JSON
        cmd_me = [
            "databricks",
            "current-user",
            "me",
            "--profile",
            self.profile_name,
            "-o",
            "json",
        ]
        output_me, ok_me = self.run_cli_command(cmd_me)
        if ok_me and output_me:
            try:
                data = json.loads(output_me)
                if isinstance(data, dict):
                    findings["user_identity"] = (
                        data.get("userName")
                        or data.get("display_name")
                        or data.get("user_name")
                        or str(data)
                    )
                    findings["groups"] = data.get("groups", []) or data.get("group_names", [])
                    if isinstance(findings["groups"], str):
                        findings["groups"] = [findings["groups"]]
                    findings["roles"] = data.get("roles", []) or data.get("role_names", [])
                    if isinstance(findings["roles"], str):
                        findings["roles"] = [findings["roles"]]
                    instance_profiles = data.get("instance_profiles", []) or data.get("instanceProfiles", [])
                    if instance_profiles:
                        findings["instance_profiles"] = instance_profiles
            except json.JSONDecodeError:
                findings["user_identity"] = output_me.strip() if output_me else None

        return findings
