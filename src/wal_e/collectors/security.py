"""Security settings and configuration data collector."""

from __future__ import annotations

from typing import Any

from wal_e.collectors.base import BaseCollector


class SecurityCollector(BaseCollector):
    """Collects security and access control configuration."""

    def collect(self) -> dict[str, Any]:
        """Collect workspace-conf, IP access lists, token info."""
        findings: dict[str, Any] = {
            "security_settings": {},
            "ip_access_list_count": 0,
            "token_info": {},
        }

        # Workspace conf - returns key-value pairs
        data, ok = self.run_api_call("/api/2.0/workspace-conf")
        if ok and data:
            keys = [
                "enableDbfsFileBrowser",
                "enableResultsDownloading",
                "enableExportNotebook",
                "maxTokenLifetimeDays",
                "enableIpAccessLists",
            ]
            for k in keys:
                if k in data:
                    findings["security_settings"][k] = data[k]

        # IP access lists
        data, ok = self.run_api_call("/api/2.0/ip-access-lists/list")
        if ok and data:
            lists_data = data.get("ip_access_lists", []) or data.get("ip_access_lists", [])
            findings["ip_access_list_count"] = len(lists_data) if isinstance(lists_data, list) else 0

        # Token list (current user's tokens - limited info for security)
        data, ok = self.run_api_call("/api/2.0/token/list")
        if ok and data:
            tokens = data.get("token_infos", []) or data.get("tokens", []) or []
            findings["token_info"] = {
                "count": len(tokens) if isinstance(tokens, list) else 0,
                "token_ids": [
                    t.get("token_id") or t.get("id")
                    for t in (tokens if isinstance(tokens, list) else [])
                    if isinstance(t, dict)
                ][:10],
            }

        return findings
