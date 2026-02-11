"""Security settings and configuration data collector."""

from __future__ import annotations
from typing import Any
from wal_e.collectors.base import BaseCollector


class SecurityCollector(BaseCollector):
    """Collects security and access control configuration."""

    def collect(self) -> dict[str, Any]:
        findings: dict[str, Any] = {
            "security_settings": {},
            "ip_access_list_count": 0,
            "ip_access_lists": [],
            "token_info": {},
        }

        # Workspace conf - MUST pass keys as query params
        conf_keys = "enableDbfsFileBrowser,enableResultsDownloading,enableExportNotebook,maxTokenLifetimeDays,enableIpAccessLists"
        data, ok = self.run_api_call(f"/api/2.0/workspace-conf?keys={conf_keys}")
        if ok and data and isinstance(data, dict):
            findings["security_settings"] = data

        # IP access lists
        data, ok = self.run_api_call("/api/2.0/ip-access-lists")
        if ok and data:
            lists_data = data.get("ip_access_lists", []) or []
            findings["ip_access_list_count"] = len(lists_data) if isinstance(lists_data, list) else 0
            findings["ip_access_lists"] = [
                {"label": l.get("label"), "list_type": l.get("list_type"), "enabled": l.get("enabled")}
                for l in (lists_data if isinstance(lists_data, list) else [])
                if isinstance(l, dict)
            ]

        # Token list
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
