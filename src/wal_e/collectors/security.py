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
            # --- NEW fields for expanded best practices ---
            "service_principal_count": 0,
            "service_principals": [],
            "scim_groups": [],
            "scim_group_count": 0,
            "scim_groups_with_external_id": 0,
            "scim_user_count": 0,
            "scim_users_with_external_id": 0,
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

        # --- NEW: Service Principals (SCIM API) ---
        data, ok = self.run_api_call("/api/2.0/preview/scim/v2/ServicePrincipals?count=100")
        if ok and data and isinstance(data, dict):
            resources = data.get("Resources", []) or []
            findings["service_principal_count"] = data.get("totalResults", len(resources))
            findings["service_principals"] = [
                {
                    "displayName": sp.get("displayName", ""),
                    "applicationId": sp.get("applicationId", ""),
                    "active": sp.get("active", True),
                }
                for sp in resources
                if isinstance(sp, dict)
            ][:50]

        # --- SCIM Groups. externalId marks a group as IdP-provisioned, but note
        # that in account-level / identity-federated setups externalId lives on
        # the account object and is frequently NOT returned by this workspace
        # endpoint even when SCIM is fully configured. Treat its absence as
        # "unverifiable", not "not implemented" (see scoring). ---
        data, ok = self.run_api_call(
            "/api/2.0/preview/scim/v2/Groups?attributes=displayName,externalId&count=200"
        )
        if ok and data and isinstance(data, dict):
            resources = data.get("Resources", []) or []
            findings["scim_group_count"] = data.get("totalResults", len(resources))
            groups = [
                {
                    "displayName": g.get("displayName", ""),
                    "externalId": g.get("externalId"),
                    "id": g.get("id"),
                }
                for g in resources
                if isinstance(g, dict)
            ]
            findings["scim_groups"] = groups[:100]
            findings["scim_groups_with_external_id"] = sum(
                1 for g in groups if g.get("externalId")
            )

        # --- SCIM Users. User-level externalId is a corroborating signal for
        # IdP provisioning that often survives when group externalId does not.
        # Only externalId is requested to keep the payload small. ---
        data, ok = self.run_api_call(
            "/api/2.0/preview/scim/v2/Users?attributes=userName,externalId&count=1000"
        )
        if ok and data and isinstance(data, dict):
            resources = data.get("Resources", []) or []
            findings["scim_user_count"] = data.get("totalResults", len(resources))
            findings["scim_users_with_external_id"] = sum(
                1
                for u in resources
                if isinstance(u, dict) and u.get("externalId")
            )

        return findings
