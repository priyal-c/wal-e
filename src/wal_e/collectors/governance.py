"""Unity Catalog governance data collector."""

from __future__ import annotations

from typing import Any

from wal_e.collectors.base import BaseCollector


class GovernanceCollector(BaseCollector):
    """Collects Unity Catalog governance metadata."""

    def collect(self) -> dict[str, Any]:
        """Collect metastore, catalogs, external locations, storage credentials."""
        findings: dict[str, Any] = {
            "catalog_count": 0,
            "catalogs": [],
            "isolation_modes": [],
            "owners": [],
            "metastore_summary": {},
            "external_location_count": 0,
            "storage_credential_count": 0,
        }

        # Metastore summary
        data, ok = self.run_api_call("/api/2.1/unity-catalog/metastore_summary")
        if ok and data:
            findings["metastore_summary"] = data
            if "name" in data:
                findings["metastore_name"] = data["name"]

        # Catalogs list
        data, ok = self.run_api_call("/api/2.1/unity-catalog/catalogs")
        if ok and data:
            catalogs = data.get("catalogs", []) or []
            findings["catalog_count"] = len(catalogs)
            findings["catalogs"] = [c.get("name", "") for c in catalogs if isinstance(c, dict)]
            for c in catalogs:
                if isinstance(c, dict):
                    mode = c.get("isolation_mode") or c.get("properties", {}).get("isolation_mode")
                    if mode and mode not in findings["isolation_modes"]:
                        findings["isolation_modes"].append(mode)
                    owner = c.get("owner") or c.get("owner_id")
                    if owner and owner not in findings["owners"]:
                        findings["owners"].append(owner)

        # External locations
        data, ok = self.run_api_call("/api/2.1/unity-catalog/external-locations")
        if ok and data:
            locs = data.get("external_locations", []) or []
            findings["external_location_count"] = len(locs)

        # Storage credentials
        data, ok = self.run_api_call("/api/2.1/unity-catalog/storage-credentials")
        if ok and data:
            creds = data.get("storage_credentials", []) or []
            findings["storage_credential_count"] = len(creds)

        return findings
