"""Compute (clusters, warehouses, policies) data collector."""

from __future__ import annotations

from typing import Any

from wal_e.collectors.base import BaseCollector


class ComputeCollector(BaseCollector):
    """Collects compute-related configuration."""

    def collect(self) -> dict[str, Any]:
        """Collect clusters, SQL warehouses, cluster policies, instance pools."""
        findings: dict[str, Any] = {
            "running_clusters": 0,
            "cluster_count": 0,
            "cluster_names": [],
            "warehouse_configs": [],
            "warehouse_count": 0,
            "policy_count": 0,
            "pool_count": 0,
        }

        # Clusters list
        data, ok = self.run_api_call("/api/2.1/clusters/list")
        if ok and data:
            clusters = data.get("clusters", []) or []
            findings["cluster_count"] = len(clusters)
            findings["cluster_names"] = [
                c.get("cluster_name", c.get("cluster_id", ""))
                for c in clusters
                if isinstance(c, dict)
            ]
            findings["running_clusters"] = sum(
                1 for c in clusters
                if isinstance(c, dict) and c.get("state") == "RUNNING"
            )

        # SQL warehouses
        data, ok = self.run_api_call("/api/2.0/sql/warehouses")
        if ok and data:
            warehouses = data.get("warehouses", []) or []
            findings["warehouse_count"] = len(warehouses)
            for w in warehouses:
                if isinstance(w, dict):
                    findings["warehouse_configs"].append({
                        "name": w.get("name"),
                        "size": w.get("size"),
                        "state": w.get("state"),
                        "cluster_size": w.get("cluster_size"),
                    })

        # Cluster policies
        data, ok = self.run_api_call("/api/2.0/cluster-policies/list")
        if ok and data:
            policies = data.get("policies", []) or []
            findings["policy_count"] = len(policies)

        # Instance pools
        data, ok = self.run_api_call("/api/2.0/instance-pools/list")
        if ok and data:
            pools = data.get("instance_pools", []) or []
            findings["pool_count"] = len(pools)

        return findings
