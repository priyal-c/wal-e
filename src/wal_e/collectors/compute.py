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
            "clusters": [],
            "warehouse_count": 0,
            "warehouses": [],
            "policy_count": 0,
            "policy_names": [],
            "pool_count": 0,
            "pools": [],
        }

        # Clusters list
        data, ok = self.run_api_call("/api/2.1/clusters/list")
        if ok and data:
            clusters = data.get("clusters", []) or []
            findings["cluster_count"] = len(clusters)
            findings["running_clusters"] = sum(
                1 for c in clusters
                if isinstance(c, dict) and c.get("state") == "RUNNING"
            )
            for c in clusters:
                if isinstance(c, dict):
                    findings["clusters"].append({
                        "cluster_name": c.get("cluster_name"),
                        "cluster_id": c.get("cluster_id"),
                        "state": c.get("state"),
                        "autoscale": c.get("autoscale"),
                        "num_workers": c.get("num_workers"),
                        "node_type_id": c.get("node_type_id"),
                        "driver_node_type_id": c.get("driver_node_type_id"),
                        "runtime_engine": c.get("runtime_engine"),
                        "spark_version": c.get("spark_version"),
                        "auto_termination_minutes": c.get("auto_termination_minutes"),
                        "policy_id": c.get("policy_id"),
                        "custom_tags": c.get("custom_tags"),
                        "cluster_source": c.get("cluster_source"),
                    })

        # SQL warehouses
        data, ok = self.run_api_call("/api/2.0/sql/warehouses")
        if ok and data:
            warehouses = data.get("warehouses", []) or []
            findings["warehouse_count"] = len(warehouses)
            for w in warehouses:
                if isinstance(w, dict):
                    findings["warehouses"].append({
                        "name": w.get("name"),
                        "id": w.get("id"),
                        "size": w.get("size"),
                        "state": w.get("state"),
                        "warehouse_type": w.get("warehouse_type"),
                        "enable_serverless_compute": w.get("enable_serverless_compute"),
                        "auto_stop_mins": w.get("auto_stop_mins"),
                        "max_num_clusters": w.get("max_num_clusters"),
                        "min_num_clusters": w.get("min_num_clusters"),
                        "cluster_size": w.get("cluster_size"),
                        "enable_photon": w.get("enable_photon"),
                        "channel": w.get("channel"),
                    })

        # Cluster policies
        data, ok = self.run_api_call("/api/2.0/cluster-policies/list")
        if ok and data:
            policies = data.get("policies", []) or []
            findings["policy_count"] = len(policies)
            findings["policy_names"] = [
                p.get("name")
                for p in policies
                if isinstance(p, dict)
            ]

        # Instance pools
        data, ok = self.run_api_call("/api/2.0/instance-pools/list")
        if ok and data:
            pools = data.get("instance_pools", []) or []
            findings["pool_count"] = len(pools)
            findings["pools"] = [
                {"name": p.get("instance_pool_name", p.get("name")), "node_type_id": p.get("node_type_id")}
                for p in pools
                if isinstance(p, dict)
            ]

        return findings
