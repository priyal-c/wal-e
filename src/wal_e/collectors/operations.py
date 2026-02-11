"""Operations (jobs, pipelines, repos, etc.) data collector."""

from __future__ import annotations

from typing import Any

from wal_e.collectors.base import BaseCollector


class OperationsCollector(BaseCollector):
    """Collects operational assets: jobs, pipelines, endpoints, repos, scripts, groups, secrets."""

    def collect(self) -> dict[str, Any]:
        """Collect jobs, pipelines, serving endpoints, repos, init scripts, groups, secret scopes."""
        findings: dict[str, Any] = {
            "job_count": 0,
            "jobs": [],
            "pipeline_count": 0,
            "pipelines": [],
            "endpoint_count": 0,
            "endpoints": [],
            "repo_count": 0,
            "init_script_count": 0,
            "init_scripts": [],
            "group_count": 0,
            "scope_count": 0,
        }

        # Jobs list
        data, ok = self.run_api_call("/api/2.1/jobs/list")
        if ok and data:
            jobs = data.get("jobs", []) or []
            findings["job_count"] = len(jobs)
            for j in jobs:
                if isinstance(j, dict):
                    settings = j.get("settings", {}) or {}
                    job_clusters = settings.get("job_clusters") or []
                    findings["jobs"].append({
                        "job_id": j.get("job_id"),
                        "name": settings.get("name", j.get("job_name", j.get("name", ""))),
                        "job_type": settings.get("job_type") or j.get("job_type"),
                        "has_git_source": bool(settings.get("git_source") or j.get("git_source")),
                        "max_retries": settings.get("max_retries") or j.get("max_retries"),
                        "has_existing_cluster_id": bool(settings.get("existing_cluster_id") or j.get("existing_cluster_id")),
                        "has_job_clusters": bool(job_clusters),
                        # --- NEW: creator for service principal ownership check ---
                        "creator_user_name": j.get("creator_user_name", ""),
                    })

        # Pipelines
        data, ok = self.run_api_call("/api/2.0/pipelines")
        if ok and data:
            pipelines = data.get("statuses", []) or data.get("pipelines", []) or []
            findings["pipeline_count"] = len(pipelines)
            for p in pipelines:
                if isinstance(p, dict):
                    findings["pipelines"].append({
                        "pipeline_id": p.get("pipeline_id"),
                        "name": p.get("name"),
                        "state": p.get("state"),
                        "creator_user_name": p.get("creator_user_name"),
                    })

        # Serving endpoints
        data, ok = self.run_api_call("/api/2.0/serving-endpoints")
        if ok and data:
            endpoints = data.get("endpoints", []) or []
            findings["endpoint_count"] = len(endpoints)
            for e in endpoints:
                if isinstance(e, dict):
                    findings["endpoints"].append({
                        "name": e.get("name"),
                        "state": e.get("state"),
                    })

        # Repos
        data, ok = self.run_api_call("/api/2.0/repos")
        if ok and data:
            repos = data.get("repos", []) or []
            findings["repo_count"] = len(repos)

        # Global init scripts - endpoint is /api/2.0/global-init-scripts (no /list)
        data, ok = self.run_api_call("/api/2.0/global-init-scripts")
        if ok and data:
            scripts = data.get("scripts", []) or []
            findings["init_script_count"] = len(scripts)
            for s in scripts:
                if isinstance(s, dict):
                    findings["init_scripts"].append({
                        "name": s.get("name"),
                        "enabled": s.get("enabled"),
                        "script_id": s.get("script_id"),
                    })

        # Groups
        data, ok = self.run_api_call("/api/2.0/groups/list")
        if ok and data:
            groups = data.get("group_names", []) or data.get("groups", []) or []
            findings["group_count"] = len(groups) if isinstance(groups, list) else 0

        # Secret scopes
        data, ok = self.run_api_call("/api/2.0/secrets/list-scopes")
        if ok and data:
            scopes = data.get("scopes", []) or []
            findings["scope_count"] = len(scopes)

        return findings
