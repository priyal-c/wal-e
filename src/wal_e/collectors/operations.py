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
            "job_names": [],
            "pipeline_count": 0,
            "pipeline_states": [],
            "endpoint_count": 0,
            "repo_count": 0,
            "init_script_count": 0,
            "init_script_status": [],
            "group_count": 0,
            "scope_count": 0,
        }

        # Jobs list
        data, ok = self.run_api_call("/api/2.1/jobs/list")
        if ok and data:
            jobs = data.get("jobs", []) or []
            findings["job_count"] = len(jobs)
            findings["job_names"] = [
                j.get("settings", {}).get("name", j.get("job_name", j.get("name", "")))
                for j in jobs
                if isinstance(j, dict)
            ][:50]

        # Pipelines
        data, ok = self.run_api_call("/api/2.0/pipelines")
        if ok and data:
            pipelines = data.get("statuses", []) or data.get("pipelines", []) or []
            findings["pipeline_count"] = len(pipelines)
            for p in pipelines:
                if isinstance(p, dict):
                    state = p.get("state") or p.get("pipeline_id")
                    if state:
                        findings["pipeline_states"].append({
                            "id": p.get("pipeline_id"),
                            "name": p.get("name"),
                            "state": state,
                        })

        # Serving endpoints
        data, ok = self.run_api_call("/api/2.0/serving-endpoints")
        if ok and data:
            endpoints = data.get("endpoints", []) or []
            findings["endpoint_count"] = len(endpoints)

        # Repos
        data, ok = self.run_api_call("/api/2.0/repos")
        if ok and data:
            repos = data.get("repos", []) or []
            findings["repo_count"] = len(repos)

        # Global init scripts
        data, ok = self.run_api_call("/api/2.0/global-init-scripts/list")
        if ok and data:
            scripts = data.get("scripts", []) or []
            findings["init_script_count"] = len(scripts)
            for s in scripts:
                if isinstance(s, dict):
                    findings["init_script_status"].append({
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
