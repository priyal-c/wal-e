"""AI / GenAI asset collector: model serving, Vector Search, UC models, Genie.

Gathers Mosaic AI signals needed to score GenAI-era best practices:
serving endpoint configuration (AI Gateway, guardrails, inference tables,
provisioned throughput), Vector Search endpoints/indexes, registered models
in Unity Catalog vs the legacy workspace registry, and AI/BI Genie spaces.

All access is read-only. Endpoint detail is capped to avoid excessive
per-endpoint calls in workspaces with many serving endpoints.
"""

from __future__ import annotations

from typing import Any

from wal_e.collectors.base import BaseCollector

# Cap per-endpoint detail GETs in standard mode (one list call + N detail calls).
_ENDPOINT_DETAIL_CAP = 50

# Substrings that suggest a production endpoint when no tags are available.
_PROD_MARKERS = ("prod", "production")
_NONPROD_MARKERS = ("dev", "develop", "staging", "stage", "test", "sandbox", "qa")


def _is_llm_entity(entity: dict, endpoint_name: str, task: str) -> bool:
    """Heuristic: does this served entity represent an LLM / foundation model?"""
    if entity.get("external_model"):
        return True
    if entity.get("foundation_model"):
        return True
    if task and "llm/" in task.lower():
        return True
    # Foundation Model API pay-per-token endpoints are named databricks-<model>.
    return endpoint_name.lower().startswith("databricks-")


def _scan_for_plaintext_secret(obj: Any) -> bool:
    """Recursively check whether any *_plaintext key carries a non-empty value."""
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key.endswith("_plaintext") and isinstance(value, str) and value.strip():
                return True
            if _scan_for_plaintext_secret(value):
                return True
    elif isinstance(obj, list):
        return any(_scan_for_plaintext_secret(item) for item in obj)
    return False


def _has_strong_guardrails(guardrails: dict) -> bool:
    """True when PII handling is active and safety is enabled on input or output."""
    if not isinstance(guardrails, dict):
        return False
    for side in ("input", "output"):
        cfg = guardrails.get(side) or {}
        if not isinstance(cfg, dict):
            continue
        pii_behavior = str((cfg.get("pii") or {}).get("behavior", "NONE")).upper()
        if pii_behavior not in ("", "NONE") or cfg.get("safety") is True:
            return True
    return False


def _looks_production(name: str) -> bool:
    """Best-effort prod detection from endpoint name (tags unavailable via list)."""
    lowered = name.lower()
    if any(marker in lowered for marker in _NONPROD_MARKERS):
        return False
    return any(marker in lowered for marker in _PROD_MARKERS)


class AICollector(BaseCollector):
    """Collects Mosaic AI / GenAI assets: serving, Vector Search, UC models, Genie."""

    def collect(self) -> dict[str, Any]:
        findings: dict[str, Any] = {
            "serving_endpoints": [],
            "endpoint_count": 0,
            "llm_endpoint_count": 0,
            "external_model_endpoint_count": 0,
            "endpoints_with_guardrails": 0,
            "endpoints_with_inference_tables": 0,
            "endpoints_with_plaintext_keys": 0,
            "prod_llm_endpoint_count": 0,
            "prod_llm_provisioned_throughput": 0,
            "prod_llm_scale_to_zero": 0,
            "vs_endpoint_count": 0,
            "vs_index_count": 0,
            "vs_delta_sync_indexes": 0,
            "vs_direct_access_indexes": 0,
            "uc_model_count": 0,
            "ws_registry_model_count": 0,
            "genie_space_count": 0,
            "genie_available": False,
        }

        self._collect_serving_endpoints(findings)
        self._collect_vector_search(findings)
        self._collect_models(findings)
        self._collect_genie(findings)

        return findings

    def _collect_serving_endpoints(self, findings: dict[str, Any]) -> None:
        data, ok = self.run_api_call("/api/2.0/serving-endpoints")
        if not (ok and data):
            return
        endpoints = data.get("endpoints", []) or []
        findings["endpoint_count"] = len(endpoints)

        for ep in endpoints[:_ENDPOINT_DETAIL_CAP]:
            if not isinstance(ep, dict):
                continue
            name = ep.get("name", "")
            detail, detail_ok = self.run_api_call(f"/api/2.0/serving-endpoints/{name}")
            ep_data = detail if (detail_ok and detail) else ep
            self._parse_endpoint(name, ep_data, findings)

    def _parse_endpoint(self, name: str, ep: dict, findings: dict[str, Any]) -> None:
        gateway = ep.get("ai_gateway") or {}
        guardrails = gateway.get("guardrails") or ep.get("guardrails") or {}
        inference_cfg = (
            gateway.get("inference_table_config")
            or ep.get("inference_table_config")
            or (ep.get("config") or {}).get("auto_capture_config")
            or {}
        )
        config = ep.get("config") or {}
        served = config.get("served_entities") or ep.get("served_entities") or []

        is_llm = False
        has_pt = False
        has_stz = False
        has_plaintext = False
        for entity in served:
            if not isinstance(entity, dict):
                continue
            task = str(entity.get("task") or (entity.get("external_model") or {}).get("task") or "")
            if _is_llm_entity(entity, name, task):
                is_llm = True
            if entity.get("external_model"):
                findings["external_model_endpoint_count"] += 1
                if _scan_for_plaintext_secret(entity.get("external_model")):
                    has_plaintext = True
            if (entity.get("min_provisioned_throughput") or entity.get("max_provisioned_throughput")
                    or entity.get("provisioned_model_units")):
                has_pt = True
            if entity.get("scale_to_zero_enabled") in (True, "true"):
                has_stz = True

        summary = {
            "name": name,
            "is_llm": is_llm,
            "has_strong_guardrails": _has_strong_guardrails(guardrails),
            "inference_table_enabled": bool(inference_cfg.get("enabled")),
            "has_plaintext_key": has_plaintext,
            "provisioned_throughput": has_pt,
            "scale_to_zero": has_stz,
            "is_production": _looks_production(name),
        }
        findings["serving_endpoints"].append(summary)

        if is_llm:
            findings["llm_endpoint_count"] += 1
            if summary["has_strong_guardrails"]:
                findings["endpoints_with_guardrails"] += 1
            if summary["inference_table_enabled"]:
                findings["endpoints_with_inference_tables"] += 1
            if summary["is_production"]:
                findings["prod_llm_endpoint_count"] += 1
                if has_pt:
                    findings["prod_llm_provisioned_throughput"] += 1
                if has_stz and not has_pt:
                    findings["prod_llm_scale_to_zero"] += 1
        if has_plaintext:
            findings["endpoints_with_plaintext_keys"] += 1

    def _collect_vector_search(self, findings: dict[str, Any]) -> None:
        data, ok = self.run_api_call("/api/2.0/vector-search/endpoints")
        if not (ok and data):
            return
        vs_endpoints = data.get("endpoints", []) or []
        findings["vs_endpoint_count"] = len(vs_endpoints)
        for vs in vs_endpoints[:_ENDPOINT_DETAIL_CAP]:
            if not isinstance(vs, dict):
                continue
            ep_name = vs.get("name", "")
            idx_data, idx_ok = self.run_api_call(
                f"/api/2.0/vector-search/indexes?endpoint_name={ep_name}"
            )
            if not (idx_ok and idx_data):
                continue
            for idx in idx_data.get("vector_indexes", []) or []:
                if not isinstance(idx, dict):
                    continue
                findings["vs_index_count"] += 1
                if str(idx.get("index_type", "")).upper() == "DELTA_SYNC":
                    findings["vs_delta_sync_indexes"] += 1
                elif str(idx.get("index_type", "")).upper() == "DIRECT_ACCESS":
                    findings["vs_direct_access_indexes"] += 1

    def _collect_models(self, findings: dict[str, Any]) -> None:
        data, ok = self.run_api_call("/api/2.1/unity-catalog/models")
        if ok and data:
            findings["uc_model_count"] = len(data.get("registered_models", []) or [])

        data, ok = self.run_api_call("/api/2.0/preview/ml/registered-models/search")
        if ok and data:
            findings["ws_registry_model_count"] = len(data.get("registered_models", []) or [])

    def _collect_genie(self, findings: dict[str, Any]) -> None:
        data, ok = self.run_api_call("/api/2.0/genie/spaces")
        if ok and data:
            findings["genie_available"] = True
            findings["genie_space_count"] = len(data.get("spaces", []) or [])
