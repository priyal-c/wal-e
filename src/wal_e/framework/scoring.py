"""
WAL-E Scoring Engine: Evaluates collected workspace data against WAL best practices.

Scores each best practice 0-2 based on collected data from Databricks API/CLI.
Returns ScoredAssessment with pillar averages, overall score, and maturity level.

Includes expanded best practices from Databricks cheat sheets and best-practice
articles (docs.databricks.com/aws/en/getting-started/best-practices).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable


def _get(data: dict, *keys: str, default: Any = None) -> Any:
    """Safely navigate nested dict by key path. Returns default if any key missing."""
    current = data
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return current


def _cloud(data: dict) -> str:
    """Return detected cloud provider: 'aws', 'azure', 'gcp', or 'unknown'."""
    return data.get("_cloud_provider", "unknown")


def _flatten_collected(data: dict) -> dict:
    """Flatten collected_data from collectors into a single dict for easy lookup."""
    result: dict[str, Any] = {}
    for collector_name, findings in data.items():
        if isinstance(findings, dict):
            for k, v in findings.items():
                result[f"{collector_name}.{k}"] = v
                result[k] = v  # Also allow direct key access
        result[collector_name] = findings
    return result


# ---------------------------------------------------------------------------
# Data & AI Governance scoring functions
# ---------------------------------------------------------------------------


def _score_gov_001(data: dict) -> tuple[int, str]:
    """Establish governance process."""
    gov = _get(data, "GovernanceCollector") or {}
    catalog_count = gov.get("catalog_count", 0) or 0
    metastore = gov.get("metastore_name") or gov.get("metastore_summary")
    if metastore and catalog_count > 0:
        if catalog_count > 100:
            return 1, f"Unity Catalog in use but {catalog_count} catalogs suggest ad-hoc governance. Formalize process."
        return 2, f"Unity Catalog adopted with {catalog_count} catalogs; governance process evident."
    if metastore:
        return 1, "Metastore exists; define formal governance process."
    return 0, "No governance process detected. Adopt Unity Catalog and define governance."


def _score_gov_002(data: dict) -> tuple[int, str]:
    """Manage metadata in one place."""
    gov = _get(data, "GovernanceCollector") or {}
    metastore = gov.get("metastore_name") or gov.get("metastore_summary")
    if metastore:
        return 2, f"Single metastore '{metastore}'; metadata managed centrally."
    return 0, "No Unity Catalog metastore detected. Configure metastore for centralized metadata."


def _score_gov_003(data: dict) -> tuple[int, str]:
    """Track lineage."""
    gov = _get(data, "GovernanceCollector") or {}
    # UC lineage is auto-enabled when metastore exists
    if gov.get("metastore_name"):
        return 1, "Unity Catalog lineage available but active tracking not verified. Monitor lineage usage."
    return 0, "Lineage not available. Enable Unity Catalog lineage."


def _score_gov_004(data: dict) -> tuple[int, str]:
    """Add descriptions."""
    gov = _get(data, "GovernanceCollector") or {}
    catalog_count = gov.get("catalog_count", 0) or 0
    # We can't verify descriptions via CLI, but catalogs with many items likely lack them
    if catalog_count > 100:
        return 0, f"{catalog_count} catalogs — likely many lack descriptions. Add comments to schemas and tables."
    if catalog_count > 0:
        return 1, "Catalogs present; verify descriptions are applied consistently."
    return 0, "No catalogs. Create catalogs with descriptions."


def _score_gov_005(data: dict) -> tuple[int, str]:
    """Allow discovery."""
    gov = _get(data, "GovernanceCollector") or {}
    catalog_count = gov.get("catalog_count", 0) or 0
    if catalog_count > 100:
        return 1, f"Unity Catalog available but {catalog_count} catalogs hinder discovery. Consolidate catalogs."
    if catalog_count > 0:
        return 2, "Unity Catalog enables data discovery for consumers."
    return 0, "Data discovery not configured. Enable Unity Catalog."


def _score_gov_006(data: dict) -> tuple[int, str]:
    """Govern AI assets."""
    ops = _get(data, "OperationsCollector") or {}
    endpoints = ops.get("endpoint_count", 0) or 0
    if endpoints > 0:
        return 1, f"{endpoints} serving endpoints — AI assets partially governed. Extend to Model Registry."
    return 0, "No ML serving endpoints. Register models in MLflow Model Registry."


def _score_gov_007(data: dict) -> tuple[int, str]:
    """Centralize access control."""
    gov = _get(data, "GovernanceCollector") or {}
    ms = gov.get("metastore_summary") or {}
    if isinstance(ms, dict):
        owner = str(ms.get("owner", "")).lower()
        if "account users" in owner or "account_users" in owner:
            return 0, f"Metastore owner is '{ms.get('owner')}' — overly permissive. Use dedicated admin group."
        if owner:
            return 2, f"Unity Catalog access control centralized. Metastore owner: {ms.get('owner')}."
    iso = gov.get("isolation_modes", [])
    if isinstance(iso, list) and "OPEN" in iso:
        return 1, "OPEN isolation mode detected on catalogs. Switch to ISOLATED for production."
    elif isinstance(iso, dict) and iso.get("OPEN", 0) > 0:
        return 1, f"{iso.get('OPEN', 0)} catalogs in OPEN isolation. Switch to ISOLATED for production."
    if gov.get("metastore_name"):
        return 1, "Metastore present; verify UC-based isolation and central RBAC."
    return 0, "No centralized access control. Migrate to Unity Catalog."


def _score_gov_008(data: dict) -> tuple[int, str]:
    """Configure audit logging."""
    sec = _get(data, "SecurityCollector") or {}
    sec_settings = sec.get("security_settings", {})
    gov = _get(data, "GovernanceCollector") or {}
    # If we got any data from SecurityCollector, audit logging is at least partially configured
    if sec_settings or sec.get("ip_access_list_count") is not None:
        return 1, "Workspace configuration accessible; audit logging partially verified. Enable system tables for full audit."
    if gov.get("metastore_name"):
        return 1, "Metastore present; verify audit logging via system tables."
    return 0, "Audit logging not configured. Enable system tables and log delivery."


def _score_gov_009(data: dict) -> tuple[int, str]:
    """Audit events."""
    sec = _get(data, "SecurityCollector") or {}
    sec_settings = sec.get("security_settings", {}) or {}
    if sec_settings:
        # Settings are accessible, so audit events are at least partially available
        return 1, "Workspace settings accessible; configure systematic audit event monitoring and alerting via system tables."
    return 0, "No audit events detected. Configure audit log delivery."


def _score_gov_010(data: dict) -> tuple[int, str]:
    """Define DQ standards."""
    ops = _get(data, "OperationsCollector") or {}
    pipeline_count = ops.get("pipeline_count", 0) or 0
    if pipeline_count > 0:
        return 1, f"{pipeline_count} DLT pipelines present — verify DQ expectations are defined."
    return 0, "No DQ standards detected. Define DLT expectations and data quality rules."


def _score_gov_011(data: dict) -> tuple[int, str]:
    """Use DQ tools."""
    ops = _get(data, "OperationsCollector") or {}
    pipeline_count = ops.get("pipeline_count", 0) or 0
    if pipeline_count > 5:
        return 1, f"{pipeline_count} DLT pipelines — verify expectations are actively used."
    if pipeline_count > 0:
        return 1, "Some DLT pipelines; expand data quality checks."
    return 0, "No DQ tools detected. Use Delta Live Tables expectations or Quality Monitors."


def _score_gov_012(data: dict) -> tuple[int, str]:
    """Enforce standardized formats."""
    gov = _get(data, "GovernanceCollector") or {}
    # If Unity Catalog is in use, Delta Lake is the default format
    if gov.get("metastore_name") and (gov.get("catalog_count", 0) or 0) > 0:
        return 1, "Unity Catalog + Delta Lake in use but cannot verify all tables use Delta format from API."
    return 0, "Standardized formats not verified. Use Delta Lake as default format."


def _score_gov_013(data: dict) -> tuple[int, str]:
    """Account-level group management (Admin Cheat Sheet + UC Best Practices)."""
    sec = _get(data, "SecurityCollector") or {}
    scim_groups = sec.get("scim_groups", []) or []
    scim_count = sec.get("scim_group_count", 0) or len(scim_groups)
    # Groups with externalId are IdP-synced (account-level)
    idp_synced = sum(1 for g in scim_groups if isinstance(g, dict) and g.get("externalId"))
    if idp_synced > 0:
        return 2, f"{idp_synced}/{scim_count} groups IdP-synced via SCIM. Account-level group management in place."
    if scim_count > 0:
        return 1, f"{scim_count} groups exist but none are IdP-synced. Sync groups from your identity provider via SCIM."
    return 0, "No SCIM groups detected. Use account-level groups synced from your IdP."


def _score_gov_014(data: dict) -> tuple[int, str]:
    """Prefer managed tables (UC Best Practices)."""
    gov = _get(data, "GovernanceCollector") or {}
    ext_loc = gov.get("external_location_count", 0) or 0
    catalog_count = gov.get("catalog_count", 0) or 0
    if catalog_count > 0 and ext_loc == 0:
        return 2, "Unity Catalog in use with no external locations; likely using managed tables."
    if catalog_count > 0 and ext_loc > 0:
        ratio = ext_loc / max(catalog_count, 1)
        if ratio > 0.5:
            return 0, f"{ext_loc} external locations vs {catalog_count} catalogs. Migrate external tables to managed tables."
        return 1, f"Some external locations ({ext_loc}). Prefer UC managed tables for new tables."
    return 0, "Unity Catalog not detected. Use UC managed tables for full governance."


def _score_gov_015(data: dict) -> tuple[int, str]:
    """BROWSE privilege for discovery (UC Best Practices)."""
    gov = _get(data, "GovernanceCollector") or {}
    if gov.get("metastore_name") and (gov.get("catalog_count", 0) or 0) > 0:
        return 1, "Unity Catalog in use. Grant BROWSE privilege on catalogs to All account users for data discovery."
    return 0, "Unity Catalog not detected. BROWSE privilege requires UC. Enable UC first."


# ---------------------------------------------------------------------------
# Interoperability & Usability scoring functions
# ---------------------------------------------------------------------------


def _score_int_001(data: dict) -> tuple[int, str]:
    """Standard integration patterns."""
    ops = _get(data, "OperationsCollector") or {}
    job_count = ops.get("job_count", 0) or 0
    jobs = ops.get("jobs", []) or []
    job_names = [j.get("name", "") for j in jobs if isinstance(j, dict)] if jobs else ops.get("job_names", []) or []
    dab_jobs = [j for j in job_names if "[DAB]" in j or "[dev " in j] if job_names else []
    if dab_jobs:
        return 1, f"{len(dab_jobs)} DAB-based jobs detected; expand to standardize integration patterns."
    if job_count > 0:
        return 1, "Jobs present but no standard integration patterns detected."
    return 0, "No standard integration patterns. Use Fivetran, Airbyte, or native connectors."


def _score_int_002(data: dict) -> tuple[int, str]:
    """Optimized connectors."""
    ops = _get(data, "OperationsCollector") or {}
    pipelines = ops.get("pipelines", []) or ops.get("pipeline_states", []) or []
    lakeflow = [p for p in pipelines if isinstance(p, dict) and ("connect" in str(p.get("name", "")).lower() or "gateway" in str(p.get("name", "")).lower())]
    if lakeflow:
        return 1, f"LakeFlow Connect/gateway pipelines detected ({len(lakeflow)}). Verify optimized connectors."
    return 1, "Connector optimization not verified from API. Prefer native Delta connectors."


def _score_int_003(data: dict) -> tuple[int, str]:
    """Certified partner tools."""
    sec = _get(data, "SecurityCollector") or {}
    ipl = sec.get("ip_access_list_count", 0) or 0
    if ipl > 0:
        return 1, "IP access lists suggest external tool integration (e.g. Power BI). Verify certified partners."
    return 1, "Partner tool certification not verified from API."


def _score_int_004(data: dict) -> tuple[int, str]:
    """Reduce pipeline complexity."""
    ops = _get(data, "OperationsCollector") or {}
    pipeline_count = ops.get("pipeline_count", 0) or 0
    pipelines = ops.get("pipelines", []) or ops.get("pipeline_states", []) or []
    failed = sum(1 for p in pipelines if isinstance(p, dict) and p.get("state") == "FAILED")
    if pipeline_count > 0:
        if failed > 0:
            return 1, f"DLT pipelines present ({pipeline_count}) but {failed} in FAILED state."
        return 2, f"{pipeline_count} DLT pipelines reduce complexity."
    return 0, "Pipeline complexity not reduced. Adopt Delta Live Tables."


def _score_int_005(data: dict) -> tuple[int, str]:
    """Use IaC."""
    ops = _get(data, "OperationsCollector") or {}
    jobs = ops.get("jobs", []) or []
    job_names = [j.get("name", "") for j in jobs if isinstance(j, dict)] if jobs else ops.get("job_names", []) or []
    dab = [j for j in (job_names or []) if "[DAB]" in j or "dabs" in j.lower()]
    if dab:
        return 1, f"{len(dab)} DAB-deployed jobs — partial IaC. Expand to Terraform/universal IaC."
    return 1, "IaC usage not detected from API. Document Terraform/CI usage."


def _score_int_006(data: dict) -> tuple[int, str]:
    """Open data formats."""
    gov = _get(data, "GovernanceCollector") or {}
    if gov.get("metastore_name") and (gov.get("catalog_count", 0) or 0) > 0:
        return 2, "Delta Lake (open format) used via Unity Catalog."
    return 0, "Open data formats not detected. Use Delta Lake."


def _score_int_007(data: dict) -> tuple[int, str]:
    """Secure sharing."""
    gov = _get(data, "GovernanceCollector") or {}
    ms = gov.get("metastore_summary", {})
    if isinstance(ms, dict):
        sharing = ms.get("delta_sharing_scope")
        if sharing and "EXTERNAL" in str(sharing):
            return 2, f"Delta Sharing configured ({sharing}) for secure sharing."
        if sharing:
            return 1, f"Delta Sharing scope: {sharing}. Consider enabling external sharing."
    return 1, "Secure sharing not verified. Configure Delta Sharing for cross-org."


def _score_int_008(data: dict) -> tuple[int, str]:
    """Open ML standards."""
    ops = _get(data, "OperationsCollector") or {}
    endpoints = ops.get("endpoint_count", 0) or 0
    if endpoints > 0:
        return 2, f"MLflow/model serving in use ({endpoints} endpoints)."
    return 0, "Open ML standards not detected. Use MLflow."


def _score_int_009(data: dict) -> tuple[int, str]:
    """Self-service."""
    compute = _get(data, "ComputeCollector") or {}
    wh_count = compute.get("warehouse_count", 0) or 0
    if wh_count > 0:
        return 2, f"{wh_count} SQL Warehouses available for self-service analytics."
    return 1, "Self-service SQL not configured. Provision SQL Warehouses."


def _score_int_010(data: dict) -> tuple[int, str]:
    """Serverless compute (Interoperability)."""
    compute = _get(data, "ComputeCollector") or {}
    warehouses = compute.get("warehouses", []) or []
    pro_or_serverless = sum(1 for w in warehouses if isinstance(w, dict) and (
        w.get("warehouse_type") == "PRO" or w.get("enable_serverless_compute") is True
    ))
    if pro_or_serverless > 0:
        return 2, f"{pro_or_serverless} SQL warehouse(s) with Pro or serverless compute."
    warehouse_count = compute.get("warehouse_count", 0) or len(warehouses)
    wh_configs = compute.get("warehouse_configs", [])
    if warehouse_count > 0 or wh_configs:
        return 1, "Warehouses present but Pro/serverless not detected. Use Pro/Serverless warehouses."
    return 0, "No SQL warehouses detected. Use Pro/Serverless warehouses."


def _score_int_011(data: dict) -> tuple[int, str]:
    """Predefined compute templates."""
    compute = _get(data, "ComputeCollector") or {}
    policy_count = compute.get("policy_count", 0) or 0
    if policy_count > 20:
        return 1, f"{policy_count} cluster policies — too many to be standardized templates. Consolidate."
    if policy_count > 0:
        return 2, f"{policy_count} cluster policies defined as compute templates."
    return 0, "No cluster policies. Define compute templates via policies."


def _score_int_012(data: dict) -> tuple[int, str]:
    """AI productivity."""
    return 1, "AI productivity not verifiable from API. Verify assistant enablement."


def _score_int_013(data: dict) -> tuple[int, str]:
    """Reusable data products."""
    gov = _get(data, "GovernanceCollector") or {}
    ms = gov.get("metastore_summary", {})
    if isinstance(ms, dict) and ms.get("delta_sharing_scope"):
        return 1, "Delta Sharing configured. Define reusable data products."
    return 1, "Reusable data products not verified. Define Delta Sharing."


def _score_int_014(data: dict) -> tuple[int, str]:
    """Semantic consistency."""
    return 1, "Semantic consistency not verifiable from API. Use Databricks SQL semantic layer."


def _score_int_015(data: dict) -> tuple[int, str]:
    """UC for discovery."""
    gov = _get(data, "GovernanceCollector") or {}
    if gov.get("metastore_name"):
        return 2, f"Unity Catalog '{gov['metastore_name']}' enables data discovery."
    return 0, "Unity Catalog not detected for discovery."


# ---------------------------------------------------------------------------
# Operational Excellence scoring functions
# ---------------------------------------------------------------------------


def _score_ops_001(data: dict) -> tuple[int, str]:
    """Dedicated ops team."""
    return 1, "Cannot verify org structure from API. Document dedicated ops ownership."


def _score_ops_002(data: dict) -> tuple[int, str]:
    """Enterprise SCM."""
    ops = _get(data, "OperationsCollector") or {}
    repo_count = ops.get("repo_count", 0) or 0
    if repo_count > 0:
        return 2, f"Repos/SCM configured ({repo_count} repos)."
    return 1, "Enterprise SCM not detected. Use Repos for source control."


def _score_ops_003(data: dict) -> tuple[int, str]:
    """Standardize CI/CD."""
    ops = _get(data, "OperationsCollector") or {}
    jobs = ops.get("jobs", []) or []
    git_jobs = sum(1 for j in jobs if isinstance(j, dict) and j.get("has_git_source"))
    job_count = ops.get("job_count", 0) or len(jobs)
    if git_jobs > 0:
        return 2, f"{git_jobs}/{job_count} job(s) use Git source — CI/CD integration in place."
    if job_count > 0:
        return 1, f"{job_count} jobs present but none use Git source. Use Repos and Git-backed job tasks for CI/CD."
    return 0, "No jobs; adopt CI/CD for deployments."


def _score_ops_004(data: dict) -> tuple[int, str]:
    """MLOps processes."""
    ops = _get(data, "OperationsCollector") or {}
    endpoint_count = ops.get("endpoint_count", 0) or 0
    if endpoint_count > 0:
        return 1, f"{endpoint_count} serving endpoints; Model Registry not verifiable from API. Register models in MLflow."
    return 0, "No Model Registry. Adopt MLOps with MLflow."


def _score_ops_005(data: dict) -> tuple[int, str]:
    """Environment isolation."""
    gov = _get(data, "GovernanceCollector") or {}
    catalogs = gov.get("catalogs", [])
    catalog_count = gov.get("catalog_count", 0) or 0
    count = catalog_count if catalog_count > 0 else (len(catalogs) if isinstance(catalogs, list) else 0)
    if count >= 2:
        return 2, "Multiple catalogs; environment isolation via UC."
    if count > 0:
        return 1, "Single catalog; add dev/test/prod catalogs."
    return 0, "Environment isolation not evident. Use catalogs per environment."


def _score_ops_006(data: dict) -> tuple[int, str]:
    """Catalog strategy."""
    gov = _get(data, "GovernanceCollector") or {}
    catalogs = gov.get("catalogs", [])
    catalog_count = gov.get("catalog_count", 0) or 0
    count = catalog_count if catalog_count > 0 else (len(catalogs) if isinstance(catalogs, list) else 0)
    if count >= 2:
        return 2, "Catalog strategy in place with multiple catalogs."
    if count > 0:
        return 1, "Single catalog; define dev/test/prod strategy."
    return 0, "No catalog strategy. Use Unity Catalog layering."


def _score_ops_007(data: dict) -> tuple[int, str]:
    """IaC deployments."""
    return _score_int_005(data)


def _score_ops_008(data: dict) -> tuple[int, str]:
    """Standardize compute."""
    compute = _get(data, "ComputeCollector") or {}
    policy_count = compute.get("policy_count", 0) or 0
    cluster_count = compute.get("cluster_count", 0) or 0
    if policy_count > 0:
        return 2, f"{policy_count} cluster policies defined for standardized compute."
    if cluster_count > 0:
        return 1, "Clusters present but no policies. Define cluster policies."
    return 0, "No cluster policies. Standardize compute via policies."


def _score_ops_009(data: dict) -> tuple[int, str]:
    """Automated workflows."""
    ops = _get(data, "OperationsCollector") or {}
    job_count = ops.get("job_count", 0) or 0
    if job_count >= 3:
        return 2, f"{job_count} automated jobs."
    if job_count > 0:
        return 1, "Some jobs; expand automation."
    return 0, "No automated workflows. Create jobs for pipelines."


def _score_ops_010(data: dict) -> tuple[int, str]:
    """Event-driven ingestion."""
    return 1, "Event-driven ingestion not verifiable from API. Use Auto Loader."


def _score_ops_011(data: dict) -> tuple[int, str]:
    """ETL frameworks."""
    ops = _get(data, "OperationsCollector") or {}
    pipeline_count = ops.get("pipeline_count", 0) or 0
    if pipeline_count > 0:
        return 2, f"Delta Live Tables in use ({pipeline_count} pipelines)."
    return 0, "No DLT pipelines. Adopt Delta Live Tables."


def _score_ops_012(data: dict) -> tuple[int, str]:
    """Deploy-code ML."""
    ops = _get(data, "OperationsCollector") or {}
    endpoint_count = ops.get("endpoint_count", 0) or 0
    if endpoint_count > 0:
        return 2, f"Model serving endpoints in use ({endpoint_count} endpoints)."
    return 1, "Model serving not detected. Use Model Serving for deployment."


def _score_ops_013(data: dict) -> tuple[int, str]:
    """Model registry."""
    return _score_ops_004(data)


def _score_ops_014(data: dict) -> tuple[int, str]:
    """Automate experiment tracking."""
    return 1, "Experiment tracking not verifiable from API. Use MLflow for experiment tracking."


def _score_ops_015(data: dict) -> tuple[int, str]:
    """Reuse ML infra."""
    return 1, "ML infra reuse not verifiable from API. Use job clusters."


def _score_ops_016(data: dict) -> tuple[int, str]:
    """Declarative management."""
    return _score_ops_011(data)


def _score_ops_017(data: dict) -> tuple[int, str]:
    """Service limits."""
    return 1, "Service limits not verifiable from API. Document capacity limits."


def _score_ops_018(data: dict) -> tuple[int, str]:
    """Capacity planning."""
    return 1, "Capacity planning not verifiable from API. Document planning process."


def _score_ops_019(data: dict) -> tuple[int, str]:
    """Monitoring processes."""
    ops = _get(data, "OperationsCollector") or {}
    if ops.get("job_count", 0) or ops.get("pipeline_count", 0):
        return 1, "Jobs/pipelines present; monitoring not fully verifiable from API. Configure alerts and dashboards."
    return 1, "Monitoring not fully verified from API. Configure alerts and dashboards."


def _score_ops_020(data: dict) -> tuple[int, str]:
    """Platform monitoring tools."""
    compute = _get(data, "ComputeCollector") or {}
    warehouse_count = compute.get("warehouse_count", 0) or 0
    if warehouse_count > 0:
        return 2, f"SQL Warehouses enable platform monitoring ({warehouse_count} warehouses)."
    return 1, "Platform monitoring tools not verified. Use Databricks SQL."


def _score_ops_021(data: dict) -> tuple[int, str]:
    """Automated rollbacks (CI/CD Best Practices)."""
    ops = _get(data, "OperationsCollector") or {}
    jobs = ops.get("jobs", []) or []
    git_jobs = sum(1 for j in jobs if isinstance(j, dict) and j.get("has_git_source"))
    if git_jobs > 0:
        return 1, f"{git_jobs} job(s) use Git source. Verify automated rollback mechanisms in CI/CD pipelines."
    return 0, "No Git-backed jobs detected. Implement CI/CD with automated rollback mechanisms."


def _score_ops_022(data: dict) -> tuple[int, str]:
    """Workload identity federation (CI/CD Best Practices)."""
    sec = _get(data, "SecurityCollector") or {}
    sp_count = sec.get("service_principal_count", 0) or 0
    if sp_count > 0:
        return 1, f"{sp_count} service principal(s) exist. Verify workload identity federation for CI/CD auth (eliminates PATs)."
    return 0, "No service principals. Use workload identity federation for secure CI/CD authentication."


def _score_ops_023(data: dict) -> tuple[int, str]:
    """Restart long-running clusters (Jobs Cheat Sheet)."""
    compute = _get(data, "ComputeCollector") or {}
    clusters = compute.get("clusters", []) or []
    running = [c for c in clusters if isinstance(c, dict) and c.get("state") == "RUNNING"]
    no_auto_term = [c for c in running if not c.get("auto_termination_minutes")]
    if no_auto_term:
        return 0, f"{len(no_auto_term)} running cluster(s) without auto-termination. Restart periodically for security patches."
    if running:
        return 1, f"{len(running)} running cluster(s) with auto-termination. Verify periodic restarts for runtime patches."
    return 2, "No long-running clusters detected."


# ---------------------------------------------------------------------------
# Security scoring functions
# ---------------------------------------------------------------------------


def _score_sec_001(data: dict) -> tuple[int, str]:
    """Least privilege IAM."""
    gov = _get(data, "GovernanceCollector") or {}
    if gov.get("metastore_name"):
        return 2, "Unity Catalog enables centralized least-privilege IAM."
    return 1, "Least-privilege IAM not fully verified. Use UC grants."


def _score_sec_002(data: dict) -> tuple[int, str]:
    """Data protection transit/rest - cloud-aware."""
    cloud = _cloud(data)
    if cloud == "aws":
        return 1, "Verify AWS SSE-S3/SSE-KMS encryption at rest and TLS in transit. Consider customer-managed keys (CMK)."
    if cloud == "azure":
        return 1, "Verify Azure SSE with Microsoft-managed or customer-managed keys (CMK). TLS enforced by default."
    if cloud == "gcp":
        return 1, "Verify GCP CMEK encryption at rest and TLS in transit. Consider customer-managed encryption keys."
    return 1, "Encryption not verifiable from API. Ensure encryption at rest and TLS in transit."


def _score_sec_003(data: dict) -> tuple[int, str]:
    """Network security."""
    sec = _get(data, "SecurityCollector") or {}
    ip_lists = sec.get("ip_access_lists", []) or []
    sec_settings = sec.get("security_settings", {}) or {}
    ipl_count = len(ip_lists) if isinstance(ip_lists, list) else (sec.get("ip_access_list_count", 0) or 0)
    enable_ipl = sec_settings.get("enableIpAccessLists", "").lower() == "true"
    if ipl_count > 0 and enable_ipl:
        return 2, f"IP access lists configured and enabled for network security ({ipl_count} lists)."
    if ipl_count > 0:
        return 1, f"IP access lists exist ({ipl_count}) but enableIpAccessLists not enabled. Enable in workspace settings."
    return 1, "IP access lists not detected. Configure network restrictions."


def _score_sec_004(data: dict) -> tuple[int, str]:
    """Shared responsibility."""
    return 1, "Shared responsibility model not verifiable from API. Document cloud security."


def _score_sec_005(data: dict) -> tuple[int, str]:
    """Compliance requirements."""
    sec = _get(data, "SecurityCollector") or {}
    sec_settings = sec.get("security_settings", {}) or {}
    # Check if key compliance controls are in place
    dbfs_off = str(sec_settings.get("enableDbfsFileBrowser", "")).lower() == "false"
    ipl_on = str(sec_settings.get("enableIpAccessLists", "")).lower() == "true"
    if dbfs_off and ipl_on:
        return 2, "Key compliance controls enforced: DBFS browser disabled, IP access lists enabled."
    if sec_settings:
        return 1, "Some workspace security settings configured; review DBFS browser, IP access lists for full compliance."
    return 1, "Compliance not fully verified. Enable audit logging and workspace security settings."


def _score_sec_006(data: dict) -> tuple[int, str]:
    """System security monitoring."""
    return 1, "System tables access not verifiable from API. Enable for security monitoring."


def _score_sec_007(data: dict) -> tuple[int, str]:
    """Generic controls."""
    sec = _get(data, "SecurityCollector") or {}
    sec_settings = sec.get("security_settings", {}) or {}
    dbfs_disabled = sec_settings.get("enableDbfsFileBrowser", "").lower() == "false"
    if dbfs_disabled:
        return 2, "DBFS file browser disabled in workspace settings — generic controls enforced."
    if sec_settings:
        return 1, "Workspace security settings present; consider disabling DBFS file browser for compliance."
    return 1, "Generic controls not fully verified. Use policies and workspace conf."


def _score_sec_008(data: dict) -> tuple[int, str]:
    """SSO configuration (Admin Cheat Sheet) - cloud-aware."""
    cloud = _cloud(data)
    sec = _get(data, "SecurityCollector") or {}
    scim_groups = sec.get("scim_groups", []) or []
    idp_synced = sum(1 for g in scim_groups if isinstance(g, dict) and g.get("externalId"))
    if cloud == "azure":
        idp_name = "Microsoft Entra ID (AAD)"
    elif cloud == "gcp":
        idp_name = "Google Cloud Identity"
    else:
        idp_name = "your identity provider (Okta, AAD, etc.)"
    if idp_synced > 0:
        return 2, f"IdP-synced groups detected ({idp_synced}), indicating SSO is configured via {idp_name}."
    scim_count = sec.get("scim_group_count", 0) or 0
    if scim_count > 0:
        return 1, f"Groups exist but no IdP sync detected. Configure SSO via {idp_name} ({cloud.upper()})."
    return 1, f"SSO not verifiable from workspace API. Set up SSO via {idp_name} ({cloud.upper()})."


def _score_sec_009(data: dict) -> tuple[int, str]:
    """SCIM provisioning (Admin Cheat Sheet + UC Best Practices)."""
    sec = _get(data, "SecurityCollector") or {}
    scim_groups = sec.get("scim_groups", []) or []
    idp_synced = sum(1 for g in scim_groups if isinstance(g, dict) and g.get("externalId"))
    if idp_synced >= 3:
        return 2, f"{idp_synced} IdP-synced SCIM groups. Automated provisioning in place."
    if idp_synced > 0:
        return 1, f"Only {idp_synced} IdP-synced group(s). Expand SCIM provisioning to all groups."
    return 0, "No SCIM-synced groups detected. Set up SCIM provisioning from your identity provider."


def _score_sec_010(data: dict) -> tuple[int, str]:
    """Service principals for automation (Admin Cheat Sheet + Jobs Cheat Sheet)."""
    sec = _get(data, "SecurityCollector") or {}
    sp_count = sec.get("service_principal_count", 0) or 0
    if sp_count >= 3:
        return 2, f"{sp_count} service principals configured for automation."
    if sp_count > 0:
        return 1, f"Only {sp_count} service principal(s). Create more for production jobs and third-party integrations."
    return 0, "No service principals detected. Use service principals for jobs and external tool authentication."


def _score_sec_011(data: dict) -> tuple[int, str]:
    """Customer-managed VPC / VNET / VPC-SC (Admin Cheat Sheet) - cloud-aware."""
    cloud = _cloud(data)
    sec = _get(data, "SecurityCollector") or {}
    sec_settings = sec.get("security_settings", {}) or {}
    ipl_on = str(sec_settings.get("enableIpAccessLists", "")).lower() == "true"
    if cloud == "aws":
        net = "customer-managed VPC with AWS PrivateLink"
    elif cloud == "azure":
        net = "VNET injection with Azure Private Link"
    elif cloud == "gcp":
        net = "customer-managed VPC with Private Service Connect"
    else:
        net = "customer-managed VPC with Private Link"
    if ipl_on:
        return 1, f"IP access lists enabled ({cloud.upper()}). Verify {net} for network-level security."
    return 0, f"No network-level controls detected ({cloud.upper()}). Configure {net}."


def _score_sec_012(data: dict) -> tuple[int, str]:
    """Restrict DBFS root data storage (Jobs Cheat Sheet)."""
    sec = _get(data, "SecurityCollector") or {}
    sec_settings = sec.get("security_settings", {}) or {}
    dbfs_off = str(sec_settings.get("enableDbfsFileBrowser", "")).lower() == "false"
    if dbfs_off:
        return 2, "DBFS file browser disabled — data exfiltration via DBFS root restricted."
    if sec_settings:
        return 0, "DBFS file browser is ENABLED. Disable immediately and migrate data out of DBFS root."
    return 1, "DBFS root access not verifiable. Disable DBFS file browser and avoid storing data in DBFS root."


# ---------------------------------------------------------------------------
# Reliability scoring functions
# ---------------------------------------------------------------------------


def _score_rel_001(data: dict) -> tuple[int, str]:
    """ACID format."""
    return _score_gov_012(data)


def _score_rel_002(data: dict) -> tuple[int, str]:
    """Resilient engine."""
    compute = _get(data, "ComputeCollector") or {}
    clusters = compute.get("clusters", []) or []
    photon_count = sum(1 for c in clusters if isinstance(c, dict) and c.get("runtime_engine") == "PHOTON")
    if photon_count > 0:
        return 2, f"Photon enabled on {photon_count} cluster(s) for better resilience."
    cluster_count = compute.get("cluster_count", 0) or len(clusters)
    if cluster_count > 0:
        return 1, "Photon not detected on clusters. Use Photon for better resilience."
    return 1, "Photon not detected. Use Photon for better resilience."


def _score_rel_003(data: dict) -> tuple[int, str]:
    """Rescue invalid data."""
    ops = _get(data, "OperationsCollector") or {}
    pipeline_count = ops.get("pipeline_count", 0) or 0
    if pipeline_count > 0:
        return 1, "Rescue invalid data pattern not verifiable from pipeline metadata. Use Auto Loader rescued column."
    return 0, "Rescue invalid data not detected. Use Auto Loader rescued column."


def _score_rel_004(data: dict) -> tuple[int, str]:
    """Auto retries."""
    ops = _get(data, "OperationsCollector") or {}
    jobs = ops.get("jobs", []) or []
    with_retries = sum(1 for j in jobs if isinstance(j, dict) and (j.get("max_retries") or 0) > 0)
    if with_retries > 0:
        return 2, f"{with_retries} job(s) configured with retries for resilience."
    job_count = ops.get("job_count", 0) or len(jobs)
    if job_count > 0:
        return 1, "Jobs present but none have max_retries > 0. Configure retries for resilience."
    return 0, "No jobs; add retries when creating jobs."


def _score_rel_005(data: dict) -> tuple[int, str]:
    """Scalable serving."""
    return _score_ops_012(data)


def _score_rel_006(data: dict) -> tuple[int, str]:
    """Managed services."""
    ops = _get(data, "OperationsCollector") or {}
    compute = _get(data, "ComputeCollector") or {}
    job_count = ops.get("job_count", 0) or 0
    pipeline_count = ops.get("pipeline_count", 0) or 0
    warehouse_count = compute.get("warehouse_count", 0) or 0
    total = job_count + pipeline_count + warehouse_count
    if total >= 3:
        return 2, "Managed services (jobs, DLT, SQL) in use."
    if total > 0:
        return 1, "Some managed services; expand usage."
    return 0, "Use managed jobs, DLT, and SQL warehouses."


def _score_rel_007(data: dict) -> tuple[int, str]:
    """Layered storage."""
    gov = _get(data, "GovernanceCollector") or {}
    catalogs = gov.get("catalogs", [])
    catalog_count = gov.get("catalog_count", 0) or 0
    count = catalog_count if catalog_count > 0 else (len(catalogs) if isinstance(catalogs, list) else 0)
    if count >= 2:
        return 1, "Multiple catalogs; schema layering not verifiable from API. Verify bronze/silver/gold."
    if count > 0:
        return 1, "Partial layering; adopt bronze/silver/gold."
    return 0, "No layered storage. Use catalog/schema layering."


def _score_rel_008(data: dict) -> tuple[int, str]:
    """Reduce redundancy."""
    return 1, "Data redundancy not verifiable from API. Document deduplication strategy."


def _score_rel_009(data: dict) -> tuple[int, str]:
    """Active schema mgmt."""
    ops = _get(data, "OperationsCollector") or {}
    if ops.get("pipeline_count", 0) > 0:
        return 1, "DLT pipelines present; schema constraints not verifiable from API. Use Delta constraints."
    return 1, "Schema management not verified. Use Delta constraints."


def _score_rel_010(data: dict) -> tuple[int, str]:
    """Constraints/expectations."""
    return _score_rel_009(data)


def _score_rel_011(data: dict) -> tuple[int, str]:
    """Data-centric ML."""
    return 1, "Feature Store not verifiable from API. Use for data-centric ML."


def _score_rel_012(data: dict) -> tuple[int, str]:
    """ETL autoscaling."""
    ops = _get(data, "OperationsCollector") or {}
    pipelines = ops.get("pipeline_states", []) or ops.get("pipelines", []) or []
    with_autoscale = sum(1 for p in pipelines if isinstance(p, dict) and p.get("autoscale"))
    pipeline_count = ops.get("pipeline_count", 0) or len(pipelines)
    if with_autoscale > 0:
        return 2, f"{with_autoscale} DLT pipeline(s) with autoscaling enabled."
    if pipeline_count > 0:
        return 1, "DLT pipelines present but autoscaling not enabled. Enable on DLT pipelines."
    return 0, "No DLT pipelines. Use DLT with autoscaling."


def _score_rel_013(data: dict) -> tuple[int, str]:
    """SQL warehouse autoscaling."""
    compute = _get(data, "ComputeCollector") or {}
    warehouses = compute.get("warehouses", []) or []
    with_autoscale = sum(1 for w in warehouses if isinstance(w, dict) and (
        (w.get("max_num_clusters") or 0) > 1 or w.get("enable_serverless_compute") is True
    ))
    if with_autoscale > 0:
        return 2, f"{with_autoscale} SQL warehouse(s) with multi-cluster or serverless autoscaling."
    warehouse_count = compute.get("warehouse_count", 0) or len(warehouses)
    wh_configs = compute.get("warehouse_configs", [])
    if warehouse_count > 0 or wh_configs:
        return 1, "Warehouses present but no multi-cluster or serverless. Enable for autoscaling."
    return 0, "No SQL warehouses. Enable multi-cluster or serverless for autoscaling."


def _score_rel_014(data: dict) -> tuple[int, str]:
    """Regular backups."""
    return 1, "Backup strategy not verifiable from API. Document Delta clone/backup process."


def _score_rel_015(data: dict) -> tuple[int, str]:
    """Streaming recovery."""
    ops = _get(data, "OperationsCollector") or {}
    pipeline_count = ops.get("pipeline_count", 0) or 0
    if pipeline_count > 0:
        return 1, "DLT checkpoints used by default; storage config not verifiable from API."
    return 0, "Streaming recovery not verified. Use DLT with checkpoints."


def _score_rel_016(data: dict) -> tuple[int, str]:
    """Time travel recovery."""
    gov = _get(data, "GovernanceCollector") or {}
    if gov.get("metastore_name") and (gov.get("catalog_count", 0) or 0) > 0:
        return 2, "Unity Catalog with Delta Lake enables time travel recovery."
    return 0, "Delta Lake required for time travel. Migrate tables."


def _score_rel_017(data: dict) -> tuple[int, str]:
    """Job automation recovery."""
    return _score_rel_004(data)


def _score_rel_018(data: dict) -> tuple[int, str]:
    """DR pattern."""
    return 1, "DR pattern not verifiable from API. Document replication and RTO/RPO."


def _score_rel_019(data: dict) -> tuple[int, str]:
    """Service principal job ownership (Jobs Cheat Sheet + UC Best Practices)."""
    sec = _get(data, "SecurityCollector") or {}
    ops = _get(data, "OperationsCollector") or {}
    sp_count = sec.get("service_principal_count", 0) or 0
    sp_names = [sp.get("displayName", "").lower() for sp in (sec.get("service_principals", []) or []) if isinstance(sp, dict)]
    jobs = ops.get("jobs", []) or []
    # Check if any job creators look like service principals
    sp_owned = sum(1 for j in jobs if isinstance(j, dict) and any(
        spn in str(j.get("creator_user_name", "")).lower() for spn in sp_names
    )) if sp_names else 0
    job_count = ops.get("job_count", 0) or len(jobs)
    if sp_count > 0 and sp_owned > 0:
        return 2, f"{sp_owned}/{job_count} job(s) owned by service principals for reliability."
    if sp_count > 0:
        return 1, f"Service principals exist but job ownership not verified. Transfer production job ownership to service principals."
    if job_count > 0:
        return 0, "Jobs owned by individual users. Create service principals and transfer production job ownership."
    return 1, "No jobs detected. Use service principals when creating production jobs."


# ---------------------------------------------------------------------------
# Performance scoring functions
# ---------------------------------------------------------------------------


def _score_perf_001(data: dict) -> tuple[int, str]:
    """Scaling."""
    compute = _get(data, "ComputeCollector") or {}
    clusters = compute.get("clusters", []) or []
    with_autoscale = sum(1 for c in clusters if isinstance(c, dict) and c.get("autoscale"))
    if with_autoscale > 0:
        return 2, f"{with_autoscale} cluster(s) with autoscaling enabled."
    cluster_count = compute.get("cluster_count", 0) or len(clusters)
    if cluster_count > 0:
        return 1, "Clusters present but autoscaling not enabled. Enable when provisioning."
    return 0, "No clusters. Configure autoscaling when adding compute."


def _score_perf_002(data: dict) -> tuple[int, str]:
    """Serverless (Performance)."""
    return _score_int_010(data)


def _score_perf_003(data: dict) -> tuple[int, str]:
    """Data patterns."""
    return 1, "Data optimization (OPTIMIZE/Z-ORDER) not verifiable from API. Use OPTIMIZE and Z-ORDER."


def _score_perf_004(data: dict) -> tuple[int, str]:
    """Parallel computation."""
    compute = _get(data, "ComputeCollector") or {}
    clusters = compute.get("clusters", []) or []
    total_workers = sum(c.get("num_workers", 0) or 0 for c in clusters if isinstance(c, dict))
    if total_workers > 0:
        return 2, f"Total {total_workers} workers across clusters — parallelism utilized."
    cluster_count = compute.get("cluster_count", 0) or len(clusters)
    if cluster_count > 0:
        return 1, "Clusters present but worker count not available. Scale workers for throughput."
    return 1, "Parallelism not fully utilized. Scale workers for throughput."


def _score_perf_005(data: dict) -> tuple[int, str]:
    """Execution chain."""
    return _score_ops_011(data)


def _score_perf_006(data: dict) -> tuple[int, str]:
    """Larger clusters."""
    compute = _get(data, "ComputeCollector") or {}
    clusters = compute.get("clusters", []) or []
    max_workers = max(((c.get("autoscale") or {}).get("max_workers") or c.get("num_workers") or 0 for c in clusters if isinstance(c, dict)), default=0)
    if max_workers > 0:
        return 2, f"Clusters sized for scale (max {max_workers} workers). Right-size for workload."
    cluster_count = compute.get("cluster_count", 0) or len(clusters)
    if cluster_count > 0:
        return 1, "Clusters present but size not available. Right-size for workload-appropriate scale."
    return 0, "No clusters. Right-size when provisioning."


def _score_perf_007(data: dict) -> tuple[int, str]:
    """Native Spark."""
    return _score_rel_002(data)


def _score_perf_008(data: dict) -> tuple[int, str]:
    """Native engines."""
    return _score_rel_002(data)


def _score_perf_009(data: dict) -> tuple[int, str]:
    """Hardware awareness."""
    compute = _get(data, "ComputeCollector") or {}
    clusters = compute.get("clusters", []) or []
    if clusters:
        types = set(c.get("node_type_id", "") for c in clusters if isinstance(c, dict) and c.get("node_type_id"))
        if len(types) > 1:
            return 2, f"Multiple instance types in use ({len(types)} types) — workload-aware selection."
        if types:
            return 1, f"Single instance type ({types.pop()}) across clusters. Consider workload-specific types."
    return 1, "Hardware awareness not verified. Match instance types to workload."


def _score_perf_010(data: dict) -> tuple[int, str]:
    """Caching."""
    return 1, "Caching not verifiable from API. Use Delta Cache for repeated reads."


def _score_perf_011(data: dict) -> tuple[int, str]:
    """Compaction."""
    return _score_perf_003(data)


def _score_perf_012(data: dict) -> tuple[int, str]:
    """Data skipping."""
    return _score_perf_003(data)


def _score_perf_013(data: dict) -> tuple[int, str]:
    """Avoid over-partition."""
    return 1, "Partitioning strategy not verifiable from API. Avoid over-partitioning."


def _score_perf_014(data: dict) -> tuple[int, str]:
    """Join optimization."""
    return 1, "Join optimization (AQE) enabled by default. Verify skew handling."


def _score_perf_015(data: dict) -> tuple[int, str]:
    """Table statistics."""
    return 1, "Table statistics not verifiable from API. Run ANALYZE TABLE."


def _score_perf_016(data: dict) -> tuple[int, str]:
    """Test on production data."""
    return 1, "Testing strategy not verifiable from API. Use clone for testing."


def _score_perf_017(data: dict) -> tuple[int, str]:
    """Prewarming."""
    compute = _get(data, "ComputeCollector") or {}
    warehouses = compute.get("warehouses", []) or []
    photon_wh = sum(1 for w in warehouses if isinstance(w, dict) and w.get("enable_photon") is True)
    if photon_wh > 0:
        return 2, f"{photon_wh} SQL warehouse(s) with Photon enabled for latency-sensitive workloads."
    warehouse_count = compute.get("warehouse_count", 0) or len(warehouses)
    wh_configs = compute.get("warehouse_configs", [])
    if warehouse_count > 0 or wh_configs:
        return 1, "Warehouses present but Photon not enabled. Enable for latency-sensitive workloads."
    return 1, "Prewarm not verified. Use for latency-sensitive workloads."


def _score_perf_018(data: dict) -> tuple[int, str]:
    """Identify bottlenecks."""
    compute = _get(data, "ComputeCollector") or {}
    warehouse_count = compute.get("warehouse_count", 0) or 0
    if warehouse_count > 0:
        return 2, "SQL Warehouses enable query history for bottleneck identification."
    return 1, "Use query history and Spark UI for bottleneck analysis."


def _score_perf_019(data: dict) -> tuple[int, str]:
    """Monitor queries."""
    compute = _get(data, "ComputeCollector") or {}
    warehouse_count = compute.get("warehouse_count", 0) or 0
    if warehouse_count > 0:
        return 2, f"SQL Warehouses enable query monitoring ({warehouse_count} warehouses)."
    return 1, "Enable SQL Warehouses for query monitoring."


def _score_perf_020(data: dict) -> tuple[int, str]:
    """Monitor streaming."""
    ops = _get(data, "OperationsCollector") or {}
    pipeline_count = ops.get("pipeline_count", 0) or 0
    if pipeline_count > 0:
        return 2, f"DLT pipelines enable streaming monitoring ({pipeline_count} pipelines)."
    return 0, "No streaming pipelines. Use DLT for monitoring."


def _score_perf_021(data: dict) -> tuple[int, str]:
    """Monitor jobs."""
    ops = _get(data, "OperationsCollector") or {}
    job_count = ops.get("job_count", 0) or 0
    if job_count > 0:
        return 2, f"Jobs enable cluster/job monitoring ({job_count} jobs)."
    return 0, "No jobs. Create jobs for operational visibility."


def _score_perf_022(data: dict) -> tuple[int, str]:
    """Predictive optimization (Delta Lake Best Practices)."""
    gov = _get(data, "GovernanceCollector") or {}
    if gov.get("metastore_name") and (gov.get("catalog_count", 0) or 0) > 0:
        return 1, "Unity Catalog in use. Enable predictive optimization for auto OPTIMIZE/VACUUM on managed tables."
    return 0, "Unity Catalog not detected. Enable UC managed tables and then predictive optimization."


def _score_perf_023(data: dict) -> tuple[int, str]:
    """Liquid clustering (Delta Lake Best Practices)."""
    gov = _get(data, "GovernanceCollector") or {}
    if gov.get("metastore_name"):
        return 1, "Unity Catalog in use. Migrate from Z-ORDER/partitioning to liquid clustering for better performance."
    return 0, "Liquid clustering requires Delta Lake. Adopt Delta tables with liquid clustering."


def _score_perf_024(data: dict) -> tuple[int, str]:
    """Cost-efficient instance types (Compute Cheat Sheet) - cloud-aware."""
    cloud = _cloud(data)
    compute = _get(data, "ComputeCollector") or {}
    clusters = compute.get("clusters", []) or []
    if not clusters:
        if cloud == "aws":
            return 1, "No clusters. Use AWS Graviton instance types for best price-to-performance."
        if cloud == "azure":
            return 1, "No clusters. Use Azure Dv5/Ev5 (Arm-based) instances for best price-to-performance."
        if cloud == "gcp":
            return 1, "No clusters. Use GCP T2D/N2D instances for best price-to-performance."
        return 1, "No clusters to evaluate. Use cost-efficient instance types."
    node_ids = [str(c.get("node_type_id", "")).lower() for c in clusters if isinstance(c, dict) and c.get("node_type_id")]
    if cloud == "aws":
        # Graviton: m6g, m7g, c6g, c7g, r6g, r7g, etc.
        efficient = sum(1 for n in node_ids if any(p in n for p in ("6g.", "7g.", "6gd.", "7gd.", "6gn.", "7gn.")))
        label = "Graviton"
    elif cloud == "azure":
        # Azure efficient: Dv5, Ev5, Dpv5, Epv5 (Arm-based Cobalt/Ampere)
        efficient = sum(1 for n in node_ids if any(p in n for p in ("dpsv5", "dpdsv5", "epsv5", "epdsv5", "dpsv6", "standard_d")))
        label = "cost-efficient (Dv5/Ev5/Cobalt)"
    elif cloud == "gcp":
        # GCP efficient: T2D, N2D, T2A (Arm)
        efficient = sum(1 for n in node_ids if any(p in n for p in ("t2d-", "n2d-", "t2a-")))
        label = "cost-efficient (T2D/N2D)"
    else:
        efficient = 0
        label = "cost-efficient"
    if efficient > 0:
        return 2, f"{efficient}/{len(clusters)} cluster(s) using {label} instances ({cloud.upper()})."
    return 0, f"No {label} instances across {len(clusters)} clusters ({cloud.upper()}). Adopt for best price-to-performance."


def _score_perf_025(data: dict) -> tuple[int, str]:
    """Standard access mode (Compute Cheat Sheet + UC Best Practices)."""
    compute = _get(data, "ComputeCollector") or {}
    clusters = compute.get("clusters", []) or []
    if not clusters:
        return 1, "No clusters. Use standard access mode when creating clusters for UC data isolation."
    standard = sum(
        1 for c in clusters if isinstance(c, dict) and
        str(c.get("data_security_mode", "")).upper() in ("USER_ISOLATION", "STANDARD")
    )
    dedicated = sum(
        1 for c in clusters if isinstance(c, dict) and
        str(c.get("data_security_mode", "")).upper() in ("SINGLE_USER", "DEDICATED")
    )
    none_mode = len(clusters) - standard - dedicated
    if standard > 0 and none_mode == 0:
        return 2, f"{standard}/{len(clusters)} cluster(s) use standard/shared access mode with UC isolation."
    if standard > 0:
        return 1, f"{standard}/{len(clusters)} cluster(s) use standard mode. {none_mode} have no access mode set. Configure all for UC."
    if none_mode > 0:
        return 0, f"{none_mode}/{len(clusters)} cluster(s) have no access mode set. Use standard access mode for UC."
    return 1, f"Clusters use dedicated mode. Prefer standard access mode unless functionality requires dedicated."


# ---------------------------------------------------------------------------
# Cost scoring functions
# ---------------------------------------------------------------------------


def _score_cost_001(data: dict) -> tuple[int, str]:
    """Optimized formats."""
    return _score_gov_012(data)


def _score_cost_002(data: dict) -> tuple[int, str]:
    """Job clusters."""
    ops = _get(data, "OperationsCollector") or {}
    jobs = ops.get("jobs", []) or []
    using_job_clusters = sum(1 for j in jobs if isinstance(j, dict) and j.get("has_job_clusters") and not j.get("existing_cluster_id"))
    if using_job_clusters > 0:
        return 2, f"{using_job_clusters} job(s) using job clusters for cost efficiency."
    job_count = ops.get("job_count", 0) or len(jobs)
    if job_count > 0:
        return 1, "Jobs present but not using job clusters. Use job clusters for cost efficiency."
    return 0, "Use job clusters instead of all-purpose clusters."


def _score_cost_003(data: dict) -> tuple[int, str]:
    """SQL for SQL."""
    compute = _get(data, "ComputeCollector") or {}
    warehouse_count = compute.get("warehouse_count", 0) or 0
    if warehouse_count > 0:
        return 2, f"SQL Warehouses used for SQL workloads ({warehouse_count} warehouses)."
    return 1, "SQL Warehouses not detected. Use for BI/SQL workloads."


def _score_cost_004(data: dict) -> tuple[int, str]:
    """Up-to-date runtimes."""
    compute = _get(data, "ComputeCollector") or {}
    clusters = compute.get("clusters", []) or []
    if clusters:
        versions = [c.get("spark_version", "") for c in clusters if isinstance(c, dict) and c.get("spark_version")]
        if versions:
            # Check if any cluster uses a recent DBR (14.x or 15.x)
            recent = [v for v in versions if any(v.startswith(f"{n}.") for n in range(14, 20))]
            if recent:
                return 2, f"Clusters use recent runtimes (e.g. {recent[0]}). {len(recent)}/{len(versions)} up-to-date."
            return 1, f"Clusters detected with older runtimes (e.g. {versions[0]}). Upgrade to latest DBR."
    cluster_count = compute.get("cluster_count", 0) or 0
    if cluster_count > 0:
        return 1, "Clusters present; runtime versions not available. Keep DBR runtimes up to date."
    return 1, "Keep DBR runtimes up to date for security and performance."


def _score_cost_005(data: dict) -> tuple[int, str]:
    """GPU right workloads."""
    compute = _get(data, "ComputeCollector") or {}
    clusters = compute.get("clusters", []) or []
    gpu_clusters = [c for c in clusters if isinstance(c, dict) and "gpu" in str(c.get("node_type_id", "")).lower()]
    if gpu_clusters:
        return 1, f"{len(gpu_clusters)} GPU cluster(s) detected. Ensure used only for ML training/inference."
    cluster_count = compute.get("cluster_count", 0) or len(clusters)
    if cluster_count > 0:
        return 2, "No GPU clusters — cost-efficient instance selection."
    return 1, "GPU usage not applicable. Use only for ML when needed."


def _score_cost_006(data: dict) -> tuple[int, str]:
    """Serverless (Cost)."""
    return _score_int_010(data)


def _score_cost_007(data: dict) -> tuple[int, str]:
    """Right instance type."""
    compute = _get(data, "ComputeCollector") or {}
    policy_count = compute.get("policy_count", 0) or 0
    if policy_count > 0:
        return 2, f"Cluster policies help enforce right instance types ({policy_count} policies)."
    return 1, "Define policies to restrict instance types."


def _score_cost_008(data: dict) -> tuple[int, str]:
    """Efficient compute size."""
    compute = _get(data, "ComputeCollector") or {}
    clusters = compute.get("clusters", []) or []
    with_autoscale = sum(1 for c in clusters if isinstance(c, dict) and c.get("autoscale"))
    if with_autoscale > 0:
        return 2, f"{with_autoscale} cluster(s) with autoscaling for right-sizing."
    cluster_count = compute.get("cluster_count", 0) or len(clusters)
    if cluster_count > 0:
        return 1, "Clusters present but autoscaling not enabled. Enable for right-sizing."
    return 0, "Configure efficient compute size when provisioning."


def _score_cost_009(data: dict) -> tuple[int, str]:
    """Performance engines."""
    return _score_rel_002(data)


def _score_cost_010(data: dict) -> tuple[int, str]:
    """Auto-scaling."""
    compute = _get(data, "ComputeCollector") or {}
    clusters = compute.get("clusters", []) or []
    warehouses = compute.get("warehouses", []) or []
    wh_autoscale = sum(1 for w in warehouses if isinstance(w, dict) and (w.get("max_num_clusters") or 0) > 1)
    cluster_autoscale = sum(1 for c in clusters if isinstance(c, dict) and c.get("autoscale"))
    total = wh_autoscale + cluster_autoscale
    if total > 0:
        return 2, f"Auto-scaling enabled: {cluster_autoscale} cluster(s), {wh_autoscale} warehouse(s) with multi-cluster."
    warehouse_count = compute.get("warehouse_count", 0) or len(warehouses)
    cluster_count = compute.get("cluster_count", 0) or len(clusters)
    if warehouse_count > 0 or cluster_count > 0:
        return 1, "Compute present but auto-scaling not enabled. Enable on warehouses and clusters."
    return 0, "Enable auto-scaling for cost efficiency."


def _score_cost_011(data: dict) -> tuple[int, str]:
    """Auto-termination."""
    compute = _get(data, "ComputeCollector") or {}
    clusters = compute.get("clusters", []) or []
    warehouses = compute.get("warehouses", []) or []
    wh_autostop = sum(1 for w in warehouses if isinstance(w, dict) and (w.get("auto_stop_mins") or 0) > 0)
    cluster_autostop = sum(1 for c in clusters if isinstance(c, dict) and (c.get("auto_termination_minutes") or 0) > 0)
    total = wh_autostop + cluster_autostop
    if total > 0:
        return 2, f"Auto-termination configured: {cluster_autostop} cluster(s), {wh_autostop} warehouse(s)."
    warehouse_count = compute.get("warehouse_count", 0) or len(warehouses)
    cluster_count = compute.get("cluster_count", 0) or len(clusters)
    if warehouse_count > 0 or cluster_count > 0:
        return 1, "Compute present but auto-termination not configured. Configure auto-stop on warehouses and clusters."
    return 0, "Enable auto-termination when provisioning."


def _score_cost_012(data: dict) -> tuple[int, str]:
    """Cluster policies costs."""
    return _score_ops_008(data)


def _score_cost_013(data: dict) -> tuple[int, str]:
    """Monitor costs."""
    return 1, "Cost tagging not verifiable from API. Add tags for cost allocation."


def _score_cost_014(data: dict) -> tuple[int, str]:
    """Tag clusters."""
    compute = _get(data, "ComputeCollector") or {}
    clusters = compute.get("clusters", []) or []
    with_tags = sum(1 for c in clusters if isinstance(c, dict) and len(c.get("custom_tags") or {}) > 0)
    if with_tags > 0:
        return 2, f"{with_tags} cluster(s) with custom tags for cost allocation."
    cluster_count = compute.get("cluster_count", 0) or len(clusters)
    if cluster_count > 0:
        return 1, "Clusters present but no custom tags. Tag clusters for cost allocation."
    return 0, "Tag clusters for chargeback and cost monitoring."


def _score_cost_015(data: dict) -> tuple[int, str]:
    """Chargeback."""
    return _score_cost_013(data)


def _score_cost_016(data: dict) -> tuple[int, str]:
    """Cost reports."""
    return _score_cost_013(data)


def _score_cost_017(data: dict) -> tuple[int, str]:
    """Streaming balance."""
    return 1, "Streaming cost/throughput balance not verifiable. Tune micro-batch size."


def _score_cost_018(data: dict) -> tuple[int, str]:
    """On-demand vs reserved - cloud-aware."""
    cloud = _cloud(data)
    if cloud == "aws":
        return 1, "Evaluate AWS Reserved Instances or Savings Plans for predictable workloads."
    if cloud == "azure":
        return 1, "Evaluate Azure Reserved VM Instances for predictable workloads."
    if cloud == "gcp":
        return 1, "Evaluate GCP Committed Use Discounts (CUDs) for predictable workloads."
    return 1, "Reserved/savings usage not verifiable from API. Evaluate cloud-specific commitment discounts."


def _score_cost_019(data: dict) -> tuple[int, str]:
    """Spot/preemptible instance strategy (Compute Cheat Sheet) - cloud-aware."""
    cloud = _cloud(data)
    compute = _get(data, "ComputeCollector") or {}
    clusters = compute.get("clusters", []) or []
    if cloud == "aws":
        label = "Spot instances"
    elif cloud == "azure":
        label = "Azure Spot VMs"
    elif cloud == "gcp":
        label = "Preemptible VMs"
    else:
        label = "spot/preemptible instances"
    if not clusters:
        return 1, f"No clusters. Use {label} for fault-tolerant batch workloads ({cloud.upper()})."
    spot_count = sum(
        1 for c in clusters if isinstance(c, dict) and (
            str(c.get("availability", "")).upper() in ("SPOT", "SPOT_WITH_FALLBACK", "PREEMPTIBLE") or
            (c.get("first_on_demand") is not None and c.get("first_on_demand") == 0)
        )
    )
    if spot_count > 0:
        return 2, f"{spot_count}/{len(clusters)} cluster(s) use {label} for cost savings ({cloud.upper()})."
    return 1, f"No {label} usage across {len(clusters)} clusters ({cloud.upper()}). Evaluate for batch workloads."


def _score_cost_020(data: dict) -> tuple[int, str]:
    """Budget alerts (Admin Cheat Sheet) - cloud-aware."""
    cloud = _cloud(data)
    if cloud == "aws":
        return 1, "Configure Databricks budget alerts in Account Console. Also set up AWS Cost Explorer and billing alarms."
    if cloud == "azure":
        return 1, "Configure Databricks budget alerts in Account Console. Also use Azure Cost Management budgets."
    if cloud == "gcp":
        return 1, "Configure Databricks budget alerts in Account Console. Also use GCP Budget & Alerts."
    return 1, "Budget alerts are account-level and not verifiable from workspace API. Configure in Account Console."


# ---------------------------------------------------------------------------
# Scoring Registry
# ---------------------------------------------------------------------------

SCORING_REGISTRY: dict[str, Callable[..., tuple[int, str]]] = {
    "gov-001": _score_gov_001,
    "gov-002": _score_gov_002,
    "gov-003": _score_gov_003,
    "gov-004": _score_gov_004,
    "gov-005": _score_gov_005,
    "gov-006": _score_gov_006,
    "gov-007": _score_gov_007,
    "gov-008": _score_gov_008,
    "gov-009": _score_gov_009,
    "gov-010": _score_gov_010,
    "gov-011": _score_gov_011,
    "gov-012": _score_gov_012,
    "gov-013": _score_gov_013,
    "gov-014": _score_gov_014,
    "gov-015": _score_gov_015,
    "int-001": _score_int_001,
    "int-002": _score_int_002,
    "int-003": _score_int_003,
    "int-004": _score_int_004,
    "int-005": _score_int_005,
    "int-006": _score_int_006,
    "int-007": _score_int_007,
    "int-008": _score_int_008,
    "int-009": _score_int_009,
    "int-010": _score_int_010,
    "int-011": _score_int_011,
    "int-012": _score_int_012,
    "int-013": _score_int_013,
    "int-014": _score_int_014,
    "int-015": _score_int_015,
    "ops-001": _score_ops_001,
    "ops-002": _score_ops_002,
    "ops-003": _score_ops_003,
    "ops-004": _score_ops_004,
    "ops-005": _score_ops_005,
    "ops-006": _score_ops_006,
    "ops-007": _score_ops_007,
    "ops-008": _score_ops_008,
    "ops-009": _score_ops_009,
    "ops-010": _score_ops_010,
    "ops-011": _score_ops_011,
    "ops-012": _score_ops_012,
    "ops-013": _score_ops_013,
    "ops-014": _score_ops_014,
    "ops-015": _score_ops_015,
    "ops-016": _score_ops_016,
    "ops-017": _score_ops_017,
    "ops-018": _score_ops_018,
    "ops-019": _score_ops_019,
    "ops-020": _score_ops_020,
    "ops-021": _score_ops_021,
    "ops-022": _score_ops_022,
    "ops-023": _score_ops_023,
    "sec-001": _score_sec_001,
    "sec-002": _score_sec_002,
    "sec-003": _score_sec_003,
    "sec-004": _score_sec_004,
    "sec-005": _score_sec_005,
    "sec-006": _score_sec_006,
    "sec-007": _score_sec_007,
    "sec-008": _score_sec_008,
    "sec-009": _score_sec_009,
    "sec-010": _score_sec_010,
    "sec-011": _score_sec_011,
    "sec-012": _score_sec_012,
    "rel-001": _score_rel_001,
    "rel-002": _score_rel_002,
    "rel-003": _score_rel_003,
    "rel-004": _score_rel_004,
    "rel-005": _score_rel_005,
    "rel-006": _score_rel_006,
    "rel-007": _score_rel_007,
    "rel-008": _score_rel_008,
    "rel-009": _score_rel_009,
    "rel-010": _score_rel_010,
    "rel-011": _score_rel_011,
    "rel-012": _score_rel_012,
    "rel-013": _score_rel_013,
    "rel-014": _score_rel_014,
    "rel-015": _score_rel_015,
    "rel-016": _score_rel_016,
    "rel-017": _score_rel_017,
    "rel-018": _score_rel_018,
    "rel-019": _score_rel_019,
    "perf-001": _score_perf_001,
    "perf-002": _score_perf_002,
    "perf-003": _score_perf_003,
    "perf-004": _score_perf_004,
    "perf-005": _score_perf_005,
    "perf-006": _score_perf_006,
    "perf-007": _score_perf_007,
    "perf-008": _score_perf_008,
    "perf-009": _score_perf_009,
    "perf-010": _score_perf_010,
    "perf-011": _score_perf_011,
    "perf-012": _score_perf_012,
    "perf-013": _score_perf_013,
    "perf-014": _score_perf_014,
    "perf-015": _score_perf_015,
    "perf-016": _score_perf_016,
    "perf-017": _score_perf_017,
    "perf-018": _score_perf_018,
    "perf-019": _score_perf_019,
    "perf-020": _score_perf_020,
    "perf-021": _score_perf_021,
    "perf-022": _score_perf_022,
    "perf-023": _score_perf_023,
    "perf-024": _score_perf_024,
    "perf-025": _score_perf_025,
    "cost-001": _score_cost_001,
    "cost-002": _score_cost_002,
    "cost-003": _score_cost_003,
    "cost-004": _score_cost_004,
    "cost-005": _score_cost_005,
    "cost-006": _score_cost_006,
    "cost-007": _score_cost_007,
    "cost-008": _score_cost_008,
    "cost-009": _score_cost_009,
    "cost-010": _score_cost_010,
    "cost-011": _score_cost_011,
    "cost-012": _score_cost_012,
    "cost-013": _score_cost_013,
    "cost-014": _score_cost_014,
    "cost-015": _score_cost_015,
    "cost-016": _score_cost_016,
    "cost-017": _score_cost_017,
    "cost-018": _score_cost_018,
    "cost-019": _score_cost_019,
    "cost-020": _score_cost_020,
}



# ---------------------------------------------------------------------------
# ScoredAssessment and ScoringEngine
# ---------------------------------------------------------------------------


@dataclass
class ScoredBestPractice:
    """Score and notes for a single best practice."""

    name: str
    pillar: str
    principle: str
    score: int
    finding_notes: str


@dataclass
class ScoredAssessment:
    """Complete scored assessment result."""

    pillar_scores: dict[str, float] = field(default_factory=dict)
    best_practice_scores: list[ScoredBestPractice] = field(default_factory=list)
    overall_score: float = 0.0
    maturity_level: str = "Beginning"
    assessment_date: str = ""
    workspace_host: str = ""
    cloud_provider: str = ""


def _maturity_from_score(avg: float) -> str:
    """Map overall average score to maturity level."""
    if avg >= 1.75:
        return "Optimized"
    if avg >= 1.25:
        return "Established"
    if avg >= 0.5:
        return "Developing"
    return "Beginning"


class ScoringEngine:
    """Scoring engine that evaluates collected data against WAL best practices."""

    def __init__(self) -> None:
        from wal_e.framework.pillars import ALL_PILLARS, ALL_BEST_PRACTICES

        self._pillars = ALL_PILLARS
        self._all_bps = ALL_BEST_PRACTICES

    def score_all(
        self,
        collected_data: dict,
        workspace_host: str = "",
    ) -> ScoredAssessment:
        """
        Score all best practices against collected data.

        Args:
            collected_data: Dict from collectors (keyed by collector name or flattened).
            workspace_host: Optional workspace host for the assessment.

        Returns:
            ScoredAssessment with pillar_scores, best_practice_scores, overall_score,
            maturity_level, assessment_date, and workspace_host.
        """
        # Flatten if top-level keys look like collector names
        flat_data = self._prepare_data(collected_data)

        scored_bps: list[ScoredBestPractice] = []
        pillar_totals: dict[str, list[int]] = {}

        for bp in self._all_bps.values():
            fn = SCORING_REGISTRY.get(bp.id)
            if fn:
                score, notes = fn(flat_data)
            else:
                score, notes = 0, "No scoring function defined."
            scored_bps.append(
                ScoredBestPractice(
                    name=bp.name,
                    pillar=bp.pillar,
                    principle=bp.principle,
                    score=score,
                    finding_notes=notes,
                )
            )
            if bp.pillar not in pillar_totals:
                pillar_totals[bp.pillar] = []
            pillar_totals[bp.pillar].append(score)

        pillar_scores = {
            p: sum(scores) / len(scores) if scores else 0.0
            for p, scores in pillar_totals.items()
        }
        all_scores = [sbp.score for sbp in scored_bps]
        overall = sum(all_scores) / len(all_scores) if all_scores else 0.0
        maturity = _maturity_from_score(overall)

        return ScoredAssessment(
            pillar_scores=pillar_scores,
            best_practice_scores=scored_bps,
            overall_score=round(overall, 2),
            maturity_level=maturity,
            assessment_date=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            workspace_host=workspace_host,
            cloud_provider=collected_data.get("_cloud_provider", "unknown"),
        )

    def _prepare_data(self, data: dict) -> dict:
        """Merge collector outputs into a flat dict for scoring."""
        result: dict[str, Any] = {}
        for key, val in data.items():
            if isinstance(val, dict):
                for k, v in val.items():
                    result[k] = v
            result[key] = val
        return result
