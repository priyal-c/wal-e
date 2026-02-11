"""
WAL-E Scoring Engine: Evaluates collected workspace data against WAL best practices.

Scores each best practice 0-2 based on collected data from Databricks API/CLI.
Returns ScoredAssessment with pillar averages, overall score, and maturity level.
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
    flat = _flatten_collected(data)
    uc = _get(data, "GovernanceCollector", "unity_catalog") or flat.get("unity_catalog") or {}
    catalogs = uc.get("catalogs", []) if isinstance(uc, dict) else []
    if catalogs and len(catalogs) > 0:
        return 2, "Unity Catalog adopted with multiple catalogs; governance process evident."
    if uc or _get(data, "GovernanceCollector"):
        return 1, "Some governance in place; consider formalizing governance process."
    return 0, "No governance process detected. Adopt Unity Catalog and define governance."


def _score_gov_002(data: dict) -> tuple[int, str]:
    """Manage metadata in one place."""
    flat = _flatten_collected(data)
    metastores = flat.get("metastores", [])
    if isinstance(metastores, list) and len(metastores) == 1:
        return 2, "Single metastore; metadata managed centrally."
    if metastores and len(metastores) <= 2:
        return 1, "Multiple metastores present; consider consolidating."
    return 0, "No single metadata store detected. Use Unity Catalog metastore."


def _score_gov_003(data: dict) -> tuple[int, str]:
    """Track lineage."""
    flat = _flatten_collected(data)
    lineage = flat.get("lineage_enabled", flat.get("lineage"))
    if lineage is True:
        return 2, "Lineage tracking enabled."
    if lineage:
        return 1, "Partial lineage; verify table/column lineage coverage."
    return 0, "Lineage not detected. Enable Unity Catalog lineage."


def _score_gov_004(data: dict) -> tuple[int, str]:
    """Add descriptions."""
    flat = _flatten_collected(data)
    schemas_with_desc = flat.get("schemas_with_descriptions", 0)
    tables_with_desc = flat.get("tables_with_descriptions", 0)
    if isinstance(schemas_with_desc, (int, float)) and isinstance(tables_with_desc, (int, float)):
        total = schemas_with_desc + tables_with_desc
        if total > 10:
            return 2, f"Good metadata coverage: {total} objects with descriptions."
        if total > 0:
            return 1, f"Some descriptions present ({total}); expand coverage."
    return 0, "No descriptions detected. Add comments to schemas and tables."


def _score_gov_005(data: dict) -> tuple[int, str]:
    """Allow discovery."""
    flat = _flatten_collected(data)
    catalog = flat.get("catalog") or flat.get("unity_catalog")
    if catalog and _get(data, "GovernanceCollector", "search_enabled"):
        return 2, "Discovery/search enabled for catalog."
    if catalog or flat.get("catalogs"):
        return 1, "Catalog present; verify search and discovery are configured."
    return 0, "Data discovery not configured. Enable Unity Catalog and search."


def _score_gov_006(data: dict) -> tuple[int, str]:
    """Govern AI assets."""
    flat = _flatten_collected(data)
    model_registry = flat.get("model_registry", flat.get("mlflow_registry"))
    feature_store = flat.get("feature_store")
    experiments = flat.get("experiments", [])
    if model_registry and (feature_store or experiments):
        return 2, "AI assets governed via Model Registry and Feature Store/experiments."
    if model_registry or feature_store:
        return 1, "Partial AI governance; extend to all ML assets."
    return 0, "No ML governance detected. Register models in MLflow Model Registry."


def _score_gov_007(data: dict) -> tuple[int, str]:
    """Centralize access control."""
    flat = _flatten_collected(data)
    metastores = flat.get("metastores", [])
    if isinstance(metastores, list):
        for m in metastores:
            owner = (m.get("default_catalog_name") or m.get("owner") or "").lower()
            if "account users" in owner or "account_users" in str(owner):
                return 0, "Metastore owner 'account users' reduces centralized control; use service principal."
            if m.get("privilege_model") == "UC_BASED" or m.get("unity_catalog"):
                return 2, "Unity Catalog–based access control centralized."
    if metastores:
        return 1, "Metastore present; verify UC-based isolation and central RBAC."
    return 0, "No centralized access control. Migrate to Unity Catalog."


def _score_gov_008(data: dict) -> tuple[int, str]:
    """Configure audit logging."""
    flat = _flatten_collected(data)
    system_tables = flat.get("system_tables_accessible", flat.get("audit_logs"))
    workspace_conf = flat.get("workspace_conf", {}) or _get(data, "SecurityCollector", "workspace_conf") or {}
    if isinstance(workspace_conf, dict):
        audit = workspace_conf.get("enableAuditLogDelivery") or workspace_conf.get("audit")
    else:
        audit = None
    if system_tables is True or audit:
        return 2, "Audit logging configured; system tables or log delivery enabled."
    if _get(data, "GovernanceCollector") or _get(data, "SecurityCollector"):
        return 1, "Partial audit; ensure system tables and log delivery are enabled."
    return 0, "Audit logging not configured. Enable system tables and log delivery."


def _score_gov_009(data: dict) -> tuple[int, str]:
    """Audit events."""
    flat = _flatten_collected(data)
    audit_events = flat.get("audit_events", flat.get("audit_log_events"))
    if audit_events and (isinstance(audit_events, list) and len(audit_events) > 0 or audit_events is True):
        return 2, "Audit events are being captured."
    if _get(data, "GovernanceCollector", "audit"):
        return 1, "Audit partially configured; verify event coverage."
    return 0, "No audit events detected. Configure audit log delivery."


def _score_gov_010(data: dict) -> tuple[int, str]:
    """Define DQ standards."""
    flat = _flatten_collected(data)
    expectations = flat.get("expectations_count", flat.get("dq_rules", 0))
    if isinstance(expectations, (int, float)) and expectations > 5:
        return 2, f"DQ standards defined: {expectations} expectations/rules."
    if expectations and expectations > 0:
        return 1, f"Some DQ rules ({expectations}); expand standards."
    return 0, "No DQ standards detected. Define expectations and rules."


def _score_gov_011(data: dict) -> tuple[int, str]:
    """Use DQ tools."""
    flat = _flatten_collected(data)
    expectations = flat.get("expectations", flat.get("dq_checks", []))
    if isinstance(expectations, list) and len(expectations) > 3:
        return 2, f"DQ tools in use: {len(expectations)} expectations."
    if expectations and len(expectations) > 0:
        return 1, "Some DQ checks; expand to critical tables."
    return 0, "No DQ tools detected. Use Delta Live Tables expectations or similar."


def _score_gov_012(data: dict) -> tuple[int, str]:
    """Enforce standardized formats."""
    flat = _flatten_collected(data)
    delta_tables = flat.get("delta_tables", flat.get("tables_delta", 0))
    total_tables = flat.get("total_tables", flat.get("tables_total", 1))
    if isinstance(delta_tables, (int, float)) and isinstance(total_tables, (int, float)):
        if total_tables > 0 and delta_tables / total_tables >= 0.9:
            return 2, "Most tables use Delta Lake format."
        if delta_tables > 0:
            return 1, f"Partial Delta adoption ({delta_tables}/{total_tables}); standardize."
    return 0, "Standardized formats not detected. Migrate to Delta Lake."


# ---------------------------------------------------------------------------
# Interoperability & Usability scoring functions
# ---------------------------------------------------------------------------


def _score_int_001(data: dict) -> tuple[int, str]:
    """Standard integration patterns."""
    flat = _flatten_collected(data)
    connectors = flat.get("connectors", [])
    if connectors and len(connectors) >= 2:
        return 2, "Multiple standard connectors in use."
    if connectors:
        return 1, "Some connectors; expand standard integration patterns."
    return 0, "No standard integration patterns. Use Fivetran, Airbyte, or native connectors."


def _score_int_002(data: dict) -> tuple[int, str]:
    """Optimized connectors."""
    flat = _flatten_collected(data)
    native = flat.get("native_connectors", flat.get("optimized_connectors", []))
    if native and len(native) > 0:
        return 2, "Optimized/native connectors in use."
    return 1, "Connector optimization not verified. Prefer native Delta connectors."


def _score_int_003(data: dict) -> tuple[int, str]:
    """Certified partner tools."""
    flat = _flatten_collected(data)
    partners = flat.get("partner_integrations", [])
    if partners and len(partners) >= 1:
        return 2, "Certified partner tools integrated."
    return 1, "Partner tool certification not verified."


def _score_int_004(data: dict) -> tuple[int, str]:
    """Reduce pipeline complexity."""
    flat = _flatten_collected(data)
    pipelines = flat.get("pipelines", flat.get("dlt_pipelines", []))
    jobs = flat.get("jobs", [])
    if isinstance(pipelines, list):
        dlt_count = len([p for p in pipelines if p.get("workflow_type") == "DLT" or "delta_live" in str(p).lower()])
        if dlt_count > 0 and len(pipelines) <= 20:
            return 2, "DLT pipelines reduce complexity."
        if dlt_count > 0:
            return 1, "DLT in use; consider consolidating pipelines."
    return 0, "Pipeline complexity not reduced. Adopt Delta Live Tables."


def _score_int_005(data: dict) -> tuple[int, str]:
    """Use IaC."""
    flat = _flatten_collected(data)
    iac = flat.get("iac_used", flat.get("terraform"))
    if iac is True:
        return 2, "Infrastructure as Code in use."
    return 1, "IaC usage not detected from API. Document Terraform/CI usage."


def _score_int_006(data: dict) -> tuple[int, str]:
    """Open data formats."""
    flat = _flatten_collected(data)
    delta = flat.get("delta_tables", 0)
    if isinstance(delta, (int, float)) and delta > 0:
        return 2, "Delta Lake (open format) in use."
    return 0, "Open data formats not detected. Use Delta Lake."


def _score_int_007(data: dict) -> tuple[int, str]:
    """Secure sharing."""
    flat = _flatten_collected(data)
    sharing = flat.get("delta_sharing", flat.get("shares", []))
    if sharing and (isinstance(sharing, list) and len(sharing) > 0 or sharing is True):
        return 2, "Delta Sharing configured for secure sharing."
    return 1, "Secure sharing not verified. Configure Delta Sharing for cross-org."


def _score_int_008(data: dict) -> tuple[int, str]:
    """Open ML standards."""
    flat = _flatten_collected(data)
    mlflow = flat.get("mlflow", flat.get("model_registry"))
    if mlflow:
        return 2, "MLflow (open ML standard) in use."
    return 0, "Open ML standards not detected. Use MLflow."


def _score_int_009(data: dict) -> tuple[int, str]:
    """Self-service."""
    flat = _flatten_collected(data)
    warehouses = flat.get("sql_warehouses", flat.get("warehouses", []))
    if isinstance(warehouses, list) and len(warehouses) > 0:
        return 2, "SQL Warehouses available for self-service analytics."
    return 1, "Self-service SQL not fully configured. Provision SQL Warehouses."


def _score_int_010(data: dict) -> tuple[int, str]:
    """Serverless compute (Interoperability)."""
    flat = _flatten_collected(data)
    whs = flat.get("sql_warehouses", flat.get("warehouses", []))
    if isinstance(whs, list):
        serverless = [w for w in whs if w.get("warehouse_type") == "PRO" or w.get("enable_serverless_compute")]
        total = len(whs)
        if total > 0:
            pct = len(serverless) / total * 100
            if pct >= 80:
                return 2, f"{pct:.0f}% of SQL warehouses are serverless."
            if pct > 0:
                return 1, f"Partial serverless ({pct:.0f}%); increase adoption."
    return 0, "No serverless SQL warehouses. Use Pro/Serverless warehouses."


def _score_int_011(data: dict) -> tuple[int, str]:
    """Predefined compute templates."""
    flat = _flatten_collected(data)
    policies = flat.get("cluster_policies", flat.get("policies", []))
    if isinstance(policies, list) and len(policies) > 0:
        return 2, "Cluster policies (compute templates) defined."
    return 0, "No cluster policies. Define compute templates via policies."


def _score_int_012(data: dict) -> tuple[int, str]:
    """AI productivity."""
    flat = _flatten_collected(data)
    assistant = flat.get("assistant_enabled", flat.get("ai_assistant"))
    if assistant is True:
        return 2, "AI assistant/productivity tools enabled."
    return 1, "AI productivity not verified from API."


def _score_int_013(data: dict) -> tuple[int, str]:
    """Reusable data products."""
    flat = _flatten_collected(data)
    shares = flat.get("shares", flat.get("data_products", []))
    if shares and (isinstance(shares, list) and len(shares) > 0):
        return 2, "Reusable data products (shares) defined."
    return 1, "Reusable data products not verified. Define Delta Sharing."


def _score_int_014(data: dict) -> tuple[int, str]:
    """Semantic consistency."""
    flat = _flatten_collected(data)
    semantic = flat.get("semantic_layer", flat.get("semantic_models"))
    if semantic:
        return 2, "Semantic layer/models for consistency."
    return 1, "Semantic consistency not verified. Use Databricks SQL semantic layer."


def _score_int_015(data: dict) -> tuple[int, str]:
    """UC for discovery."""
    flat = _flatten_collected(data)
    uc = flat.get("unity_catalog", flat.get("metastores"))
    if uc and (isinstance(uc, dict) or (isinstance(uc, list) and len(uc) > 0)):
        return 2, "Unity Catalog used for discovery."
    return 0, "Unity Catalog not detected for discovery."


# ---------------------------------------------------------------------------
# Operational Excellence scoring functions
# ---------------------------------------------------------------------------


def _score_ops_001(data: dict) -> tuple[int, str]:
    """Dedicated ops team."""
    return 1, "Cannot verify org structure from API. Document dedicated ops ownership."


def _score_ops_002(data: dict) -> tuple[int, str]:
    """Enterprise SCM."""
    flat = _flatten_collected(data)
    repos = flat.get("repos", flat.get("git_repos", []))
    if repos and len(repos) > 0:
        return 2, "Repos/SCM configured."
    return 1, "Enterprise SCM not detected. Use Repos for source control."


def _score_ops_003(data: dict) -> tuple[int, str]:
    """Standardize CI/CD."""
    flat = _flatten_collected(data)
    jobs = flat.get("jobs", [])
    if isinstance(jobs, list) and len(jobs) > 0:
        git_jobs = [j for j in jobs if j.get("git_source") or j.get("task", {}).get("git_source")]
        if git_jobs and len(git_jobs) / max(len(jobs), 1) >= 0.5:
            return 2, "Jobs use Git-based CI/CD."
        return 1, "Some jobs present; standardize Git-based CI/CD."
    return 0, "No jobs; adopt CI/CD for deployments."


def _score_ops_004(data: dict) -> tuple[int, str]:
    """MLOps processes."""
    flat = _flatten_collected(data)
    reg = flat.get("model_registry", flat.get("registered_models", []))
    if reg and (isinstance(reg, list) and len(reg) > 0 or isinstance(reg, dict)):
        return 2, "Model Registry in use; MLOps processes evident."
    return 0, "No Model Registry. Adopt MLOps with MLflow."


def _score_ops_005(data: dict) -> tuple[int, str]:
    """Environment isolation."""
    flat = _flatten_collected(data)
    workspaces = flat.get("workspaces", flat.get("workspace_count", 0))
    catalogs = flat.get("catalogs", [])
    if isinstance(catalogs, list) and len(catalogs) >= 2:
        return 2, "Multiple catalogs; environment isolation via UC."
    if catalogs:
        return 1, "Some catalog structure; add dev/test/prod catalogs."
    return 0, "Environment isolation not evident. Use catalogs per environment."


def _score_ops_006(data: dict) -> tuple[int, str]:
    """Catalog strategy."""
    flat = _flatten_collected(data)
    catalogs = flat.get("catalogs", [])
    if isinstance(catalogs, list) and len(catalogs) >= 2:
        return 2, "Catalog strategy in place with multiple catalogs."
    if catalogs:
        return 1, "Single catalog; define dev/test/prod strategy."
    return 0, "No catalog strategy. Use Unity Catalog layering."


def _score_ops_007(data: dict) -> tuple[int, str]:
    """IaC deployments."""
    return _score_int_005(data)


def _score_ops_008(data: dict) -> tuple[int, str]:
    """Standardize compute."""
    flat = _flatten_collected(data)
    policies = flat.get("cluster_policies", [])
    clusters = flat.get("clusters", [])
    if isinstance(policies, list) and len(policies) > 0:
        policy_usage = sum(1 for c in clusters if c.get("policy_id")) if isinstance(clusters, list) else 0
        if policy_usage > 0 or len(clusters) == 0:
            return 2, "Cluster policies enforce standardized compute."
        return 1, "Policies defined; ensure all clusters use them."
    return 0, "No cluster policies. Standardize compute via policies."


def _score_ops_009(data: dict) -> tuple[int, str]:
    """Automated workflows."""
    flat = _flatten_collected(data)
    jobs = flat.get("jobs", [])
    if isinstance(jobs, list) and len(jobs) >= 3:
        return 2, f"{len(jobs)} automated jobs."
    if jobs:
        return 1, "Some jobs; expand automation."
    return 0, "No automated workflows. Create jobs for pipelines."


def _score_ops_010(data: dict) -> tuple[int, str]:
    """Event-driven ingestion."""
    flat = _flatten_collected(data)
    autoloader = flat.get("autoloader_jobs", flat.get("streaming_jobs", []))
    if autoloader and len(autoloader) > 0:
        return 2, "Event-driven/streaming ingestion in use."
    return 1, "Event-driven ingestion not verified. Use Auto Loader."


def _score_ops_011(data: dict) -> tuple[int, str]:
    """ETL frameworks."""
    flat = _flatten_collected(data)
    pipelines = flat.get("pipelines", [])
    dlt = [p for p in pipelines if p.get("workflow_type") == "DLT" or "delta_live" in str(p).lower()] if pipelines else []
    if dlt and len(dlt) > 0:
        return 2, "Delta Live Tables (ETL framework) in use."
    return 0, "No DLT pipelines. Adopt Delta Live Tables."


def _score_ops_012(data: dict) -> tuple[int, str]:
    """Deploy-code ML."""
    flat = _flatten_collected(data)
    serving = flat.get("model_serving_endpoints", flat.get("serving_endpoints", []))
    if serving and len(serving) > 0:
        return 2, "Model serving endpoints (deploy-code ML) in use."
    return 1, "Model serving not detected. Use Model Serving for deployment."


def _score_ops_013(data: dict) -> tuple[int, str]:
    """Model registry."""
    return _score_ops_004(data)


def _score_ops_014(data: dict) -> tuple[int, str]:
    """Automate experiment tracking."""
    flat = _flatten_collected(data)
    exps = flat.get("experiments", flat.get("mlflow_experiments", []))
    if exps and len(exps) > 0:
        return 2, "MLflow experiments for tracking."
    return 0, "No experiments. Use MLflow for experiment tracking."


def _score_ops_015(data: dict) -> tuple[int, str]:
    """Reuse ML infra."""
    flat = _flatten_collected(data)
    job_clusters = [j for j in (flat.get("jobs") or []) if j.get("job_clusters") or j.get("job_type") == "JOB"]
    if job_clusters and len(job_clusters) >= 2:
        return 2, "Job clusters reuse ML infra."
    return 1, "ML infra reuse not verified. Use job clusters."


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
    flat = _flatten_collected(data)
    alerts = flat.get("alerts", flat.get("monitoring"))
    if alerts or _get(data, "OperationsCollector"):
        return 2, "Monitoring/alerts configured."
    return 1, "Monitoring not fully verified. Configure alerts and dashboards."


def _score_ops_020(data: dict) -> tuple[int, str]:
    """Platform monitoring tools."""
    flat = _flatten_collected(data)
    warehouses = flat.get("sql_warehouses", [])
    if warehouses and len(warehouses) > 0:
        return 2, "SQL Warehouses enable platform monitoring (query history, etc.)."
    return 1, "Platform monitoring tools not verified. Use Databricks SQL."


# ---------------------------------------------------------------------------
# Security scoring functions
# ---------------------------------------------------------------------------


def _score_sec_001(data: dict) -> tuple[int, str]:
    """Least privilege IAM."""
    flat = _flatten_collected(data)
    uc = flat.get("unity_catalog", flat.get("metastores"))
    if uc:
        return 2, "Unity Catalog enables centralized least-privilege IAM."
    return 1, "Least-privilege IAM not fully verified. Use UC grants."


def _score_sec_002(data: dict) -> tuple[int, str]:
    """Data protection transit/rest."""
    flat = _flatten_collected(data)
    enc = flat.get("encryption", flat.get("encryption_enabled"))
    if enc is True or (isinstance(enc, dict) and enc.get("enabled")):
        return 2, "Encryption (transit/rest) configured."
    return 1, "Encryption not verified from API. Ensure SSE and TLS."


def _score_sec_003(data: dict) -> tuple[int, str]:
    """Network security."""
    flat = _flatten_collected(data)
    ipl = flat.get("ip_access_lists", flat.get("ip_allow_list", []))
    if ipl and len(ipl) > 0:
        return 2, "IP access lists configured for network security."
    return 1, "IP access lists not detected. Configure network restrictions."


def _score_sec_004(data: dict) -> tuple[int, str]:
    """Shared responsibility."""
    return 1, "Shared responsibility model not verifiable from API. Document cloud security."


def _score_sec_005(data: dict) -> tuple[int, str]:
    """Compliance requirements."""
    flat = _flatten_collected(data)
    audit = flat.get("audit_logs", flat.get("system_tables_accessible"))
    if audit:
        return 2, "Audit/compliance controls in place."
    return 1, "Compliance not fully verified. Enable audit logging."


def _score_sec_006(data: dict) -> tuple[int, str]:
    """System security monitoring."""
    flat = _flatten_collected(data)
    sys_tables = flat.get("system_tables_accessible")
    if sys_tables is True:
        return 2, "System tables accessible for security monitoring."
    return 1, "System tables access not verified. Enable for monitoring."


def _score_sec_007(data: dict) -> tuple[int, str]:
    """Generic controls."""
    flat = _flatten_collected(data)
    workspace_conf = flat.get("workspace_conf", {}) or _get(data, "SecurityCollector", "workspace_conf") or {}
    if isinstance(workspace_conf, dict):
        dbfs_browser = workspace_conf.get("enableDbfsFileBrowser") or workspace_conf.get("enableDbfsBrowser")
        if dbfs_browser is False or dbfs_browser == "false":
            return 2, "DBFS browser restricted; workspace conf enforces security controls."
        if dbfs_browser is True or dbfs_browser == "true":
            return 1, "DBFS browser enabled; consider disabling for stricter access control."
    policies = flat.get("cluster_policies", [])
    if policies and len(policies) > 0:
        return 2, "Cluster policies enforce security controls."
    return 1, "Generic controls not fully verified. Use policies and workspace conf."


# ---------------------------------------------------------------------------
# Reliability scoring functions
# ---------------------------------------------------------------------------


def _score_rel_001(data: dict) -> tuple[int, str]:
    """ACID format."""
    return _score_gov_012(data)


def _score_rel_002(data: dict) -> tuple[int, str]:
    """Resilient engine."""
    flat = _flatten_collected(data)
    clusters = flat.get("clusters", [])
    photon = sum(1 for c in clusters if c.get("runtime_engine") == "PHOTON" or c.get("photon")) if clusters else 0
    if photon > 0 and len(clusters) > 0:
        return 2, "Photon (resilient engine) in use."
    return 1, "Photon not detected. Use Photon for better resilience."


def _score_rel_003(data: dict) -> tuple[int, str]:
    """Rescue invalid data."""
    flat = _flatten_collected(data)
    pipelines = flat.get("pipelines", [])
    autoloader = flat.get("autoloader", flat.get("rescue_data"))
    if autoloader or any("autoloader" in str(p).lower() or "rescued" in str(p).lower() for p in (pipelines or [])):
        return 2, "Auto Loader/rescue invalid data pattern in use."
    return 0, "Rescue invalid data not detected. Use Auto Loader rescued column."


def _score_rel_004(data: dict) -> tuple[int, str]:
    """Auto retries."""
    flat = _flatten_collected(data)
    jobs = flat.get("jobs", [])
    with_retries = sum(1 for j in jobs if j.get("max_retries", 0) > 0 or j.get("retry_on_timeout")) if jobs else 0
    if with_retries > 0 and len(jobs) > 0:
        return 2, f"{with_retries} jobs configured with retries."
    if jobs:
        return 1, "Configure retries on jobs for resilience."
    return 0, "No jobs; add retries when creating jobs."


def _score_rel_005(data: dict) -> tuple[int, str]:
    """Scalable serving."""
    return _score_ops_012(data)


def _score_rel_006(data: dict) -> tuple[int, str]:
    """Managed services."""
    flat = _flatten_collected(data)
    jobs = flat.get("jobs", [])
    pipelines = flat.get("pipelines", [])
    whs = flat.get("sql_warehouses", [])
    total = len(jobs or []) + len(pipelines or []) + len(whs or [])
    if total >= 3:
        return 2, "Managed services (jobs, DLT, SQL) in use."
    if total > 0:
        return 1, "Some managed services; expand usage."
    return 0, "Use managed jobs, DLT, and SQL warehouses."


def _score_rel_007(data: dict) -> tuple[int, str]:
    """Layered storage."""
    flat = _flatten_collected(data)
    catalogs = flat.get("catalogs", [])
    schemas = flat.get("schemas", [])
    if isinstance(catalogs, list) and isinstance(schemas, list) and len(catalogs) >= 2 and len(schemas) >= 2:
        return 2, "Layered storage via catalogs/schemas."
    if catalogs or schemas:
        return 1, "Partial layering; adopt bronze/silver/gold."
    return 0, "No layered storage. Use catalog/schema layering."


def _score_rel_008(data: dict) -> tuple[int, str]:
    """Reduce redundancy."""
    return 1, "Data redundancy not verifiable from API. Document deduplication strategy."


def _score_rel_009(data: dict) -> tuple[int, str]:
    """Active schema mgmt."""
    flat = _flatten_collected(data)
    constraints = flat.get("constraints", flat.get("expectations", []))
    if constraints and len(constraints) > 0:
        return 2, "Schema constraints/expectations for active management."
    return 1, "Schema management not verified. Use Delta constraints."


def _score_rel_010(data: dict) -> tuple[int, str]:
    """Constraints/expectations."""
    return _score_rel_009(data)


def _score_rel_011(data: dict) -> tuple[int, str]:
    """Data-centric ML."""
    flat = _flatten_collected(data)
    fs = flat.get("feature_store", flat.get("feature_tables"))
    if fs:
        return 2, "Feature Store (data-centric ML) in use."
    return 1, "Feature Store not detected. Use for data-centric ML."


def _score_rel_012(data: dict) -> tuple[int, str]:
    """ETL autoscaling."""
    flat = _flatten_collected(data)
    pipelines = flat.get("pipelines", [])
    autoscale = [p for p in (pipelines or []) if p.get("enable_autoscaling") or p.get("autoscale")] if pipelines else []
    if autoscale and len(autoscale) > 0:
        return 2, "DLT pipelines with autoscaling."
    if pipelines:
        return 1, "Enable autoscaling on DLT pipelines."
    return 0, "No DLT pipelines. Use DLT with autoscaling."


def _score_rel_013(data: dict) -> tuple[int, str]:
    """SQL warehouse autoscaling."""
    flat = _flatten_collected(data)
    whs = flat.get("sql_warehouses", flat.get("warehouses", []))
    if isinstance(whs, list):
        with_scale = [w for w in whs if w.get("max_num_clusters", 1) > 1 or w.get("enable_serverless_compute")]
        if with_scale and len(with_scale) / max(len(whs), 1) >= 0.5:
            return 2, "SQL warehouses use autoscaling."
        if with_scale:
            return 1, "Partial autoscaling; enable on all warehouses."
    return 0, "SQL warehouse autoscaling not detected. Enable multi-cluster or serverless."


def _score_rel_014(data: dict) -> tuple[int, str]:
    """Regular backups."""
    return 1, "Backup strategy not verifiable from API. Document Delta clone/backup process."


def _score_rel_015(data: dict) -> tuple[int, str]:
    """Streaming recovery."""
    flat = _flatten_collected(data)
    pipelines = flat.get("pipelines", [])
    checkpoint = any(p.get("storage") or p.get("checkpoint") for p in (pipelines or []))
    if checkpoint or pipelines:
        return 2, "DLT checkpoints enable streaming recovery."
    return 0, "Streaming recovery not verified. Use DLT with checkpoints."


def _score_rel_016(data: dict) -> tuple[int, str]:
    """Time travel recovery."""
    flat = _flatten_collected(data)
    delta = flat.get("delta_tables", 0)
    if isinstance(delta, (int, float)) and delta > 0:
        return 2, "Delta Lake enables time travel recovery."
    return 0, "Delta Lake required for time travel. Migrate tables."


def _score_rel_017(data: dict) -> tuple[int, str]:
    """Job automation recovery."""
    return _score_rel_004(data)


def _score_rel_018(data: dict) -> tuple[int, str]:
    """DR pattern."""
    return 1, "DR pattern not verifiable from API. Document replication and RTO/RPO."


# ---------------------------------------------------------------------------
# Performance scoring functions
# ---------------------------------------------------------------------------


def _score_perf_001(data: dict) -> tuple[int, str]:
    """Scaling."""
    flat = _flatten_collected(data)
    clusters = flat.get("clusters", [])
    autoscale = sum(1 for c in clusters if c.get("autoscale") or c.get("num_workers", 0) == 0) if clusters else 0
    if autoscale > 0 and len(clusters) > 0:
        return 2, "Autoscaling configured on clusters."
    if clusters:
        return 1, "Enable autoscaling on clusters."
    return 0, "No clusters. Configure autoscaling when adding compute."


def _score_perf_002(data: dict) -> tuple[int, str]:
    """Serverless (Performance)."""
    return _score_int_010(data)


def _score_perf_003(data: dict) -> tuple[int, str]:
    """Data patterns."""
    flat = _flatten_collected(data)
    tables = flat.get("tables_optimized", flat.get("optimized_tables", 0))
    if isinstance(tables, (int, float)) and tables > 0:
        return 2, "Data optimization (OPTIMIZE/Z-ORDER) in use."
    return 1, "Data patterns not verified. Use OPTIMIZE and Z-ORDER."


def _score_perf_004(data: dict) -> tuple[int, str]:
    """Parallel computation."""
    flat = _flatten_collected(data)
    clusters = flat.get("clusters", [])
    workers = sum(c.get("num_workers", 0) for c in clusters) if clusters else 0
    if workers >= 4:
        return 2, "Parallel computation via multi-worker clusters."
    return 1, "Parallelism not fully utilized. Scale workers for throughput."


def _score_perf_005(data: dict) -> tuple[int, str]:
    """Execution chain."""
    return _score_ops_011(data)


def _score_perf_006(data: dict) -> tuple[int, str]:
    """Larger clusters."""
    flat = _flatten_collected(data)
    clusters = flat.get("clusters", [])
    if clusters:
        max_workers = max(c.get("num_workers", 0) or c.get("autoscale", {}).get("max_workers", 0) for c in clusters)
        if max_workers >= 4:
            return 2, "Larger clusters for workload-appropriate scale."
        return 1, "Consider larger clusters for heavy workloads."
    return 0, "No clusters. Right-size when provisioning."


def _score_perf_007(data: dict) -> tuple[int, str]:
    """Native Spark."""
    return _score_rel_002(data)


def _score_perf_008(data: dict) -> tuple[int, str]:
    """Native engines."""
    return _score_rel_002(data)


def _score_perf_009(data: dict) -> tuple[int, str]:
    """Hardware awareness."""
    flat = _flatten_collected(data)
    clusters = flat.get("clusters", [])
    if clusters:
        return 2, "Cluster configs imply hardware awareness. Use instance types for workload."
    return 1, "Hardware awareness not verified. Match instance types to workload."


def _score_perf_010(data: dict) -> tuple[int, str]:
    """Caching."""
    flat = _flatten_collected(data)
    cache = flat.get("delta_cache", flat.get("photon_cache"))
    if cache is True or (isinstance(cache, dict) and cache.get("enabled")):
        return 2, "Delta Cache/caching enabled."
    return 1, "Caching not verified. Use Delta Cache for repeated reads."


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
    flat = _flatten_collected(data)
    whs = flat.get("sql_warehouses", [])
    prewarm = [w for w in whs if w.get("enable_photon") or w.get("channel", {}).get("name") == "CHANNEL_NAME_PREVIEW"]
    if prewarm and len(prewarm) > 0:
        return 2, "Prewarm or Photon warehouses in use."
    return 1, "Prewarm not verified. Use for latency-sensitive workloads."


def _score_perf_018(data: dict) -> tuple[int, str]:
    """Identify bottlenecks."""
    flat = _flatten_collected(data)
    if flat.get("query_history") or flat.get("sql_warehouses"):
        return 2, "Query history/SQL available for bottleneck identification."
    return 1, "Use query history and Spark UI for bottleneck analysis."


def _score_perf_019(data: dict) -> tuple[int, str]:
    """Monitor queries."""
    flat = _flatten_collected(data)
    whs = flat.get("sql_warehouses", [])
    if whs and len(whs) > 0:
        return 2, "SQL Warehouses enable query monitoring."
    return 1, "Enable SQL Warehouses for query monitoring."


def _score_perf_020(data: dict) -> tuple[int, str]:
    """Monitor streaming."""
    flat = _flatten_collected(data)
    pipelines = flat.get("pipelines", [])
    if pipelines:
        return 2, "DLT pipelines enable streaming monitoring."
    return 0, "No streaming pipelines. Use DLT for monitoring."


def _score_perf_021(data: dict) -> tuple[int, str]:
    """Monitor jobs."""
    flat = _flatten_collected(data)
    jobs = flat.get("jobs", [])
    if jobs:
        return 2, "Jobs enable cluster/job monitoring."
    return 0, "No jobs. Create jobs for operational visibility."


# ---------------------------------------------------------------------------
# Cost scoring functions
# ---------------------------------------------------------------------------


def _score_cost_001(data: dict) -> tuple[int, str]:
    """Optimized formats."""
    return _score_gov_012(data)


def _score_cost_002(data: dict) -> tuple[int, str]:
    """Job clusters."""
    flat = _flatten_collected(data)
    jobs = flat.get("jobs", [])
    job_clusters = [j for j in jobs if j.get("job_clusters") or j.get("job_type") == "JOB" and not j.get("existing_cluster_id")] if jobs else []
    if job_clusters and len(job_clusters) / max(len(jobs), 1) >= 0.5:
        return 2, "Job clusters used for cost efficiency."
    if job_clusters:
        return 1, "Some job clusters; expand to more jobs."
    return 0, "Use job clusters instead of all-purpose clusters."


def _score_cost_003(data: dict) -> tuple[int, str]:
    """SQL for SQL."""
    flat = _flatten_collected(data)
    whs = flat.get("sql_warehouses", [])
    if whs and len(whs) > 0:
        return 2, "SQL Warehouses used for SQL workloads."
    return 1, "SQL Warehouses not detected. Use for BI/SQL workloads."


def _score_cost_004(data: dict) -> tuple[int, str]:
    """Up-to-date runtimes."""
    flat = _flatten_collected(data)
    clusters = flat.get("clusters", [])
    if clusters:
        return 2, "Runtime version check requires DBR version API. Assume maintained."
    return 1, "Keep DBR runtimes up to date for security and performance."


def _score_cost_005(data: dict) -> tuple[int, str]:
    """GPU right workloads."""
    flat = _flatten_collected(data)
    clusters = flat.get("clusters", [])
    gpu = [c for c in clusters if "gpu" in str(c.get("node_type_id", "")).lower() or c.get("gpu")] if clusters else []
    if gpu:
        return 2, "GPU clusters for appropriate ML workloads."
    return 1, "GPU usage not detected. Use only for ML training/inference."


def _score_cost_006(data: dict) -> tuple[int, str]:
    """Serverless (Cost)."""
    return _score_int_010(data)


def _score_cost_007(data: dict) -> tuple[int, str]:
    """Right instance type."""
    flat = _flatten_collected(data)
    policies = flat.get("cluster_policies", [])
    if policies:
        return 2, "Cluster policies help enforce right instance types."
    return 1, "Define policies to restrict instance types."


def _score_cost_008(data: dict) -> tuple[int, str]:
    """Efficient compute size."""
    flat = _flatten_collected(data)
    clusters = flat.get("clusters", [])
    if clusters:
        autoscale = sum(1 for c in clusters if c.get("autoscale"))
        if autoscale > 0:
            return 2, "Autoscaling enables efficient compute size."
        return 1, "Enable autoscaling for right-sizing."
    return 0, "Configure efficient compute size when provisioning."


def _score_cost_009(data: dict) -> tuple[int, str]:
    """Performance engines."""
    return _score_rel_002(data)


def _score_cost_010(data: dict) -> tuple[int, str]:
    """Auto-scaling."""
    flat = _flatten_collected(data)
    whs = flat.get("sql_warehouses", [])
    clusters = flat.get("clusters", [])
    with_scale = sum(1 for w in whs if w.get("max_num_clusters", 1) > 1) if whs else 0
    with_scale += sum(1 for c in clusters if c.get("autoscale")) if clusters else 0
    if with_scale > 0:
        return 2, "Auto-scaling configured on warehouses and/or clusters."
    return 0, "Enable auto-scaling for cost efficiency."


def _score_cost_011(data: dict) -> tuple[int, str]:
    """Auto-termination."""
    flat = _flatten_collected(data)
    whs = flat.get("sql_warehouses", flat.get("warehouses", []))
    clusters = flat.get("clusters", [])
    wh_stopped = [w for w in whs if w.get("auto_stop_mins") and w.get("auto_stop_mins") <= 120] if whs else []
    cluster_stop = [c for c in clusters if c.get("auto_termination_minutes")] if clusters else []
    if (wh_stopped and len(wh_stopped) / max(len(whs), 1) >= 0.5) or cluster_stop:
        return 2, "Auto-termination configured to reduce idle cost."
    if whs or clusters:
        return 1, "Configure auto-stop on warehouses and clusters."
    return 0, "Enable auto-termination when provisioning."


def _score_cost_012(data: dict) -> tuple[int, str]:
    """Cluster policies costs."""
    return _score_ops_008(data)


def _score_cost_013(data: dict) -> tuple[int, str]:
    """Monitor costs."""
    flat = _flatten_collected(data)
    tags = flat.get("cluster_tags", flat.get("cost_tags", flat.get("tags")))
    if tags and (isinstance(tags, list) and len(tags) > 0 or isinstance(tags, dict) and len(tags) > 0):
        return 2, "Cost tagging in place for monitoring."
    return 1, "Cost monitoring not verified. Add tags for cost allocation."


def _score_cost_014(data: dict) -> tuple[int, str]:
    """Tag clusters."""
    flat = _flatten_collected(data)
    clusters = flat.get("clusters", [])
    tagged = [c for c in clusters if c.get("custom_tags") and len(c.get("custom_tags", {})) > 0] if clusters else []
    if tagged and len(tagged) / max(len(clusters), 1) >= 0.5:
        return 2, "Clusters tagged for cost allocation."
    if tagged:
        return 1, "Some clusters tagged; expand coverage."
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
    """On-demand vs reserved."""
    return 1, "Reserved/Spot usage not verifiable from API. Evaluate instance savings."


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
    "sec-001": _score_sec_001,
    "sec-002": _score_sec_002,
    "sec-003": _score_sec_003,
    "sec-004": _score_sec_004,
    "sec-005": _score_sec_005,
    "sec-006": _score_sec_006,
    "sec-007": _score_sec_007,
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
