# Well-Architected Lakehouse Assessment
## Final Readout

---

| **Field** | **Details** |
|---|---|
| **Workspace** | https://acme-analytics.cloud.databricks.com |
| **Cloud Provider** | Amazon Web Services (AWS) |
| **Metastore** | acme_metastore |
| **Assessment Date** | 2026-07-29T19:28:28Z |
| **Catalogs** | 2411 |

---

## Executive Summary

This document presents the findings of a Well-Architected Lakehouse (WAL) assessment. The assessment follows the Databricks Well-Architected Framework covering all **seven pillars**.

### Overall Maturity Score

| Pillar | Score (0-2) | Percentage | Maturity |
|--------|:-----------:|:----------:|----------|
| Data & AI Governance | **1.2** | 59% | Developing |
| Interoperability & Usability | **1.5** | 73% | Established |
| Operational Excellence | **1.4** | 71% | Established |
| Security, Compliance & Privacy | **1.0** | 50% | Developing |
| Reliability | **1.1** | 55% | Developing |
| Performance Efficiency | **1.4** | 70% | Established |
| Cost Optimization | **1.5** | 74% | Established |
| **Overall Average** | **1.30** | **65%** | **Established** |

> **Scoring Key:** 0 = Not Implemented, 1 = Partially Implemented, 2 = Fully Implemented / Best Practice


## Pillar 1: Data & AI Governance

### 1.1 Current State Findings

| Area | Finding | Status |
|------|---------|--------|
| **Unity Catalog** | Metastore: `acme_metastore` | Implemented |
| **Catalog Count** | **2411 catalogs** | Critical Gap |
| **External Locations** | 377 external locations | Gap |
| **Storage Credentials** | 509 storage credentials | Gap |
| **Catalog Isolation** | OPEN isolation mode detected on catalogs | Gap |

### 1.2 Assessment Against Best Practices

| # | Best Practice | Score (0-2) | Notes |
|---|---------------|:-----------:|-------|
| 1 | Establish governance process | **1** | Unity Catalog in use but 2411 catalogs suggest ad-hoc governance. Formalize process. |
| 2 | Manage metadata in one place | **2** | Single metastore 'acme_metastore'; metadata managed centrally. |
| 3 | Track lineage | **1** | Unity Catalog lineage available but active tracking not verified. Monitor lineage usage. |
| 4 | Add descriptions | **0** | 2411 catalogs — likely many lack descriptions. Add comments to schemas and tables. |
| 5 | Allow discovery | **1** | Unity Catalog available but 2411 catalogs hinder discovery. Consolidate catalogs. |
| 6 | Govern AI assets | **2** | 63 model(s) governed in Unity Catalog (Vector Search indexes: 61). AI assets governed with data. |
| 7 | Centralize access control | **2** | Unity Catalog access control centralized. Metastore owner: Sample User 01. |
| 8 | Configure audit logging | **1** | Workspace configuration accessible; audit logging partially verified. Enable system tables for full audit. |
| 9 | Audit events | **1** | Workspace settings accessible; configure systematic audit event monitoring and alerting via system tables. |
| 10 | Define DQ standards | **1** | 16 DLT pipelines present — verify DQ expectations are defined. |
| 11 | Use DQ tools | **1** | 16 DLT pipelines — verify expectations are actively used. |
| 12 | Enforce standardized formats | **1** | Unity Catalog + Delta Lake in use but cannot verify all tables use Delta format from API. |
| 13 | Account-level group management | **1** | 1666 group(s) present but IdP sync is not verifiable from the workspace API (externalId is exposed only at the account level). Verify account-level, IdP-synced groups in the account console. |
| 14 | Prefer managed tables | **1** | 377 external location(s) configured, but the managed-vs-external table split is not verifiable from the API (external-location count is not a reliable proxy). Prefer UC managed tables for new tables. |
| 15 | BROWSE privilege for discovery | **1** | Unity Catalog in use. Grant BROWSE privilege on catalogs to All account users for data discovery. |
| 16 | Models registered in Unity Catalog | **2** | All 63 registered model(s) are in Unity Catalog. Centralized model governance in place. |
| 17 | Inference tables / payload logging for AI endpoints | **1** | 9/27 LLM endpoint(s) log payloads. Enable inference tables on the remaining endpoints. |

### 1.3 Recommendations (Prioritized)

| Priority | Recommendation | Impact |
|----------|---------------|--------|
| **P0** | **Add descriptions:** 2411 catalogs — likely many lack descriptions. Add comments to schemas and tables. | Critical |
| **P1** | **Establish governance process:** Unity Catalog in use but 2411 catalogs suggest ad-hoc governance. Formalize process. | High |
| **P1** | **Track lineage:** Unity Catalog lineage available but active tracking not verified. Monitor lineage usage. | High |
| **P1** | **Allow discovery:** Unity Catalog available but 2411 catalogs hinder discovery. Consolidate catalogs. | High |
| **P1** | **Configure audit logging:** Workspace configuration accessible; audit logging partially verified. Enable system tables for full audit. | High |
| **P1** | **Audit events:** Workspace settings accessible; configure systematic audit event monitoring and alerting via system tables. | High |
| **P1** | **Define DQ standards:** 16 DLT pipelines present — verify DQ expectations are defined. | High |
| **P1** | **Use DQ tools:** 16 DLT pipelines — verify expectations are actively used. | High |
| **P1** | **Enforce standardized formats:** Unity Catalog + Delta Lake in use but cannot verify all tables use Delta format from API. | High |
| **P1** | **Account-level group management:** 1666 group(s) present but IdP sync is not verifiable from the workspace API (externalId is exposed only at the account level). Verify account-level, IdP-synced groups in the account console. | High |
| **P1** | **Prefer managed tables:** 377 external location(s) configured, but the managed-vs-external table split is not verifiable from the API (external-location count is not a reliable proxy). Prefer UC managed tables for new tables. | High |
| **P1** | **BROWSE privilege for discovery:** Unity Catalog in use. Grant BROWSE privilege on catalogs to All account users for data discovery. | High |
| **P1** | **Inference tables / payload logging for AI endpoints:** 9/27 LLM endpoint(s) log payloads. Enable inference tables on the remaining endpoints. | High |

---

## Pillar 2: Interoperability & Usability

### 2.1 Current State Findings

| Area | Finding | Status |
|------|---------|--------|
| **SQL Warehouses** | 4 warehouses (7 serverless) | Implemented |
| **Cluster Policies** | 38 policies (e.g. sample-resource-17, sample-resource-18, sample-resource-19) | Gap |
| **Git Integration** | 4 repositories connected | Implemented |

### 2.2 Assessment Against Best Practices

| # | Best Practice | Score (0-2) | Notes |
|---|---------------|:-----------:|-------|
| 1 | Standard integration patterns | **1** | Jobs present but no standard integration patterns detected. |
| 2 | Optimized connectors | **1** | Connector optimization not verified from API. Prefer native Delta connectors. |
| 3 | Certified partner tools | **1** | Partner tool certification not verified from API. |
| 4 | Reduce pipeline complexity | **2** | 16 DLT pipelines reduce complexity. |
| 5 | Use IaC | **1** | IaC usage not detected from API. Document Terraform/CI usage. |
| 6 | Open data formats | **2** | Delta Lake (open format) used via Unity Catalog. |
| 7 | Secure sharing | **2** | Delta Sharing configured (INTERNAL_AND_EXTERNAL) for secure sharing. |
| 8 | Open ML standards | **2** | Open ML standards in use: 63 MLflow model(s) in Unity Catalog (32 serving endpoints). |
| 9 | Self-service | **2** | 4 SQL Warehouses available for self-service analytics. |
| 10 | Serverless compute | **2** | 7 SQL warehouse(s) with Pro or serverless compute. |
| 11 | Predefined compute templates | **1** | 38 cluster policies — too many to be standardized templates. Consolidate. |
| 12 | AI productivity | **1** | AI productivity not verifiable from API. Verify assistant enablement. |
| 13 | Reusable data products | **1** | Delta Sharing configured. Define reusable data products. |
| 14 | Semantic consistency | **1** | Semantic consistency not verifiable from API. Use Databricks SQL semantic layer. |
| 15 | UC for discovery | **2** | Unity Catalog 'acme_metastore' enables data discovery. |

### 2.3 Recommendations (Prioritized)

| Priority | Recommendation | Impact |
|----------|---------------|--------|
| **P1** | **Standard integration patterns:** Jobs present but no standard integration patterns detected. | High |
| **P1** | **Optimized connectors:** Connector optimization not verified from API. Prefer native Delta connectors. | High |
| **P1** | **Certified partner tools:** Partner tool certification not verified from API. | High |
| **P1** | **Use IaC:** IaC usage not detected from API. Document Terraform/CI usage. | High |
| **P1** | **Predefined compute templates:** 38 cluster policies — too many to be standardized templates. Consolidate. | High |
| **P1** | **AI productivity:** AI productivity not verifiable from API. Verify assistant enablement. | High |
| **P1** | **Reusable data products:** Delta Sharing configured. Define reusable data products. | High |
| **P1** | **Semantic consistency:** Semantic consistency not verifiable from API. Use Databricks SQL semantic layer. | High |

---

## Pillar 3: Operational Excellence

### 3.1 Current State Findings

| Area | Finding | Status |
|------|---------|--------|
| **Job Management** | 11 jobs configured | Implemented |
| **DLT Pipelines** | 16 pipelines | Implemented |
| **Workspace Hygiene** | 230 untitled notebooks | Gap |
| **Instance Pools** | 2 pools configured | Implemented |

### 3.2 Assessment Against Best Practices

| # | Best Practice | Score (0-2) | Notes |
|---|---------------|:-----------:|-------|
| 1 | Dedicated ops team | **1** | Cannot verify org structure from API. Document dedicated ops ownership. |
| 2 | Enterprise SCM | **2** | Repos/SCM configured (4 repos). |
| 3 | Standardize CI/CD | **1** | 11 jobs present but none use Git source. Use Repos and Git-backed job tasks for CI/CD. |
| 4 | MLOps processes | **1** | 32 serving endpoints; Model Registry not verifiable from API. Register models in MLflow. |
| 5 | Environment isolation | **2** | Multiple catalogs; environment isolation via UC. |
| 6 | Catalog strategy | **2** | Catalog strategy in place with multiple catalogs. |
| 7 | IaC deployments | **1** | IaC usage not detected from API. Document Terraform/CI usage. |
| 8 | Standardize compute | **2** | 38 cluster policies defined for standardized compute. |
| 9 | Automated workflows | **2** | 11 automated jobs. |
| 10 | Event-driven ingestion | **1** | Event-driven ingestion not verifiable from API. Use Auto Loader. |
| 11 | ETL frameworks | **2** | Delta Live Tables in use (16 pipelines). |
| 12 | Deploy-code ML | **2** | Model serving endpoints in use (32 endpoints). |
| 13 | Model registry | **1** | 32 serving endpoints; Model Registry not verifiable from API. Register models in MLflow. |
| 14 | Automate experiment tracking | **1** | Experiment tracking not verifiable from API. Use MLflow for experiment tracking. |
| 15 | Reuse ML infra | **1** | ML infra reuse not verifiable from API. Use job clusters. |
| 16 | Declarative management | **2** | Delta Live Tables in use (16 pipelines). |
| 17 | Service limits | **1** | Service limits not verifiable from API. Document capacity limits. |
| 18 | Capacity planning | **1** | Capacity planning not verifiable from API. Document planning process. |
| 19 | Monitoring processes | **1** | Jobs/pipelines present; monitoring not fully verifiable from API. Configure alerts and dashboards. |
| 20 | Platform monitoring tools | **2** | SQL Warehouses enable platform monitoring (4 warehouses). |
| 21 | Automated rollbacks | **0** | No Git-backed jobs detected. Implement CI/CD with automated rollback mechanisms. |
| 22 | Workload identity federation | **1** | 4298 service principal(s) exist. Verify workload identity federation for CI/CD auth (eliminates PATs). |
| 23 | Restart long-running clusters | **2** | No long-running interactive clusters detected. |
| 24 | Cluster utilization efficiency (deep) | **2** | 40 active clusters with avg 151h uptime/30d. Utilization looks reasonable. |

### 3.3 Recommendations (Prioritized)

| Priority | Recommendation | Impact |
|----------|---------------|--------|
| **P0** | **Automated rollbacks:** No Git-backed jobs detected. Implement CI/CD with automated rollback mechanisms. | Critical |
| **P1** | **Dedicated ops team:** Cannot verify org structure from API. Document dedicated ops ownership. | High |
| **P1** | **Standardize CI/CD:** 11 jobs present but none use Git source. Use Repos and Git-backed job tasks for CI/CD. | High |
| **P1** | **MLOps processes:** 32 serving endpoints; Model Registry not verifiable from API. Register models in MLflow. | High |
| **P1** | **IaC deployments:** IaC usage not detected from API. Document Terraform/CI usage. | High |
| **P1** | **Event-driven ingestion:** Event-driven ingestion not verifiable from API. Use Auto Loader. | High |
| **P1** | **Model registry:** 32 serving endpoints; Model Registry not verifiable from API. Register models in MLflow. | High |
| **P1** | **Automate experiment tracking:** Experiment tracking not verifiable from API. Use MLflow for experiment tracking. | High |
| **P1** | **Reuse ML infra:** ML infra reuse not verifiable from API. Use job clusters. | High |
| **P1** | **Service limits:** Service limits not verifiable from API. Document capacity limits. | High |
| **P1** | **Capacity planning:** Capacity planning not verifiable from API. Document planning process. | High |
| **P1** | **Monitoring processes:** Jobs/pipelines present; monitoring not fully verifiable from API. Configure alerts and dashboards. | High |
| **P1** | **Workload identity federation:** 4298 service principal(s) exist. Verify workload identity federation for CI/CD auth (eliminates PATs). | High |

---

## Pillar 4: Security, Compliance & Privacy

### 4.1 Current State Findings

| Area | Finding | Status |
|------|---------|--------|
| **DBFS File Browser** | **ENABLED** — data exfiltration risk | Critical Gap |
| **Results Downloading** | **ENABLED** — consider disabling for production | Gap |
| **Notebook Export** | **ENABLED** — IP protection concern | Gap |
| **Token Lifetime** | Max **53 days** | Gap |
| **IP Access Lists** | **None configured** | Gap |

### 4.2 Assessment Against Best Practices

| # | Best Practice | Score (0-2) | Notes |
|---|---------------|:-----------:|-------|
| 1 | Least privilege IAM | **2** | Unity Catalog enables centralized least-privilege IAM. |
| 2 | Data protection transit/rest | **1** | Verify AWS SSE-S3/SSE-KMS encryption at rest and TLS in transit. Consider customer-managed keys (CMK). |
| 3 | Network security | **1** | IP access lists not detected. Configure network restrictions. |
| 4 | Shared responsibility | **1** | Shared responsibility model not verifiable from API. Document cloud security. |
| 5 | Compliance requirements | **1** | Some workspace security settings configured; review DBFS browser, IP access lists for full compliance. |
| 6 | System security monitoring | **1** | System tables access not verifiable from API. Enable for security monitoring. |
| 7 | Generic controls | **1** | Workspace security settings present; consider disabling DBFS file browser for compliance. |
| 8 | SSO configuration | **1** | Identities present but IdP sync is not verifiable from the workspace API. Confirm SSO via your identity provider (Okta, AAD, etc.) (AWS) in the account console. |
| 9 | SCIM provisioning | **1** | SCIM sync is not verifiable from the workspace API (1666 group(s), 0 user(s) present but no externalId returned). SCIM is typically configured at the account level, where externalId is not exposed to this endpoint — verify SCIM provisioning in the account console before treating this as a gap. |
| 10 | Service principals for automation | **2** | 4298 service principals configured for automation. |
| 11 | Customer-managed VPC | **1** | IP access lists enabled (AWS). Network isolation (customer-managed VPC with AWS PrivateLink) is configured at the account/deployment level and is not verifiable from the workspace API; confirm in the account console. |
| 12 | Restrict DBFS root data storage | **0** | DBFS file browser is ENABLED. Disable immediately and migrate data out of DBFS root. |
| 13 | Guardrails on LLM endpoints | **1** | 2/27 LLM endpoint(s) have guardrails. Add PII/safety guardrails to the remaining endpoints. |
| 14 | No plaintext credentials on external-model endpoints | **2** | 2 external-model endpoint(s) reference credentials via secret scopes (no plaintext keys). |
| 15 | Failed login monitoring (deep) | **0** | 315 failed login attempts in 30 days. Possible brute-force. Enable IP access lists and review. |
| 16 | Permission change audit (deep) | **0** | 522057 permission changes in 30 days (of 277430572 total events). High churn — review governance. |

### 4.3 Recommendations (Prioritized)

| Priority | Recommendation | Impact |
|----------|---------------|--------|
| **P0** | **Restrict DBFS root data storage:** DBFS file browser is ENABLED. Disable immediately and migrate data out of DBFS root. | Critical |
| **P0** | **Failed login monitoring (deep):** 315 failed login attempts in 30 days. Possible brute-force. Enable IP access lists and review. | Critical |
| **P0** | **Permission change audit (deep):** 522057 permission changes in 30 days (of 277430572 total events). High churn — review governance. | Critical |
| **P1** | **Data protection transit/rest:** Verify AWS SSE-S3/SSE-KMS encryption at rest and TLS in transit. Consider customer-managed keys (CMK). | High |
| **P1** | **Network security:** IP access lists not detected. Configure network restrictions. | High |
| **P1** | **Shared responsibility:** Shared responsibility model not verifiable from API. Document cloud security. | High |
| **P1** | **Compliance requirements:** Some workspace security settings configured; review DBFS browser, IP access lists for full compliance. | High |
| **P1** | **System security monitoring:** System tables access not verifiable from API. Enable for security monitoring. | High |
| **P1** | **Generic controls:** Workspace security settings present; consider disabling DBFS file browser for compliance. | High |
| **P1** | **SSO configuration:** Identities present but IdP sync is not verifiable from the workspace API. Confirm SSO via your identity provider (Okta, AAD, etc.) (AWS) in the account console. | High |
| **P1** | **SCIM provisioning:** SCIM sync is not verifiable from the workspace API (1666 group(s), 0 user(s) present but no externalId returned). SCIM is typically configured at the account level, where externalId is not exposed to this endpoint — verify SCIM provisioning in the account console before treating this as a gap. | High |
| **P1** | **Customer-managed VPC:** IP access lists enabled (AWS). Network isolation (customer-managed VPC with AWS PrivateLink) is configured at the account/deployment level and is not verifiable from the workspace API; confirm in the account console. | High |
| **P1** | **Guardrails on LLM endpoints:** 2/27 LLM endpoint(s) have guardrails. Add PII/safety guardrails to the remaining endpoints. | High |

---

## Pillar 5: Reliability

### 5.1 Current State Findings

| Area | Finding | Status |
|------|---------|--------|
| **Compute** | 2 clusters (0 Photon-enabled) | Gap |
| **Job Resilience** | **No jobs have retries configured** | Gap |
| **Model Serving** | 32 serving endpoints | Implemented |

### 5.2 Assessment Against Best Practices

| # | Best Practice | Score (0-2) | Notes |
|---|---------------|:-----------:|-------|
| 1 | ACID format | **1** | Unity Catalog + Delta Lake in use but cannot verify all tables use Delta format from API. |
| 2 | Resilient engine | **1** | Photon not detected on clusters. Use Photon for better resilience. |
| 3 | Rescue invalid data | **1** | Rescue invalid data pattern not verifiable from pipeline metadata. Use Auto Loader rescued column. |
| 4 | Auto retries | **1** | Jobs present but none have max_retries > 0. Configure retries for resilience. |
| 5 | Scalable serving | **2** | Model serving endpoints in use (32 endpoints). |
| 6 | Managed services | **2** | Managed services (jobs, DLT, SQL) in use. |
| 7 | Layered storage | **1** | Multiple catalogs; schema layering not verifiable from API. Verify bronze/silver/gold. |
| 8 | Reduce redundancy | **1** | Data redundancy not verifiable from API. Document deduplication strategy. |
| 9 | Active schema mgmt | **1** | DLT pipelines present; schema constraints not verifiable from API. Use Delta constraints. |
| 10 | Constraints/expectations | **1** | DLT pipelines present; schema constraints not verifiable from API. Use Delta constraints. |
| 11 | Data-centric ML | **1** | Feature Store not verifiable from API. Use for data-centric ML. |
| 12 | ETL autoscaling | **1** | DLT pipelines present but autoscaling not enabled. Enable on DLT pipelines. |
| 13 | SQL warehouse autoscaling | **2** | 7 SQL warehouse(s) with multi-cluster or serverless autoscaling. |
| 14 | Regular backups | **1** | Backup strategy not verifiable from API. Document Delta clone/backup process. |
| 15 | Streaming recovery | **1** | DLT checkpoints used by default; storage config not verifiable from API. |
| 16 | Time travel recovery | **2** | Unity Catalog with Delta Lake enables time travel recovery. |
| 17 | Job automation recovery | **1** | Jobs present but none have max_retries > 0. Configure retries for resilience. |
| 18 | DR pattern | **1** | DR pattern not verifiable from API. Document replication and RTO/RPO. |
| 19 | Service principal job ownership | **1** | Service principals exist but job ownership not verified. Transfer production job ownership to service principals. |
| 20 | Provisioned throughput for production LLM serving | **1** | 27 LLM endpoint(s), none clearly production-named. Use provisioned throughput for production SLAs (heuristic by name). |
| 21 | Job success rate (deep) | **0** | Job success rate is 53.5256% (7892 failures out of 93503 runs). Investigate and add retries. |
| 22 | Recurring job failures (deep) | **0** | 10 jobs have 5+ failures in 30 days. Chronic reliability issue — investigate root causes. |

### 5.3 Recommendations (Prioritized)

| Priority | Recommendation | Impact |
|----------|---------------|--------|
| **P0** | **Job success rate (deep):** Job success rate is 53.5256% (7892 failures out of 93503 runs). Investigate and add retries. | Critical |
| **P0** | **Recurring job failures (deep):** 10 jobs have 5+ failures in 30 days. Chronic reliability issue — investigate root causes. | Critical |
| **P1** | **ACID format:** Unity Catalog + Delta Lake in use but cannot verify all tables use Delta format from API. | High |
| **P1** | **Resilient engine:** Photon not detected on clusters. Use Photon for better resilience. | High |
| **P1** | **Rescue invalid data:** Rescue invalid data pattern not verifiable from pipeline metadata. Use Auto Loader rescued column. | High |
| **P1** | **Auto retries:** Jobs present but none have max_retries > 0. Configure retries for resilience. | High |
| **P1** | **Layered storage:** Multiple catalogs; schema layering not verifiable from API. Verify bronze/silver/gold. | High |
| **P1** | **Reduce redundancy:** Data redundancy not verifiable from API. Document deduplication strategy. | High |
| **P1** | **Active schema mgmt:** DLT pipelines present; schema constraints not verifiable from API. Use Delta constraints. | High |
| **P1** | **Constraints/expectations:** DLT pipelines present; schema constraints not verifiable from API. Use Delta constraints. | High |
| **P1** | **Data-centric ML:** Feature Store not verifiable from API. Use for data-centric ML. | High |
| **P1** | **ETL autoscaling:** DLT pipelines present but autoscaling not enabled. Enable on DLT pipelines. | High |
| **P1** | **Regular backups:** Backup strategy not verifiable from API. Document Delta clone/backup process. | High |
| **P1** | **Streaming recovery:** DLT checkpoints used by default; storage config not verifiable from API. | High |
| **P1** | **Job automation recovery:** Jobs present but none have max_retries > 0. Configure retries for resilience. | High |
| **P1** | **DR pattern:** DR pattern not verifiable from API. Document replication and RTO/RPO. | High |
| **P1** | **Service principal job ownership:** Service principals exist but job ownership not verified. Transfer production job ownership to service principals. | High |
| **P1** | **Provisioned throughput for production LLM serving:** 27 LLM endpoint(s), none clearly production-named. Use provisioned throughput for production SLAs (heuristic by name). | High |

---

## Pillar 6: Performance Efficiency

### 6.1 Current State Findings

| Area | Finding | Status |
|------|---------|--------|
| **SQL Warehouses** | 4 warehouses (7 Photon, 7 serverless) | Implemented |
| **Clusters** | 2 clusters (3 with autoscaling) | Implemented |

### 6.2 Assessment Against Best Practices

| # | Best Practice | Score (0-2) | Notes |
|---|---------------|:-----------:|-------|
| 1 | Scaling | **2** | 3 cluster(s) with autoscaling enabled. |
| 2 | Serverless | **2** | 7 SQL warehouse(s) with Pro or serverless compute. |
| 3 | Data patterns | **1** | Data optimization (OPTIMIZE/Z-ORDER) not verifiable from API. Use OPTIMIZE and Z-ORDER. |
| 4 | Parallel computation | **1** | Clusters present but worker count not available. Scale workers for throughput. |
| 5 | Execution chain | **2** | Delta Live Tables in use (16 pipelines). |
| 6 | Larger clusters | **2** | Clusters sized for scale (max 3 workers). Right-size for workload. |
| 7 | Native Spark | **1** | Photon not detected on clusters. Use Photon for better resilience. |
| 8 | Native engines | **1** | Photon not detected on clusters. Use Photon for better resilience. |
| 9 | Hardware awareness | **2** | Multiple instance types in use (3 types) — workload-aware selection. |
| 10 | Caching | **1** | Caching not verifiable from API. Use Delta Cache for repeated reads. |
| 11 | Compaction | **1** | Data optimization (OPTIMIZE/Z-ORDER) not verifiable from API. Use OPTIMIZE and Z-ORDER. |
| 12 | Data skipping | **1** | Data optimization (OPTIMIZE/Z-ORDER) not verifiable from API. Use OPTIMIZE and Z-ORDER. |
| 13 | Avoid over-partition | **1** | Partitioning strategy not verifiable from API. Avoid over-partitioning. |
| 14 | Join optimization | **1** | Join optimization (AQE) enabled by default. Verify skew handling. |
| 15 | Table statistics | **1** | Table statistics not verifiable from API. Run ANALYZE TABLE. |
| 16 | Test on production data | **1** | Testing strategy not verifiable from API. Use clone for testing. |
| 17 | Prewarming | **2** | 7 SQL warehouse(s) with Photon enabled for latency-sensitive workloads. |
| 18 | Identify bottlenecks | **2** | SQL Warehouses enable query history for bottleneck identification. |
| 19 | Monitor queries | **2** | SQL Warehouses enable query monitoring (4 warehouses). |
| 20 | Monitor streaming | **2** | DLT pipelines enable streaming monitoring (16 pipelines). |
| 21 | Monitor jobs | **2** | Jobs enable cluster/job monitoring (11 jobs). |
| 22 | Predictive optimization | **1** | Unity Catalog in use. Enable predictive optimization for auto OPTIMIZE/VACUUM on managed tables. |
| 23 | Liquid clustering | **1** | Unity Catalog in use. Migrate from Z-ORDER/partitioning to liquid clustering for better performance. |
| 24 | Graviton instance types | **0** | No Graviton instances across 4 clusters (AWS). Adopt for best price-to-performance. |
| 25 | Standard access mode | **2** | 3/4 cluster(s) use standard/shared access mode with UC isolation. |
| 26 | Query failure rate (deep) | **0** | Query failure rate is 12.0607% (899642 of 5243829 queries). Investigate failing queries. |
| 27 | Slow query prevalence (deep) | **2** | Only 18311 queries (of 5243829) exceeded 5 min. P95: 7.1s. Performance healthy. |
| 28 | Warehouse utilization balance (deep) | **2** | Queries distributed across 10 warehouses (top: 13%). |

### 6.3 Recommendations (Prioritized)

| Priority | Recommendation | Impact |
|----------|---------------|--------|
| **P0** | **Graviton instance types:** No Graviton instances across 4 clusters (AWS). Adopt for best price-to-performance. | Critical |
| **P0** | **Query failure rate (deep):** Query failure rate is 12.0607% (899642 of 5243829 queries). Investigate failing queries. | Critical |
| **P1** | **Data patterns:** Data optimization (OPTIMIZE/Z-ORDER) not verifiable from API. Use OPTIMIZE and Z-ORDER. | High |
| **P1** | **Parallel computation:** Clusters present but worker count not available. Scale workers for throughput. | High |
| **P1** | **Native Spark:** Photon not detected on clusters. Use Photon for better resilience. | High |
| **P1** | **Native engines:** Photon not detected on clusters. Use Photon for better resilience. | High |
| **P1** | **Caching:** Caching not verifiable from API. Use Delta Cache for repeated reads. | High |
| **P1** | **Compaction:** Data optimization (OPTIMIZE/Z-ORDER) not verifiable from API. Use OPTIMIZE and Z-ORDER. | High |
| **P1** | **Data skipping:** Data optimization (OPTIMIZE/Z-ORDER) not verifiable from API. Use OPTIMIZE and Z-ORDER. | High |
| **P1** | **Avoid over-partition:** Partitioning strategy not verifiable from API. Avoid over-partitioning. | High |
| **P1** | **Join optimization:** Join optimization (AQE) enabled by default. Verify skew handling. | High |
| **P1** | **Table statistics:** Table statistics not verifiable from API. Run ANALYZE TABLE. | High |
| **P1** | **Test on production data:** Testing strategy not verifiable from API. Use clone for testing. | High |
| **P1** | **Predictive optimization:** Unity Catalog in use. Enable predictive optimization for auto OPTIMIZE/VACUUM on managed tables. | High |
| **P1** | **Liquid clustering:** Unity Catalog in use. Migrate from Z-ORDER/partitioning to liquid clustering for better performance. | High |

---

## Pillar 7: Cost Optimization

### 7.1 Current State Findings

| Area | Finding | Status |
|------|---------|--------|
| **Clusters** | **2 clusters** (0 running), **no auto-termination configured** | Gap |
| **Cost Tags** | 4/4 clusters tagged | Implemented |
| **SQL Warehouses** | 4 warehouses (7 auto-stop, 7 serverless) | Implemented |
| **Cluster Policies** | 38 policies for cost controls | Implemented |

### 7.2 Assessment Against Best Practices

| # | Best Practice | Score (0-2) | Notes |
|---|---------------|:-----------:|-------|
| 1 | Optimized formats | **1** | Unity Catalog + Delta Lake in use but cannot verify all tables use Delta format from API. |
| 2 | Job clusters | **1** | Jobs present but not using job clusters. Use job clusters for cost efficiency. |
| 3 | SQL for SQL | **2** | SQL Warehouses used for SQL workloads (4 warehouses). |
| 4 | Up-to-date runtimes | **2** | 4/4 cluster(s) on recent runtimes (>=14.x). |
| 5 | GPU right workloads | **2** | No GPU clusters — cost-efficient instance selection. |
| 6 | Serverless | **2** | 7 SQL warehouse(s) with Pro or serverless compute. |
| 7 | Right instance type | **2** | Cluster policies help enforce right instance types (38 policies). |
| 8 | Efficient compute size | **2** | 3 cluster(s) with autoscaling for right-sizing. |
| 9 | Performance engines | **1** | Photon not detected on clusters. Use Photon for better resilience. |
| 10 | Auto-scaling | **2** | Auto-scaling enabled: 3 cluster(s), 2 warehouse(s) with multi-cluster. |
| 11 | Auto-termination | **1** | Auto-termination partial: 0/1 interactive cluster(s), 7/7 warehouse(s). Configure auto-stop on the rest. |
| 12 | Cluster policies costs | **2** | 38 cluster policies defined for standardized compute. |
| 13 | Monitor costs | **1** | Cost tagging not verifiable from API. Add tags for cost allocation. |
| 14 | Tag clusters | **2** | 1/1 interactive cluster(s) carry custom tags for cost allocation. |
| 15 | Chargeback | **1** | Cost tagging not verifiable from API. Add tags for cost allocation. |
| 16 | Cost reports | **1** | Cost tagging not verifiable from API. Add tags for cost allocation. |
| 17 | Streaming balance | **1** | Streaming cost/throughput balance not verifiable. Tune micro-batch size. |
| 18 | On-demand vs reserved | **1** | Evaluate AWS Reserved Instances or Savings Plans for predictable workloads. |
| 19 | Spot instance strategy | **2** | 1/4 cluster(s) use Spot instances for cost savings (AWS). |
| 20 | Budget alerts | **1** | Configure Databricks budget alerts in Account Console. Also set up AWS Cost Explorer and billing alarms. |
| 21 | Idle cluster waste (deep) | **0** | 10 idle clusters totaling 2383h in 30d. Top: sample-resource-147, sample-resource-149, sample-resource-150. Terminate or auto-stop. |
| 22 | Cost trend analysis (deep) | **2** | DBU spend stable or declining (-24.2% change, 3789925 DBUs/30d). |
| 23 | DBU concentration risk (deep) | **2** | DBU spend well distributed across clusters (top cluster: 1%). |

### 7.3 Recommendations (Prioritized)

| Priority | Recommendation | Impact |
|----------|---------------|--------|
| **P0** | **Idle cluster waste (deep):** 10 idle clusters totaling 2383h in 30d. Top: sample-resource-147, sample-resource-149, sample-resource-150. Terminate or auto-stop. | Critical |
| **P1** | **Optimized formats:** Unity Catalog + Delta Lake in use but cannot verify all tables use Delta format from API. | High |
| **P1** | **Job clusters:** Jobs present but not using job clusters. Use job clusters for cost efficiency. | High |
| **P1** | **Performance engines:** Photon not detected on clusters. Use Photon for better resilience. | High |
| **P1** | **Auto-termination:** Auto-termination partial: 0/1 interactive cluster(s), 7/7 warehouse(s). Configure auto-stop on the rest. | High |
| **P1** | **Monitor costs:** Cost tagging not verifiable from API. Add tags for cost allocation. | High |
| **P1** | **Chargeback:** Cost tagging not verifiable from API. Add tags for cost allocation. | High |
| **P1** | **Cost reports:** Cost tagging not verifiable from API. Add tags for cost allocation. | High |
| **P1** | **Streaming balance:** Streaming cost/throughput balance not verifiable. Tune micro-batch size. | High |
| **P1** | **On-demand vs reserved:** Evaluate AWS Reserved Instances or Savings Plans for predictable workloads. | High |
| **P1** | **Budget alerts:** Configure Databricks budget alerts in Account Console. Also set up AWS Cost Explorer and billing alarms. | High |

---

## Summary of Critical Findings (Score = 0)

| # | Finding | Pillar | Risk Level |
|---|---------|--------|------------|
| 1 | Idle cluster waste (deep): 10 idle clusters totaling 2383h in 30d. Top: sample-resource-147, sample-resource-149, sample-resource-150. Terminate or auto-stop. | Cost Optimization | **Critical** |
| 2 | Add descriptions: 2411 catalogs — likely many lack descriptions. Add comments to schemas and tables. | Data & AI Governance | **Critical** |
| 3 | Automated rollbacks: No Git-backed jobs detected. Implement CI/CD with automated rollback mechanisms. | Operational Excellence | **Critical** |
| 4 | Graviton instance types: No Graviton instances across 4 clusters (AWS). Adopt for best price-to-performance. | Performance Efficiency | **Critical** |
| 5 | Query failure rate (deep): Query failure rate is 12.0607% (899642 of 5243829 queries). Investigate failing queries. | Performance Efficiency | **Critical** |
| 6 | Job success rate (deep): Job success rate is 53.5256% (7892 failures out of 93503 runs). Investigate and add retries. | Reliability | **Critical** |
| 7 | Recurring job failures (deep): 10 jobs have 5+ failures in 30 days. Chronic reliability issue — investigate root causes. | Reliability | **Critical** |
| 8 | Restrict DBFS root data storage: DBFS file browser is ENABLED. Disable immediately and migrate data out of DBFS root. | Security, Compliance & Privacy | **Critical** |
| 9 | Failed login monitoring (deep): 315 failed login attempts in 30 days. Possible brute-force. Enable IP access lists and review. | Security, Compliance & Privacy | **Critical** |
| 10 | Permission change audit (deep): 522057 permission changes in 30 days (of 277430572 total events). High churn — review governance. | Security, Compliance & Privacy | **Critical** |


## Recommended Remediation Roadmap

### Phase 1: Immediate Actions (Week 1-2) - Quick Wins

| Action | Pillar | Effort | Impact |
|--------|--------|--------|--------|
| Add descriptions: 2411 catalogs — likely many lack descriptions. Add comments to schemas and tables. | Data & AI Governance | Low-Medium | Critical |
| Automated rollbacks: No Git-backed jobs detected. Implement CI/CD with automated rollback mechanisms. | Operational Excellence | Low-Medium | Critical |
| Restrict DBFS root data storage: DBFS file browser is ENABLED. Disable immediately and migrate data out of DBFS root. | Security, Compliance & Privacy | Low-Medium | Critical |
| Failed login monitoring (deep): 315 failed login attempts in 30 days. Possible brute-force. Enable IP access lists and review. | Security, Compliance & Privacy | Low-Medium | Critical |
| Permission change audit (deep): 522057 permission changes in 30 days (of 277430572 total events). High churn — review governance. | Security, Compliance & Privacy | Low-Medium | Critical |
| Job success rate (deep): Job success rate is 53.5256% (7892 failures out of 93503 runs). Investigate and add retries. | Reliability | Low-Medium | Critical |

### Phase 2: Foundation (Week 3-6) - Governance & Security

| Action | Pillar | Effort | Impact |
|--------|--------|--------|--------|
| Recurring job failures (deep): 10 jobs have 5+ failures in 30 days. Chronic reliability issue — investigate root causes. | Reliability | Medium | High |
| Graviton instance types: No Graviton instances across 4 clusters (AWS). Adopt for best price-to-performance. | Performance Efficiency | Medium | High |
| Query failure rate (deep): Query failure rate is 12.0607% (899642 of 5243829 queries). Investigate failing queries. | Performance Efficiency | Medium | High |
| Idle cluster waste (deep): 10 idle clusters totaling 2383h in 30d. Top: sample-resource-147, sample-resource-149, sample-resource-150. Terminate or auto-stop. | Cost Optimization | Medium | High |

### Phase 3: Operational Maturity (Week 7-12) - Automation & Monitoring

| Action | Pillar | Effort | Impact |
|--------|--------|--------|--------|
| Improve: Establish governance process | Data & AI Governance | Medium-High | High |
| Improve: Track lineage | Data & AI Governance | Medium-High | High |
| Improve: Allow discovery | Data & AI Governance | Medium-High | High |
| Improve: Configure audit logging | Data & AI Governance | Medium-High | High |
| Improve: Audit events | Data & AI Governance | Medium-High | High |
| Improve: Define DQ standards | Data & AI Governance | Medium-High | High |
| Improve: Use DQ tools | Data & AI Governance | Medium-High | High |
| Improve: Enforce standardized formats | Data & AI Governance | Medium-High | High |

### Phase 4: Optimization (Week 13-20) - Performance & Advanced

| Action | Pillar | Effort | Impact |
|--------|--------|--------|--------|
| Optimize: Account-level group management | Data & AI Governance | High | Medium |
| Optimize: Prefer managed tables | Data & AI Governance | High | Medium |
| Optimize: BROWSE privilege for discovery | Data & AI Governance | High | Medium |
| Optimize: Inference tables / payload logging for AI endpoints | Data & AI Governance | High | Medium |
| Optimize: Standard integration patterns | Interoperability & Usability | High | Medium |
| Optimize: Optimized connectors | Interoperability & Usability | High | Medium |


## Requires Manual Verification

The following **41 best practices** could not be automatically assessed from the available API data or system tables. These require manual review by the customer and SA together.

| # | Pillar | Best Practice | Reason |
|---|--------|--------------|--------|
| 1 | Cost Optimization | Optimized formats | Unity Catalog + Delta Lake in use but cannot verify all tables use Delta format from API. |
| 2 | Cost Optimization | Monitor costs | Cost tagging not verifiable from API. Add tags for cost allocation. |
| 3 | Cost Optimization | Chargeback | Cost tagging not verifiable from API. Add tags for cost allocation. |
| 4 | Cost Optimization | Cost reports | Cost tagging not verifiable from API. Add tags for cost allocation. |
| 5 | Cost Optimization | Streaming balance | Streaming cost/throughput balance not verifiable. Tune micro-batch size. |
| 6 | Data & AI Governance | Enforce standardized formats | Unity Catalog + Delta Lake in use but cannot verify all tables use Delta format from API. |
| 7 | Data & AI Governance | Account-level group management | 1666 group(s) present but IdP sync is not verifiable from the workspace API (externalId is exposed only at the account level). Verify account-level, IdP-synced groups in the account console. |
| 8 | Data & AI Governance | Prefer managed tables | 377 external location(s) configured, but the managed-vs-external table split is not verifiable from the API (external-location count is not a reliable proxy). Prefer UC managed tables for new tables. |
| 9 | Interoperability & Usability | AI productivity | AI productivity not verifiable from API. Verify assistant enablement. |
| 10 | Interoperability & Usability | Semantic consistency | Semantic consistency not verifiable from API. Use Databricks SQL semantic layer. |
| 11 | Operational Excellence | Dedicated ops team | Cannot verify org structure from API. Document dedicated ops ownership. |
| 12 | Operational Excellence | MLOps processes | 32 serving endpoints; Model Registry not verifiable from API. Register models in MLflow. |
| 13 | Operational Excellence | Event-driven ingestion | Event-driven ingestion not verifiable from API. Use Auto Loader. |
| 14 | Operational Excellence | Model registry | 32 serving endpoints; Model Registry not verifiable from API. Register models in MLflow. |
| 15 | Operational Excellence | Automate experiment tracking | Experiment tracking not verifiable from API. Use MLflow for experiment tracking. |
| 16 | Operational Excellence | Reuse ML infra | ML infra reuse not verifiable from API. Use job clusters. |
| 17 | Operational Excellence | Service limits | Service limits not verifiable from API. Document capacity limits. |
| 18 | Operational Excellence | Capacity planning | Capacity planning not verifiable from API. Document planning process. |
| 19 | Operational Excellence | Monitoring processes | Jobs/pipelines present; monitoring not fully verifiable from API. Configure alerts and dashboards. |
| 20 | Performance Efficiency | Data patterns | Data optimization (OPTIMIZE/Z-ORDER) not verifiable from API. Use OPTIMIZE and Z-ORDER. |
| 21 | Performance Efficiency | Caching | Caching not verifiable from API. Use Delta Cache for repeated reads. |
| 22 | Performance Efficiency | Compaction | Data optimization (OPTIMIZE/Z-ORDER) not verifiable from API. Use OPTIMIZE and Z-ORDER. |
| 23 | Performance Efficiency | Data skipping | Data optimization (OPTIMIZE/Z-ORDER) not verifiable from API. Use OPTIMIZE and Z-ORDER. |
| 24 | Performance Efficiency | Avoid over-partition | Partitioning strategy not verifiable from API. Avoid over-partitioning. |
| 25 | Performance Efficiency | Table statistics | Table statistics not verifiable from API. Run ANALYZE TABLE. |
| 26 | Performance Efficiency | Test on production data | Testing strategy not verifiable from API. Use clone for testing. |
| 27 | Reliability | ACID format | Unity Catalog + Delta Lake in use but cannot verify all tables use Delta format from API. |
| 28 | Reliability | Rescue invalid data | Rescue invalid data pattern not verifiable from pipeline metadata. Use Auto Loader rescued column. |
| 29 | Reliability | Layered storage | Multiple catalogs; schema layering not verifiable from API. Verify bronze/silver/gold. |
| 30 | Reliability | Reduce redundancy | Data redundancy not verifiable from API. Document deduplication strategy. |
| 31 | Reliability | Active schema mgmt | DLT pipelines present; schema constraints not verifiable from API. Use Delta constraints. |
| 32 | Reliability | Constraints/expectations | DLT pipelines present; schema constraints not verifiable from API. Use Delta constraints. |
| 33 | Reliability | Data-centric ML | Feature Store not verifiable from API. Use for data-centric ML. |
| 34 | Reliability | Regular backups | Backup strategy not verifiable from API. Document Delta clone/backup process. |
| 35 | Reliability | Streaming recovery | DLT checkpoints used by default; storage config not verifiable from API. |
| 36 | Reliability | DR pattern | DR pattern not verifiable from API. Document replication and RTO/RPO. |
| 37 | Security, Compliance & Privacy | Shared responsibility | Shared responsibility model not verifiable from API. Document cloud security. |
| 38 | Security, Compliance & Privacy | System security monitoring | System tables access not verifiable from API. Enable for security monitoring. |
| 39 | Security, Compliance & Privacy | SSO configuration | Identities present but IdP sync is not verifiable from the workspace API. Confirm SSO via your identity provider (Okta, AAD, etc.) (AWS) in the account console. |
| 40 | Security, Compliance & Privacy | SCIM provisioning | SCIM sync is not verifiable from the workspace API (1666 group(s), 0 user(s) present but no externalId returned). SCIM is typically configured at the account level, where externalId is not exposed to this endpoint — verify SCIM provisioning in the account console before treating this as a gap. |
| 41 | Security, Compliance & Privacy | Customer-managed VPC | IP access lists enabled (AWS). Network isolation (customer-managed VPC with AWS PrivateLink) is configured at the account/deployment level and is not verifiable from the workspace API; confirm in the account console. |

### How to Increase Coverage

| Action | Impact |
|--------|--------|
| Run with **workspace admin** access | Unlocks security config settings |
| Run with **metastore admin** access | Unlocks full catalog and governance data |
| Use `--deep` mode with system tables | Adds 11 operational best practices (cost, performance, reliability, security) |
| Manual review with SA | Addresses process/organizational checks (compliance, shared responsibility, SIEM) |

---

---

## Appendix A: Workspace Inventory Summary

| Resource | Count | Notes |
|----------|:-----:|-------|
| Catalogs | 2411 | Metastore: acme_metastore |
| External Locations | 377 | |
| Storage Credentials | 509 | |
| Running Clusters | 0 | |
| SQL Warehouses | 4 | |
| Cluster Policies | 38 | |
| Instance Pools | 2 | |
| Jobs | 11 | |
| DLT Pipelines | 16 | |
| Serving Endpoints | 32 | |
| Git Repos | 4 | |
| Secret Scopes | 0 | |
| Groups | 0 | |
| IP Access Lists | 0 | |
| Workspace Objects | 1109 | |

## Appendix B: Workspace Configuration

| Setting | Value | Recommendation |
|---------|-------|----------------|
| enableDbfsFileBrowser | **true** | Change to **false** — data exfiltration risk |
| enableExportNotebook | **true** | Consider **false** for IP protection |
| enableIpAccessLists | **true** | Good — current value is recommended |
| enableResultsDownloading | **true** | Consider **false** for production |
| maxTokenLifetimeDays | **53** | Reduce to **30 days** or less |

## Appendix C: Assessment Methodology

This assessment was conducted following the **Databricks Well-Architected Lakehouse Assessment Delivery Playbook**.

**Data Collection Methods:**
- Databricks CLI workspace queries
- REST API calls for configuration inspection
- Unity Catalog API for governance assessment

**Scoring Methodology:**
- **0 (Not Implemented):** Best practice not in place; no evidence of implementation
- **1 (Partially Implemented):** Some aspects of the best practice are in place but not fully adopted
- **2 (Fully Implemented):** Best practice fully adopted and actively maintained

Total API/CLI commands executed: 0
