---
description: WAL-E Databricks Well-Architected Lakehouse assessment tool usage and integration
globs: "**/*.py, **/wal_e/**/*"
alwaysApply: false
---

# WAL-E Assessment Tool Rule

## When to Use WAL-E

Apply this rule and suggest WAL-E when the user:

- Asks about **Databricks assessment**, **architecture review**, or **Well-Architected Lakehouse (WAL) framework**
- Wants to **evaluate a Databricks workspace** against best practices
- Needs an **automated scan** of governance, security, cost, or operational maturity
- Asks for **readout decks**, **assessment reports**, or **pillar scores** for a Databricks lakehouse
- References **99 best practices**, **7 pillars**, or **WAL Assessment Tool**
- Is a **Solutions Architect** preparing customer assessments or architecture reviews

## How to Run WAL-E

### CLI Commands

```bash
# Full assessment (collect → score → report)
python -m wal_e assess

# With options
python -m wal_e assess --profile customer-workspace --output ./results --format md csv pptx audit

# Validate workspace access before assessment
python -m wal_e validate

# Show setup guide for customer collaboration
python -m wal_e setup --guide
```

### Python API

```python
from wal_e.core.config import WalEConfig
from wal_e.core.engine import AssessmentEngine
from wal_e.framework.scoring import ScoringEngine

config = WalEConfig(profile_name="DEFAULT", output_dir="./assessment-results")
engine = AssessmentEngine(config)
result = engine.run_assessment()

# Score the collected data
scoring = ScoringEngine()
assessment = scoring.score_all(result.collected_data, config.workspace_host)
print(f"Overall: {assessment.overall_score}/2 | Maturity: {assessment.maturity_level}")
```

## Interpreting Results

### Scoring Scale (0–2)

| Score | Meaning | Action |
|-------|---------|--------|
| 0 | Not Implemented | Prioritize remediation |
| 1 | Partial | Expand coverage |
| 2 | Full | Document and maintain |

### Maturity Levels

| Overall Avg | Level | Description |
|-------------|-------|-------------|
| ≥ 1.75 | Optimized | Best practices widely adopted |
| ≥ 1.25 | Established | Good coverage, some gaps |
| ≥ 0.5 | Developing | Early adoption |
| < 0.5 | Beginning | Significant gaps |

### Output Files

- `WAL_Assessment_Readout.md` – Full report (all pillars)
- `WAL_Assessment_Scores.csv` – 99 best practices, scores, notes
- `WAL_Assessment_Presentation.pptx` – Executive deck
- `WAL_Assessment_Audit_Report.md` – API call evidence trail

## Customizing Scoring

1. **Adjust scoring functions**: Edit `src/wal_e/framework/scoring.py`
   - Each best practice has `_score_<id>(data) -> tuple[int, str]`
   - Return `(0|1|2, "finding_notes")`
2. **Add best practices**: Add to `src/wal_e/framework/pillars.py`, then add scorer in `scoring.py` and register in `SCORING_REGISTRY`
3. **Add custom thresholds**: Modify `_maturity_from_score()` in `scoring.py` for different maturity cutoffs

## Adding New Collectors

1. Create `src/wal_e/collectors/<name>.py`
2. Extend `BaseCollector` from `collectors.base`
3. Implement `collect() -> dict[str, Any]` using `run_cli_command()` or `run_api_call()`
4. Register in `AssessmentEngine._collectors` in `core/engine.py`

```python
from wal_e.collectors.base import BaseCollector

class MyCollector(BaseCollector):
    def collect(self) -> dict[str, Any]:
        data, ok = self.run_api_call("/api/2.0/my/endpoint")
        return {"my_data": data} if ok else {}
```

## Access Requirements

- **Databricks CLI** v0.200+ configured with a profile
- **Read-only** workspace permissions (admin recommended)
- Required APIs: clusters, SQL warehouses, Unity Catalog, jobs, pipelines, cluster policies, workspace conf, IP access lists, secrets

## Critical Constraints

- **Never modify** the customer workspace; WAL-E is read-only
- **Always validate** access before running full assessment
- **Include audit trail** in deliverables for compliance
- **Use parameterized/CLI** for workspace host and profile—no hardcoded credentials
