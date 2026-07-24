"""Tests for compute-hygiene scoring accuracy.

Covers the false-positive/false-negative fixes for how DLT (PIPELINE) and other
ephemeral clusters are handled, and how runtime currency is scored ratio-aware
with DLT runtime-image parsing.
"""

from wal_e.framework.scoring import (
    _interactive_clusters,
    _is_recent_runtime,
    _score_cost_004,
    _score_cost_011,
    _score_cost_014,
    _score_ops_023,
)


def _compute(clusters=None, warehouses=None):
    return {"ComputeCollector": {"clusters": clusters or [], "warehouses": warehouses or []}}


def test_is_recent_runtime_handles_dlt_prefix():
    assert _is_recent_runtime("dlt:17.3.12-delta-pipelines") is True
    assert _is_recent_runtime("15.4.x-scala2.12") is True
    assert _is_recent_runtime("14.0.x-scala2.12") is True
    assert _is_recent_runtime("13.3.x-scala2.12") is False
    assert _is_recent_runtime("dlt:9.1.0-x") is False
    assert _is_recent_runtime("") is False
    assert _is_recent_runtime(None) is False


def test_interactive_clusters_excludes_ephemeral():
    clusters = [
        {"cluster_name": "ui", "cluster_source": "UI"},
        {"cluster_name": "api", "cluster_source": "API"},
        {"cluster_name": "dlt", "cluster_source": "PIPELINE"},
        {"cluster_name": "job", "cluster_source": "JOB"},
    ]
    names = [c["cluster_name"] for c in _interactive_clusters(clusters)]
    assert names == ["ui", "api"]


def test_cost_004_ratio_aware_not_inflated_by_single_recent():
    clusters = [{"cluster_source": "UI", "spark_version": "15.4.x-scala2.12"}]
    clusters += [{"cluster_source": "UI", "spark_version": "12.2.x-scala2.12"} for _ in range(19)]
    score, note = _score_cost_004(_compute(clusters))
    assert score == 1
    assert "1/20" in note


def test_cost_004_dlt_runtimes_count_as_recent():
    clusters = [{"cluster_source": "UI", "spark_version": "15.4.x-scala2.12"}]
    clusters += [{"cluster_source": "PIPELINE", "spark_version": f"dlt:17.3.{i}-delta-pipelines"} for i in range(19)]
    score, note = _score_cost_004(_compute(clusters))
    assert score == 2
    assert "20/20" in note


def test_ops_023_ignores_dlt_clusters():
    # 19 DLT clusters without auto-term should not trigger a false negative.
    clusters = [{"cluster_source": "PIPELINE", "state": "RUNNING", "auto_termination_minutes": None} for _ in range(19)]
    clusters += [{"cluster_source": "UI", "state": "TERMINATED", "auto_termination_minutes": None}]
    score, note = _score_ops_023(_compute(clusters))
    assert score == 2
    assert "interactive" in note


def test_ops_023_flags_interactive_without_autoterm():
    clusters = [{"cluster_source": "UI", "state": "RUNNING", "auto_termination_minutes": None}]
    clusters += [{"cluster_source": "PIPELINE", "state": "RUNNING", "auto_termination_minutes": None} for _ in range(19)]
    score, note = _score_ops_023(_compute(clusters))
    assert score == 0
    assert "1 interactive" in note


def test_cost_011_partial_when_interactive_lacks_autoterm():
    clusters = [{"cluster_source": "UI", "auto_termination_minutes": None}]
    clusters += [{"cluster_source": "PIPELINE", "auto_termination_minutes": None} for _ in range(19)]
    warehouses = [{"auto_stop_mins": 10} for _ in range(6)] + [{"auto_stop_mins": 0}]
    score, note = _score_cost_011(_compute(clusters, warehouses))
    assert score == 1
    assert "0/1 interactive" in note


def test_cost_011_full_when_all_applicable_covered():
    clusters = [{"cluster_source": "UI", "auto_termination_minutes": 30}]
    warehouses = [{"auto_stop_mins": 10} for _ in range(3)]
    score, _ = _score_cost_011(_compute(clusters, warehouses))
    assert score == 2


def test_cost_014_scores_on_interactive_only():
    clusters = [{"cluster_source": "UI", "custom_tags": {"team": "ds"}}]
    clusters += [{"cluster_source": "PIPELINE", "custom_tags": {}} for _ in range(19)]
    score, note = _score_cost_014(_compute(clusters))
    assert score == 2
    assert "1/1 interactive" in note


def test_cost_014_untagged_interactive_scores_zero():
    clusters = [{"cluster_source": "UI", "custom_tags": {}}]
    score, _ = _score_cost_014(_compute(clusters))
    assert score == 0
