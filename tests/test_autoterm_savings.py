"""Tests for quantified auto-termination savings (Track B).

Covers the SystemTablesCollector aggregation + cloud-aware list-price join, and
the scoring headline that surfaces the dollar figure in cost-011 / cost-021.
"""

from wal_e.collectors.system_tables import SystemTablesCollector
from wal_e.framework.scoring import (
    _autoterm_headline,
    _score_cost_011,
    _score_cost_021,
)

_CANNED_ROWS = [
    {
        "cluster_id": "c1", "cluster_name": "alpha", "uptime_hrs": "100.0",
        "reclaim_hrs_at_30": "40.0", "total_dbus": "500", "avg_dbu_per_hr": "5.0",
        "price_per_dbu": "0.55", "savings_at_10": "150.0", "savings_at_30": "110.0",
        "savings_at_60": "80.0",
    },
    {
        "cluster_id": "c2", "cluster_name": "beta", "uptime_hrs": "50.0",
        "reclaim_hrs_at_30": "10.0", "total_dbus": "100", "avg_dbu_per_hr": "2.0",
        "price_per_dbu": "0.40", "savings_at_10": "20.0", "savings_at_30": "12.0",
        "savings_at_60": "5.0",
    },
]


def _collector_with_rows(cloud, rows=_CANNED_ROWS, ok=True):
    """Build a collector whose _run_sql returns canned rows and captures SQL."""
    c = SystemTablesCollector("DEFAULT", "wh123", cloud_provider=cloud)
    captured = {}

    def fake_run_sql(sql, label=""):
        captured["sql"] = sql
        captured["label"] = label
        return (rows if ok else None), ok

    c._run_sql = fake_run_sql  # type: ignore[assignment]
    return c, captured


def test_to_float_coerces_strings_and_none():
    assert SystemTablesCollector._to_float("12.5") == 12.5
    assert SystemTablesCollector._to_float(None) == 0.0
    assert SystemTablesCollector._to_float("not-a-number") == 0.0


def test_savings_aggregation_math():
    c, _ = _collector_with_rows("aws")
    result = c._collect_autoterm_savings()
    assert result["available"] is True
    assert result["cluster_count"] == 2
    assert result["window_days"] == 30
    assert result["savings_window_at_10"] == 170.0
    assert result["savings_window_at_30"] == 122.0
    assert result["savings_window_at_60"] == 85.0
    # annualized = 122 * 365 / 30
    assert result["annualized_at_30"] == 1484.33
    assert result["reclaim_hrs_at_30"] == 50.0
    # spend = 500*0.55 + 100*0.40
    assert result["total_allpurpose_spend_window"] == 315.0
    assert len(result["clusters"]) == 2


def test_cloud_filter_aws():
    c, captured = _collector_with_rows("aws")
    c._collect_autoterm_savings()
    assert "lp.cloud = 'AWS'" in captured["sql"]


def test_cloud_filter_azure_is_uppercase():
    c, captured = _collector_with_rows("azure")
    c._collect_autoterm_savings()
    assert "lp.cloud = 'AZURE'" in captured["sql"]


def test_cloud_filter_gcp():
    c, captured = _collector_with_rows("gcp")
    c._collect_autoterm_savings()
    assert "lp.cloud = 'GCP'" in captured["sql"]


def test_cloud_filter_omitted_when_unknown():
    c, captured = _collector_with_rows("unknown")
    c._collect_autoterm_savings()
    assert "lp.cloud" not in captured["sql"]


def test_savings_unavailable_when_query_fails():
    c, _ = _collector_with_rows("aws", ok=False)
    result = c._collect_autoterm_savings()
    assert result == {"available": False}


def _st_data(sav):
    return {"SystemTablesCollector": {"available": True, "autoterm_savings": sav}}


def test_headline_empty_without_data():
    assert _autoterm_headline({}) == ""
    assert _autoterm_headline(_st_data({"available": True, "annualized_at_30": 0})) == ""


def test_headline_formats_dollars():
    data = _st_data({
        "available": True, "annualized_at_30": 12000, "cluster_count": 3,
        "reclaim_hrs_at_30": 400, "window_days": 30,
    })
    headline = _autoterm_headline(data)
    assert "$12,000/yr" in headline
    assert "3 interactive cluster" in headline


def test_cost_011_zero_carries_headline():
    data = {
        "ComputeCollector": {
            "clusters": [{"cluster_source": "UI", "auto_termination_minutes": None}],
            "warehouses": [],
        },
        "SystemTablesCollector": {
            "available": True,
            "autoterm_savings": {
                "available": True, "annualized_at_30": 12000, "cluster_count": 1,
                "reclaim_hrs_at_30": 400, "window_days": 30,
            },
        },
    }
    score, note = _score_cost_011(data)
    assert score == 0
    assert "$12,000/yr" in note


def test_cost_011_full_has_no_headline():
    data = {
        "ComputeCollector": {
            "clusters": [{"cluster_source": "UI", "auto_termination_minutes": 30}],
            "warehouses": [],
        },
        "SystemTablesCollector": {
            "available": True,
            "autoterm_savings": {
                "available": True, "annualized_at_30": 12000, "cluster_count": 1,
                "reclaim_hrs_at_30": 400, "window_days": 30,
            },
        },
    }
    score, note = _score_cost_011(data)
    assert score == 2
    assert "reclaimable" not in note


def test_cost_021_carries_headline():
    data = {
        "SystemTablesCollector": {
            "available": True,
            "compute_history": {
                "available": True,
                "idle_clusters": [{"cluster_name": "x"}],
                "idle_hours_30d": 150,
            },
            "autoterm_savings": {
                "available": True, "annualized_at_30": 5000, "cluster_count": 2,
                "reclaim_hrs_at_30": 100, "window_days": 30,
            },
        },
    }
    score, note = _score_cost_021(data)
    assert score == 0
    assert "$5,000/yr" in note
