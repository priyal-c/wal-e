"""Tests for false-negative fixes and the CSV 'why unverifiable' explainer.

Confirms that controls which are not observable from the workspace REST API
(account-level config, deep-scan-only signals, unreliable heuristics) are
scored as unverifiable partials rather than confident zeros.
"""

from wal_e.framework.scoring import (
    _is_verified,
    _score_gov_006,
    _score_gov_009,
    _score_gov_014,
    _score_sec_011,
)
from wal_e.reporters.csv_report import CSVReporter


def test_audit_events_unverifiable_without_deep_scan():
    data = {"SecurityCollector": {"security_settings": {}}}
    score, notes = _score_gov_009(data)
    assert score == 1
    assert not _is_verified(score, notes)
    assert "--deep" in notes


def test_managed_tables_not_zero_from_external_location_ratio():
    data = {"GovernanceCollector": {"external_location_count": 62, "catalog_count": 44}}
    score, notes = _score_gov_014(data)
    assert score == 1
    assert not _is_verified(score, notes)
    assert "not a reliable proxy" in notes


def test_managed_tables_full_when_no_external_locations():
    data = {"GovernanceCollector": {"external_location_count": 0, "catalog_count": 10}}
    score, _ = _score_gov_014(data)
    assert score == 2


def test_vpc_unverifiable_not_zero():
    data = {"_cloud_provider": "aws", "SecurityCollector": {"security_settings": {}}}
    score, notes = _score_sec_011(data)
    assert score == 1
    assert not _is_verified(score, notes)
    assert "account" in notes.lower()


def test_vpc_unverifiable_with_ip_access_lists():
    data = {
        "_cloud_provider": "aws",
        "SecurityCollector": {"security_settings": {"enableIpAccessLists": "true"}},
    }
    score, notes = _score_sec_011(data)
    assert score == 1
    assert "account" in notes.lower()


def test_govern_ai_uc_models_full():
    data = {"AICollector": {"uc_model_count": 5, "ws_registry_model_count": 0, "vs_index_count": 2}}
    score, _ = _score_gov_006(data)
    assert score == 2


def test_govern_ai_no_collector_signal_is_unverifiable():
    # Empty AICollector (e.g. older cached data) with endpoints only via fallback.
    data = {"AICollector": {}, "OperationsCollector": {"endpoint_count": 42}}
    score, notes = _score_gov_006(data)
    assert score == 1
    assert not _is_verified(score, notes)
    assert "not verifiable" in notes.lower()


def test_govern_ai_all_external_endpoints_not_a_gap():
    data = {"AICollector": {"endpoint_count": 3, "external_model_endpoint_count": 3,
                            "uc_model_count": 0, "ws_registry_model_count": 0}}
    score, notes = _score_gov_006(data)
    assert score == 1
    assert "external" in notes.lower()


def test_govern_ai_custom_endpoints_no_uc_is_flagged_gap():
    # Custom (non-external) models served, none in UC = real gap, but verified
    # (not hidden as unverifiable) and never a confident 0.
    data = {"AICollector": {"endpoint_count": 4, "external_model_endpoint_count": 1,
                            "uc_model_count": 0, "ws_registry_model_count": 0}}
    score, notes = _score_gov_006(data)
    assert score == 1
    assert _is_verified(score, notes)
    assert "no models registered in unity catalog" in notes.lower()


def test_govern_ai_workspace_registry_only_is_gap():
    data = {"AICollector": {"endpoint_count": 0, "uc_model_count": 0, "ws_registry_model_count": 3}}
    score, notes = _score_gov_006(data)
    assert score == 1
    assert "workspace registry" in notes.lower()


def test_csv_reason_deep_scan():
    reason = CSVReporter._unverifiable_reason("Requires --deep scan with system tables.")
    assert "--deep" in reason


def test_csv_reason_account_level():
    reason = CSVReporter._unverifiable_reason(
        "SCIM is configured at the account level; verify in the account console."
    )
    assert "account console" in reason


def test_csv_reason_generic_api():
    reason = CSVReporter._unverifiable_reason("Caching not verifiable from API.")
    assert "manual review" in reason


def test_notes_with_reason_appends_only_when_unverified():
    r = CSVReporter()
    verified_note = r._notes_with_reason("All good.", verified=True)
    assert verified_note == "All good."
    unverified_note = r._notes_with_reason("Caching not verifiable from API.", verified=False)
    assert "Why unverifiable" in unverified_note
    assert unverified_note.startswith("Caching not verifiable from API.")
