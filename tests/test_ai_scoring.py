"""Tests for the AICollector parsing and GenAI best-practice scoring."""

from wal_e.collectors.ai import (
    _has_strong_guardrails,
    _is_llm_entity,
    _looks_production,
    _scan_for_plaintext_secret,
)
from wal_e.framework.scoring import (
    _score_gov_006,
    _score_gov_016,
    _score_gov_018,
    _score_int_008,
    _score_rel_022,
    _score_sec_015,
    _score_sec_016,
)


# ---------------------------------------------------------------------------
# Collector helpers
# ---------------------------------------------------------------------------


def test_is_llm_entity_detects_external_and_fmapi():
    assert _is_llm_entity({"external_model": {"provider": "openai"}}, "my-ep", "")
    assert _is_llm_entity({}, "databricks-meta-llama-3", "")
    assert _is_llm_entity({}, "my-ep", "llm/v1/chat")
    assert not _is_llm_entity({}, "fraud-classifier", "")


def test_scan_for_plaintext_secret():
    safe = {"openai_config": {"openai_api_key": "{{secrets/scope/key}}"}}
    leaky = {"openai_config": {"openai_api_key_plaintext": "sk-realkey"}}
    empty_plaintext = {"openai_config": {"openai_api_key_plaintext": ""}}
    assert not _scan_for_plaintext_secret(safe)
    assert _scan_for_plaintext_secret(leaky)
    assert not _scan_for_plaintext_secret(empty_plaintext)


def test_has_strong_guardrails():
    assert _has_strong_guardrails({"input": {"pii": {"behavior": "BLOCK"}}})
    assert _has_strong_guardrails({"output": {"safety": True}})
    assert not _has_strong_guardrails({"input": {"pii": {"behavior": "NONE"}, "safety": False}})
    assert not _has_strong_guardrails({})


def test_looks_production():
    assert _looks_production("prod-rag-chatbot")
    assert not _looks_production("dev-prod-experiment")  # non-prod marker wins
    assert not _looks_production("rag-chatbot")


# ---------------------------------------------------------------------------
# Scoring: gov-016 (Models in Unity Catalog)
# ---------------------------------------------------------------------------


def test_gov_016_all_in_uc():
    data = {"AICollector": {"uc_model_count": 5, "ws_registry_model_count": 0}}
    score, _ = _score_gov_016(data)
    assert score == 2


def test_gov_016_partial_migration():
    data = {"AICollector": {"uc_model_count": 3, "ws_registry_model_count": 2}}
    score, _ = _score_gov_016(data)
    assert score == 1


def test_gov_016_legacy_only():
    data = {"AICollector": {"uc_model_count": 0, "ws_registry_model_count": 4}}
    score, _ = _score_gov_016(data)
    assert score == 0


def test_gov_016_no_models_unverifiable():
    data = {"AICollector": {"uc_model_count": 0, "ws_registry_model_count": 0}}
    score, notes = _score_gov_016(data)
    assert score == 1
    assert "no registered models" in notes.lower()


# ---------------------------------------------------------------------------
# Scoring: gov-018 (inference tables) and sec-015 (guardrails)
# ---------------------------------------------------------------------------


def test_gov_018_all_logged():
    data = {"AICollector": {"llm_endpoint_count": 2, "endpoints_with_inference_tables": 2}}
    assert _score_gov_018(data)[0] == 2


def test_gov_018_none_logged():
    data = {"AICollector": {"llm_endpoint_count": 2, "endpoints_with_inference_tables": 0}}
    assert _score_gov_018(data)[0] == 0


def test_gov_018_no_llm_is_na_full():
    data = {"AICollector": {"llm_endpoint_count": 0}}
    assert _score_gov_018(data)[0] == 2


def test_sec_015_partial_guardrails():
    data = {"AICollector": {"llm_endpoint_count": 3, "endpoints_with_guardrails": 1}}
    assert _score_sec_015(data)[0] == 1


# ---------------------------------------------------------------------------
# Scoring: sec-016 (plaintext credentials) — hard fail
# ---------------------------------------------------------------------------


def test_sec_016_plaintext_is_hard_fail():
    data = {"AICollector": {"external_model_endpoint_count": 2, "endpoints_with_plaintext_keys": 1}}
    score, notes = _score_sec_016(data)
    assert score == 0
    assert "secret scope" in notes.lower()


def test_sec_016_secrets_clean():
    data = {"AICollector": {"external_model_endpoint_count": 2, "endpoints_with_plaintext_keys": 0}}
    assert _score_sec_016(data)[0] == 2


# ---------------------------------------------------------------------------
# Scoring: rel-022 (provisioned throughput for prod LLM)
# ---------------------------------------------------------------------------


def test_rel_022_prod_with_provisioned_throughput():
    data = {"AICollector": {
        "llm_endpoint_count": 1, "prod_llm_endpoint_count": 1,
        "prod_llm_provisioned_throughput": 1, "prod_llm_scale_to_zero": 0,
    }}
    assert _score_rel_022(data)[0] == 2


def test_rel_022_prod_scale_to_zero_penalized():
    data = {"AICollector": {
        "llm_endpoint_count": 1, "prod_llm_endpoint_count": 1,
        "prod_llm_provisioned_throughput": 0, "prod_llm_scale_to_zero": 1,
    }}
    assert _score_rel_022(data)[0] == 0


def test_rel_022_no_llm_unverifiable():
    data = {"AICollector": {"llm_endpoint_count": 0}}
    score, notes = _score_rel_022(data)
    assert score == 1


# ---------------------------------------------------------------------------
# Scoring: gov-006 / int-008 upgrades read from AICollector
# ---------------------------------------------------------------------------


def test_gov_006_governed_in_uc():
    data = {"AICollector": {"uc_model_count": 4, "ws_registry_model_count": 0, "vs_endpoint_count": 1}}
    assert _score_gov_006(data)[0] == 2


def test_int_008_uc_models_full():
    data = {"AICollector": {"uc_model_count": 2, "endpoint_count": 3}}
    assert _score_int_008(data)[0] == 2


def test_int_008_endpoints_without_uc_models_partial():
    data = {"AICollector": {"uc_model_count": 0, "endpoint_count": 2}}
    assert _score_int_008(data)[0] == 1
