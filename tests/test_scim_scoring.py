"""Tests for SCIM / SSO / group-management scoring.

Regression coverage for the false-negative bug where WAL-E reported SCIM as
"not implemented" (a confident score of 0) whenever workspace-level SCIM groups
lacked an ``externalId`` — which routinely happens in account-level /
identity-federated deployments even when SCIM is fully configured.
"""

from wal_e.framework.scoring import (
    _is_verified,
    _score_gov_013,
    _score_sec_008,
    _score_sec_009,
)


def _data(security: dict, cloud: str = "aws") -> dict:
    return {"SecurityCollector": security, "_cloud_provider": cloud}


def test_scim_federated_no_external_id_is_not_a_confident_zero():
    """Groups exist but carry no externalId (account-level SCIM) -> unverifiable, not 0."""
    data = _data({
        "scim_groups": [{"displayName": "engineers", "externalId": None, "id": "1"}],
        "scim_group_count": 12,
        "scim_groups_with_external_id": 0,
        "scim_user_count": 200,
        "scim_users_with_external_id": 0,
    })
    score, notes = _score_sec_009(data)
    assert score != 0, "must not assert a confident 'not implemented'"
    assert score == 1
    assert "not verifiable" in notes.lower()
    # And it must be flagged as unverified rather than a real gap.
    assert _is_verified(score, notes) is False


def test_scim_user_external_id_corroborates_provisioning():
    """No group externalId, but users are IdP-synced -> at least partial credit."""
    data = _data({
        "scim_groups": [{"displayName": "engineers", "externalId": None, "id": "1"}],
        "scim_group_count": 5,
        "scim_groups_with_external_id": 0,
        "scim_user_count": 300,
        "scim_users_with_external_id": 300,
    })
    score, notes = _score_sec_009(data)
    assert score == 2, "many IdP-synced users is strong evidence of SCIM"
    assert _is_verified(score, notes) is True


def test_scim_fully_synced_groups_scores_two():
    data = _data({
        "scim_groups": [
            {"displayName": "a", "externalId": "x1", "id": "1"},
            {"displayName": "b", "externalId": "x2", "id": "2"},
            {"displayName": "c", "externalId": "x3", "id": "3"},
        ],
        "scim_group_count": 3,
        "scim_groups_with_external_id": 3,
        "scim_user_count": 50,
        "scim_users_with_external_id": 50,
    })
    score, notes = _score_sec_009(data)
    assert score == 2
    assert _is_verified(score, notes) is True


def test_scim_partial_single_group_synced():
    data = _data({
        "scim_groups": [{"displayName": "a", "externalId": "x1", "id": "1"}],
        "scim_group_count": 4,
        "scim_groups_with_external_id": 1,
        "scim_user_count": 2,
        "scim_users_with_external_id": 0,
    })
    score, _ = _score_sec_009(data)
    assert score == 1


def test_scim_truly_empty_is_unverifiable_not_zero():
    """No identity data at all -> still unverifiable (never a confident 0)."""
    data = _data({
        "scim_groups": [],
        "scim_group_count": 0,
        "scim_groups_with_external_id": 0,
        "scim_user_count": 0,
        "scim_users_with_external_id": 0,
    })
    score, notes = _score_sec_009(data)
    assert score == 1
    assert _is_verified(score, notes) is False


def test_sso_detected_from_user_external_id():
    data = _data({
        "scim_group_count": 5,
        "scim_groups_with_external_id": 0,
        "scim_user_count": 100,
        "scim_users_with_external_id": 100,
    })
    score, notes = _score_sec_008(data)
    assert score == 2
    assert "sso" in notes.lower()


def test_sso_present_but_unverifiable_is_flagged():
    data = _data({
        "scim_group_count": 5,
        "scim_groups_with_external_id": 0,
        "scim_user_count": 100,
        "scim_users_with_external_id": 0,
    })
    score, notes = _score_sec_008(data)
    assert score == 1
    assert _is_verified(score, notes) is False


def test_group_management_no_longer_returns_zero_when_groups_present():
    data = _data({
        "scim_group_count": 20,
        "scim_groups_with_external_id": 0,
        "scim_user_count": 0,
        "scim_users_with_external_id": 0,
    })
    score, notes = _score_gov_013(data)
    assert score == 1
    assert _is_verified(score, notes) is False


def test_group_management_synced_groups_scores_two():
    data = _data({
        "scim_groups": [{"displayName": "a", "externalId": "x1", "id": "1"}],
        "scim_group_count": 8,
        "scim_groups_with_external_id": 1,
        "scim_user_count": 10,
        "scim_users_with_external_id": 10,
    })
    score, _ = _score_gov_013(data)
    assert score == 2


def test_signals_fall_back_to_counting_when_summary_field_absent():
    """Older cached data without the *_with_external_id summary still works."""
    data = _data({
        "scim_groups": [
            {"displayName": "a", "externalId": "x1", "id": "1"},
            {"displayName": "b", "externalId": None, "id": "2"},
        ],
        "scim_group_count": 2,
        # no scim_groups_with_external_id key on purpose
    })
    score, _ = _score_sec_009(data)
    assert score == 1  # exactly one synced group -> partial
