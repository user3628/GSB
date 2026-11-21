from __future__ import annotations

from copy import deepcopy

import pytest

from app.errors import BusinessError


def test_policy_digest_is_stable_and_version_is_monotonic(service, policy_v1):
    first = service.submit_policy(policy_v1)
    digest = first["digest"]
    assert len(digest) == 64

    with pytest.raises(BusinessError) as error:
        service.submit_policy(policy_v1)
    assert error.value.code == "E_STALE_POLICY"
    assert error.value.context["submitted_version"] == 1
    assert error.value.context["current_version"] == 1

    policies = service.list_policies()
    assert len(policies) == 1
    assert policies[0]["digest"] == digest


def test_invalid_policy_is_rejected_without_state_change(service, policy_v1):
    invalid = deepcopy(policy_v1)
    invalid["window_ms"] = 10
    with pytest.raises(BusinessError) as error:
        service.submit_policy(invalid)
    assert error.value.code == "E_POLICY_INVALID"
    assert error.value.context["field"] == "window_ms"
    assert service.list_policies() == []


def test_policy_digest_mismatch_is_rejected(service, policy_v1):
    policy = deepcopy(policy_v1)
    policy["digest"] = "0" * 64
    with pytest.raises(BusinessError) as error:
        service.submit_policy(policy)
    assert error.value.code == "E_POLICY_INVALID"
    assert error.value.context["field"] == "digest"
    assert service.list_policies() == []


def test_newer_policy_can_reuse_same_window_geometry(service, policy_v1, policy_v2):
    service.submit_policy(policy_v1)
    response = service.submit_policy(policy_v2)
    assert response["version"] == 2
    assert [item["version"] for item in service.list_policies()] == [2, 1]
