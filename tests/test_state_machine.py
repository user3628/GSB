from __future__ import annotations

import pytest

from app.errors import AppError, ErrorCode
from tests.conftest import GI, T0


def request(request_id: str, cpu: int, memory: int = GI) -> dict:
    return {
        "request_id": request_id,
        "tenant_id": "alice",
        "namespace": "shared-prod",
        "arrival_ms": T0 + 1000,
        "cpu_millicores": cpu,
        "memory_bytes": memory,
    }


def test_illegal_transition_is_atomic(loaded_service):
    svc = loaded_service
    before = svc.meta()
    with pytest.raises(AppError) as exc:
        svc.transition_state("alice", "ACTIVE")
    assert exc.value.code == ErrorCode.E_INVALID_TRANSITION
    assert svc.meta() == before
    alice = next(t for t in svc.list_tenants()["tenants"] if t["tenant_id"] == "alice")
    assert alice["status"] == "ACTIVE"


def test_legal_transition_and_quota_release_share_one_revision(loaded_service):
    svc = loaded_service
    accepted = svc.submit_decision(request("state-1", 1200, GI))
    assert accepted.decision["decision"] == "ACCEPTED"
    revision_before = svc.meta()["revision"]

    out = svc.transition_state(
        "alice",
        "MIGRATING",
        release={"cpu_millicores": 500, "memory_bytes": GI // 2},
        reason="maintenance",
    )
    assert out["from_state"] == "ACTIVE"
    assert out["to_state"] == "MIGRATING"
    assert out["revision"] == revision_before + 1
    assert out["released"] == {"cpu_millicores": 500, "memory_bytes": GI // 2}

    tenants = {t["tenant_id"]: t for t in svc.list_tenants()["tenants"]}
    assert tenants["alice"]["usage"]["cpu_millicores"] == 700
    assert tenants["alice"]["usage"]["memory_bytes"] == GI // 2
    assert tenants["team-alpha"]["usage"]["cpu_millicores"] == 700
    assert tenants["platform"]["usage"]["cpu_millicores"] == 700
    assert tenants["alice"]["status"] == "MIGRATING"

    blocked = svc.submit_decision(request("state-2", 10, 1024)).decision
    assert blocked["code"] == ErrorCode.E_TENANT_MIGRATING
    tenants = {t["tenant_id"]: t for t in svc.list_tenants()["tenants"]}
    assert tenants["alice"]["usage"]["cpu_millicores"] == 700


def test_suspended_to_throttled_is_invalid_and_keeps_state(loaded_service):
    svc = loaded_service
    svc.transition_state("alice", "SUSPENDED")
    revision = svc.meta()["revision"]
    with pytest.raises(AppError) as exc:
        svc.transition_state("alice", "THROTTLED")
    assert exc.value.code == ErrorCode.E_INVALID_TRANSITION
    assert svc.meta()["revision"] == revision
    alice = next(t for t in svc.list_tenants()["tenants"] if t["tenant_id"] == "alice")
    assert alice["status"] == "SUSPENDED"
