from __future__ import annotations

import copy

from app.errors import AppError, ErrorCode
from app.policy import SCHEMA_V2, with_checksum
from app.service import GovernanceService
from tests.conftest import GI, T0, T1, sample


def app(request_id: str, tenant_id: str, arrival: int, cpu: int, memory: int = GI) -> dict:
    return {
        "request_id": request_id,
        "tenant_id": tenant_id,
        "namespace": "shared-prod",
        "arrival_ms": arrival,
        "cpu_millicores": cpu,
        "memory_bytes": memory,
    }


def test_token_bucket_integer_ms_refill_and_rejection_without_usage(loaded_service):
    svc = loaded_service
    first = svc.submit_decision(app("tok-1", "starter", T0 + 500, 100, 2 * 1024 * 1024))
    assert first.decision["decision"] == "ACCEPTED"
    starter = next(t for t in svc.list_tenants()["tenants"] if t["tenant_id"] == "starter")
    assert starter["token_bucket"]["tokens_scaled"] == 0

    second = svc.submit_decision(app("tok-2", "starter", T0 + 1000, 100, 2 * 1024 * 1024))
    assert second.decision["decision"] == "ACCEPTED"

    third = svc.submit_decision(app("tok-3", "starter", T0 + 1200, 100, 2 * 1024 * 1024))
    d = third.decision
    assert d["decision"] == "REJECTED"
    assert d["code"] == ErrorCode.E_RATE_LIMITED
    assert d["details"]["tokens"]["shortfall_units"] == 600
    assert d["details"]["tokens"]["available_units"] == 400
    assert d["token_advances"][0]["refill_units"] == 400
    assert d["state_after"] == "THROTTLED"
    assert d["usage_changes"] == []

    starter = next(t for t in svc.list_tenants()["tenants"] if t["tenant_id"] == "starter")
    assert starter["usage"]["cpu_millicores"] == 200
    assert starter["status"] == "THROTTLED"


def test_direct_cpu_quota_is_atomic_and_reports_shortfall(loaded_service):
    svc = loaded_service
    assert svc.submit_decision(app("alice-1", "alice", T0 + 1000, 1200)).decision["decision"] == "ACCEPTED"
    assert svc.submit_decision(app("alice-2", "alice", T0 + 1100, 1800)).decision["decision"] == "ACCEPTED"

    before = next(t for t in svc.list_tenants()["tenants"] if t["tenant_id"] == "alice")
    result = svc.submit_decision(app("alice-3", "alice", T0 + 1200, 1600))
    d = result.decision
    assert d["decision"] == "REJECTED"
    assert d["code"] == ErrorCode.E_QUOTA_EXCEEDED
    assert d["details"]["resources"]["cpu_millicores"]["remaining"] == 1500
    assert d["details"]["resources"]["cpu_millicores"]["shortfall"] == 100
    assert d["details"]["resources"]["cpu_millicores"]["effective_limit"] == 4500
    assert d["state_after"] == "QUOTA_BLOCKED"
    after = next(t for t in svc.list_tenants()["tenants"] if t["tenant_id"] == "alice")
    assert after["usage"] == before["usage"]
    assert after["usage"]["cpu_millicores"] == 3000


def test_memory_quota_and_burst_are_both_enforced(tmp_path):
    tenant = {
        "tenant_id": "mem",
        "parent_id": None,
        "quota": {
            "cpu_millicores": 1000,
            "memory_bytes": 100,
            "burst": {"cpu_millicores": 100, "memory_bytes": 50},
        },
        "rate_limit": {"refill_per_second": 10, "capacity": 10, "initial_tokens": 10},
        "lifecycle": {"start_ms": T0, "end_ms": T1},
    }
    document = with_checksum(
        {
            "schema_version": SCHEMA_V2,
            "version": 1,
            "generated_at_ms": T0,
            "tenants": [tenant],
        }
    )
    svc = GovernanceService(str(tmp_path / "mem.sqlite"))
    svc.startup()
    svc.load_snapshot(document)

    accepted = svc.submit_decision(
        {
            "request_id": "mem-1",
            "tenant_id": "mem",
            "namespace": "shared",
            "arrival_ms": T0 + 100,
            "cpu_millicores": 50,
            "memory_bytes": 140,
        }
    )
    assert accepted.decision["decision"] == "ACCEPTED"

    rejected = svc.submit_decision(
        {
            "request_id": "mem-2",
            "tenant_id": "mem",
            "namespace": "shared",
            "arrival_ms": T0 + 200,
            "cpu_millicores": 1,
            "memory_bytes": 20,
        }
    ).decision
    assert rejected["code"] == ErrorCode.E_QUOTA_EXCEEDED
    assert rejected["details"]["resources"]["memory_bytes"]["effective_limit"] == 150
    assert rejected["details"]["resources"]["memory_bytes"]["shortfall"] == 10


def test_out_of_order_single_request_is_rejected(loaded_service):
    svc = loaded_service
    svc.submit_decision(app("clock-1", "starter", T0 + 1000, 10, 1024))
    result = svc.submit_decision(app("clock-2", "starter", T0 + 999, 10, 1024)).decision
    assert result["code"] == ErrorCode.E_TIMESTAMP_REGRESSION
    assert result["details"]["last_refill_ms"] == T0 + 1000
