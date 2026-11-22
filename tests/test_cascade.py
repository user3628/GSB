from __future__ import annotations

from app.errors import ErrorCode
from app.policy import SCHEMA_V2, with_checksum
from app.service import GovernanceService
from tests.conftest import GI, T0, T1


def app(request_id: str, tenant_id: str, arrival: int, cpu: int, memory: int = 128 * 1024 * 1024) -> dict:
    return {
        "request_id": request_id,
        "tenant_id": tenant_id,
        "namespace": "shared-prod",
        "arrival_ms": arrival,
        "cpu_millicores": cpu,
        "memory_bytes": memory,
    }


def _tenant(tenant_id, parent_id, cpu, mem, rate, capacity, initial):
    return {
        "tenant_id": tenant_id,
        "parent_id": parent_id,
        "quota": {
            "cpu_millicores": cpu,
            "memory_bytes": mem,
            "burst": {"cpu_millicores": 0, "memory_bytes": 0},
        },
        "rate_limit": {
            "refill_per_second": rate,
            "capacity": capacity,
            "initial_tokens": initial,
        },
        "lifecycle": {"start_ms": T0, "end_ms": T1},
    }


def test_child_usage_and_tokens_cascade_to_all_ancestors(loaded_service):
    svc = loaded_service
    result = svc.submit_decision(app("bob-cascade", "bob", T0 + 1300, 1000))
    assert result.decision["decision"] == "ACCEPTED"
    tenants = {t["tenant_id"]: t for t in svc.list_tenants()["tenants"]}
    assert tenants["bob"]["usage"]["cpu_millicores"] == 1000
    assert tenants["bob"]["usage"]["direct_cpu_millicores"] == 1000
    assert tenants["team-beta"]["usage"]["cpu_millicores"] == 1000
    assert tenants["team-beta"]["usage"]["direct_cpu_millicores"] == 0
    assert tenants["platform"]["usage"]["cpu_millicores"] == 1000
    assert tenants["bob"]["token_bucket"]["tokens_scaled"] == 19000
    assert tenants["team-beta"]["token_bucket"]["tokens_scaled"] == 39000
    assert tenants["platform"]["token_bucket"]["tokens_scaled"] == 199000


def test_first_insufficient_ancestor_blocks_whole_request(loaded_service):
    svc = loaded_service
    svc.submit_decision(app("bob-1", "bob", T0 + 1000, 1000))
    result = svc.submit_decision(app("dave-1", "dave", T0 + 1100, 1000)).decision
    assert result["decision"] == "REJECTED"
    assert result["code"] == ErrorCode.E_PARENT_QUOTA_EXCEEDED
    assert result["details"]["blocking_tenant_id"] == "team-beta"
    assert result["details"]["resources"]["cpu_millicores"]["used"] == 1000
    assert result["details"]["resources"]["cpu_millicores"]["remaining"] == 800
    assert result["details"]["resources"]["cpu_millicores"]["shortfall"] == 200
    tenants = {t["tenant_id"]: t for t in svc.list_tenants()["tenants"]}
    assert tenants["dave"]["usage"]["cpu_millicores"] == 0
    assert tenants["team-beta"]["usage"]["cpu_millicores"] == 1000
    assert tenants["platform"]["usage"]["cpu_millicores"] == 1000
    assert tenants["dave"]["status"] == "QUOTA_BLOCKED"


def test_ancestor_token_bucket_is_enforced(tmp_path):
    tenants = [
        _tenant("root", None, 10_000, 10 * GI, 1, 1, 1),
        _tenant("child", "root", 10_000, 10 * GI, 100, 100, 100),
    ]
    document = with_checksum(
        {
            "schema_version": SCHEMA_V2,
            "version": 1,
            "generated_at_ms": T0,
            "tenants": tenants,
        }
    )
    svc = GovernanceService(str(tmp_path / "ancestor-token.sqlite"))
    svc.startup()
    svc.load_snapshot(document)
    first = svc.submit_decision(app("c1", "child", T0 + 100, 10)).decision
    assert first["decision"] == "ACCEPTED"
    second = svc.submit_decision(app("c2", "child", T0 + 100, 10)).decision
    assert second["decision"] == "REJECTED"
    assert second["code"] == ErrorCode.E_RATE_LIMITED
    assert second["details"]["scope"] == "ancestor"
    assert second["details"]["blocking_tenant_id"] == "root"
    state = {t["tenant_id"]: t for t in svc.list_tenants()["tenants"]}
    assert state["child"]["usage"]["cpu_millicores"] == 10
    assert state["root"]["usage"]["cpu_millicores"] == 10


def test_sibling_usage_aggregates_at_parent(loaded_service):
    svc = loaded_service
    assert svc.submit_decision(app("bob-a", "bob", T0 + 1000, 900)).decision["decision"] == "ACCEPTED"
    assert svc.submit_decision(app("dave-a", "dave", T0 + 1000, 900)).decision["decision"] == "ACCEPTED"
    third = svc.submit_decision(app("bob-b", "bob", T0 + 1100, 900)).decision
    assert third["code"] == ErrorCode.E_PARENT_QUOTA_EXCEEDED
    assert third["details"]["blocking_tenant_id"] == "team-beta"
    state = {t["tenant_id"]: t for t in svc.list_tenants()["tenants"]}
    assert state["team-beta"]["usage"]["cpu_millicores"] == 1800
    assert state["bob"]["usage"]["cpu_millicores"] == 900
    assert state["dave"]["usage"]["cpu_millicores"] == 900
