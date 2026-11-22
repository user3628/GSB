from __future__ import annotations

import copy
import threading

import pytest

from app.errors import AppError, ErrorCode
from app.policy import compute_checksum
from tests.conftest import T0, sample


def test_full_snapshot_replacement_is_monotonic_and_revisioned(service):
    first = service.load_snapshot(sample("policies_hierarchical_v2.json"))
    assert first["version"] == 2
    assert service.current_snapshot()["version"] == 2

    stale = sample("policies_hierarchical_v2.json")
    stale["version"] = 2
    stale["checksum"] = compute_checksum(stale)
    with pytest.raises(AppError) as exc:
        service.load_snapshot(stale)
    assert exc.value.code == ErrorCode.E_VERSION_CONFLICT
    assert service.current_snapshot()["version"] == 2


def test_incremental_patch_updates_only_affected_tenants(service):
    service.load_snapshot(sample("policies_hierarchical_v2.json"))
    patch = {
        "base_version": 2,
        "generated_at_ms": T0 + 5000,
        "upsert": [
            {
                "tenant_id": "alice",
                "quota": {"cpu_millicores": 5000},
                "rate_limit": {"capacity": 50},
            }
        ],
        "remove": [],
    }
    result = service.submit_incremental(patch)
    assert result["version"] == 3
    assert result["base_version"] == 2
    current = service.current_snapshot()["snapshot"]
    alice = next(t for t in current["tenants"] if t["tenant_id"] == "alice")
    team = next(t for t in current["tenants"] if t["tenant_id"] == "team-alpha")
    assert alice["quota"]["cpu_millicores"] == 5000
    assert alice["quota"]["burst"]["cpu_millicores"] == 500
    assert alice["rate_limit"]["capacity"] == 50
    assert team["quota"]["cpu_millicores"] == 6000


def test_stale_incremental_does_not_change_revision_or_snapshot(service):
    service.load_snapshot(sample("policies_hierarchical_v2.json"))
    before = service.meta()
    with pytest.raises(AppError) as exc:
        service.submit_incremental(
            {
                "base_version": 1,
                "generated_at_ms": T0 + 5000,
                "upsert": [{"tenant_id": "alice", "quota": {"cpu_millicores": 5000}}],
            }
        )
    assert exc.value.code == ErrorCode.E_STALE_BASE
    assert service.meta() == before


def test_two_concurrent_increments_same_base_only_one_wins(service):
    service.load_snapshot(sample("policies_hierarchical_v2.json"))
    barrier = threading.Barrier(2)
    result: dict[str, object] = {}

    def worker(name: str, cpu: int):
        barrier.wait()
        try:
            out = service.submit_incremental(
                {
                    "base_version": 2,
                    "generated_at_ms": T0 + 5000,
                    "upsert": [{"tenant_id": "alice", "quota": {"cpu_millicores": cpu}}],
                }
            )
            result[name] = ("OK", out["version"])
        except AppError as exc:
            result[name] = ("ERR", exc.code)

    threads = [
        threading.Thread(target=worker, args=("a", 5100)),
        threading.Thread(target=worker, args=("b", 5200)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    outcomes = sorted(result.values(), key=str)
    assert sum(1 for status, _ in outcomes if status == "OK") == 1
    assert sum(
        1 for status, value in outcomes if status == "ERR" and value == ErrorCode.E_STALE_BASE
    ) == 1
    assert service.current_snapshot()["version"] == 3


def test_invalid_full_snapshot_does_not_persist(service):
    service.load_snapshot(sample("policies_hierarchical_v2.json"))
    before = service.meta()
    broken = copy.deepcopy(sample("policies_hierarchical_v2.json"))
    broken["version"] = 3
    broken["tenants"][0]["parent_id"] = "missing-parent"
    broken["checksum"] = compute_checksum(broken)
    with pytest.raises(AppError) as exc:
        service.load_snapshot(broken)
    assert exc.value.code == ErrorCode.E_SPEC_INVALID
    assert service.meta() == before


def test_revision_pinned_reads_do_not_mix_snapshots(service):
    service.load_snapshot(sample("policies_hierarchical_v2.json"))
    revision_v2 = service.meta()["revision"]
    service.submit_incremental(
        {
            "base_version": 2,
            "generated_at_ms": T0 + 5000,
            "upsert": [{"tenant_id": "alice", "quota": {"cpu_millicores": 7000}}],
        }
    )
    old = service.list_tenants(revision=revision_v2)
    new = service.list_tenants()
    old_alice = next(t for t in old["tenants"] if t["tenant_id"] == "alice")
    new_alice = next(t for t in new["tenants"] if t["tenant_id"] == "alice")
    assert old["snapshot_version"] == 2
    assert old_alice["quota"]["cpu_millicores"] == 4000
    assert new["snapshot_version"] == 3
    assert new_alice["quota"]["cpu_millicores"] == 7000
