from __future__ import annotations

import pytest

from conftest import load_sample
from graph_bench.errors import (
    ConflictingRequest,
    NoSnapshot,
    SpecInvalid,
    StaleBase,
)
from graph_bench.service import BenchService
from graph_bench.storage import Storage


def test_snapshot_submit_replaces_state_without_mixing(service: BenchService) -> None:
    result = service.submit_snapshot(load_sample("snapshot_v1.json"), "s1")
    assert result["snapshot_version"] == 1
    snapshot = service.get_snapshot()["snapshot"]
    assert {item["id"] for item in snapshot["resources"]} == {
        "gateway", "auth", "billing", "catalog", "order", "inventory", "search", "shipment",
    }

    small = load_sample("snapshot_bad_checksum_v2.json")
    small["checksum"] = None
    from graph_bench.models import Snapshot
    parsed = Snapshot.from_dict(small)
    small_payload = {
        "version": 2,
        "generated_at": parsed.generated_at,
        "resources": [item.to_dict() for item in parsed.resources],
    }
    from graph_bench.canonical import snapshot_digest
    small_payload["checksum"] = snapshot_digest(2, small_payload["generated_at"], small_payload["resources"])
    service.submit_snapshot(small_payload, "s2")
    current = service.get_snapshot()["snapshot"]
    assert [item["id"] for item in current["resources"]] == ["gateway"]
    assert service.get_snapshot(1)["snapshot"]["version"] == 1


def test_invalid_snapshot_keeps_old_state_usable(service: BenchService) -> None:
    service.submit_snapshot(load_sample("snapshot_v1.json"), "s1")
    with pytest.raises(Exception) as info:
        service.submit_snapshot(load_sample("snapshot_bad_checksum_v2.json"), "s-bad")
    assert info.value.code == "E_SNAPSHOT_INVALID"
    assert service.get_snapshot()["snapshot"]["version"] == 1


def test_increment_happy_path_and_cascade_invalidation(seeded_service: BenchService) -> None:
    service = seeded_service
    result = service.submit_increment(load_sample("increment_weight_auth.json"), "i1")
    assert result["snapshot_version"] == 2
    assert result["increment"]["cascade_invalidated"] == ["auth", "order", "shipment"]
    auth = service.get_resource("auth")
    assert auth["spec"]["weight_milli"] == 900
    assert auth["snapshot_version"] == 2


def test_stale_base_and_concurrent_same_base(seeded_service: BenchService) -> None:
    service = seeded_service
    service.submit_increment(load_sample("increment_weight_auth.json"), "i1")
    with pytest.raises(StaleBase):
        service.submit_increment(load_sample("increment_stale_base.json"), "i2")

    # 两个基于同一版本的并发增量，只允许一个成功（使用全新的 v1 服务）。
    import threading

    fresh = BenchService(Storage(service.storage.db_path.parent / "concurrent.db"))
    fresh.submit_snapshot(load_sample("snapshot_v1.json"), "fresh-v1")
    payload_a = load_sample("increment_weight_auth.json")
    payload_b = {
        "base_version": 1,
        "upserts": [
            {"id": "billing", "replicas": 2, "parents": ["gateway"], "weight_milli": 700, "desired_version": "2.0.1"}
        ],
        "removals": [],
    }
    outcomes: list[str] = []
    barrier = threading.Barrier(2)

    def worker(target: BenchService, payload: dict, request_id: str) -> None:
        barrier.wait()
        try:
            target.submit_increment(payload, request_id)
            outcomes.append("ok")
        except StaleBase:
            outcomes.append("stale")

    t1 = threading.Thread(target=worker, args=(fresh, payload_a, "c1"))
    t2 = threading.Thread(target=worker, args=(fresh, payload_b, "c2"))
    t1.start(); t2.start(); t1.join(); t2.join()
    assert sorted(outcomes) == ["ok", "stale"]


def test_spec_invalid_does_not_mutate_state(seeded_service: BenchService) -> None:
    service = seeded_service
    before = service.get_snapshot()
    with pytest.raises(SpecInvalid):
        service.submit_increment(load_sample("increment_bad_weight.json"), "bad")
    after = service.get_snapshot()
    assert before["snapshot"] == after["snapshot"]


def test_idempotent_replay_and_conflicting_request(seeded_service: BenchService) -> None:
    service = seeded_service
    payload = load_sample("increment_weight_auth.json")
    first = service.submit_increment(payload, "idem-1")
    second = service.submit_increment(payload, "idem-1")
    assert second["idempotent_replay"] is True
    assert second["snapshot_version"] == first["snapshot_version"]

    conflicting = dict(payload)
    conflicting["upserts"] = [
        {"id": "auth", "replicas": 2, "parents": ["gateway"], "weight_milli": 100, "desired_version": "0.0.1"}
    ]
    with pytest.raises(ConflictingRequest):
        service.submit_increment(conflicting, "idem-1")


def test_cycle_snapshot_returns_partial_view_and_keeps_old_version(seeded_service: BenchService) -> None:
    service = seeded_service
    result = service.submit_snapshot(load_sample("snapshot_cycle_v9.json"), "cycle")
    assert result["is_acyclic"] is False
    assert result["cycles"][0][0] == result["cycles"][0][-1]
    layers = service.get_layers()
    assert layers["layers"] == [{"layer": 0, "resources": ["standalone-report"]}]
    assert "gateway" in layers["blocked_resources"]
    # 旧版本仍可按版本号读取。
    assert service.get_snapshot(1)["snapshot"]["version"] == 1


def test_repeated_queries_are_bit_identical(seeded_service: BenchService) -> None:
    import json

    service = seeded_service
    service.submit_increment(load_sample("increment_weight_auth.json"), "i1")
    service.submit_increment(load_sample("increment_add_risk.json"), "i2")
    first = json.dumps(service.get_layers(2), sort_keys=True, ensure_ascii=False)
    second = json.dumps(service.get_layers(2), sort_keys=True, ensure_ascii=False)
    assert first == second
    impact_a = json.dumps(service.get_impact("gateway", version=2), sort_keys=True)
    impact_b = json.dumps(service.get_impact("gateway", version=2), sort_keys=True)
    assert impact_a == impact_b


def test_budget_returns_partial_results_from_single_version(seeded_service: BenchService) -> None:
    service = seeded_service
    partial = service.get_impact("gateway", budget_ms=1, delay_per_item_ms=5)
    assert partial["snapshot_version"] == service.storage.get_current_version()
    if not partial["completed"]:
        assert partial["remaining"]
        assert set(partial["downstream"]).isdisjoint(set(partial["remaining"]))
    full = service.get_impact("gateway")
    assert full["completed"] is True


def test_persistence_survives_reopen(tmp_path) -> None:
    db = tmp_path / "persist.db"
    service = BenchService(Storage(db))
    service.submit_snapshot(load_sample("snapshot_v1.json"), "s1")
    reopened = BenchService(Storage(db))
    report = reopened.recover()
    assert report["pending_intent_count"] == 0
    assert reopened.get_snapshot()["snapshot"]["version"] == 1
    with pytest.raises(NoSnapshot):
        BenchService(Storage(tmp_path / "empty.db")).get_snapshot()
