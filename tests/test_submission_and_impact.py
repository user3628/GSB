from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from config_coordinator import Coordinator, DomainError, IncrementRequest, Snapshot
from config_coordinator.codec import compute_snapshot_checksum
from config_coordinator.errors import E_DEPENDENCY_CYCLE, E_SNAPSHOT_INVALID, E_STALE_BASE, E_STALE_SNAPSHOT

from conftest import ROOT, component, sample_components, snapshot


def test_snapshot_integrity_and_stale_failures_do_not_publish(coordinator: Coordinator) -> None:
    configs = {"a": 1, "b": 2}
    components = [component("svc", ["a"], logical_component="svc")]
    report = coordinator.submit_snapshot(snapshot(1, configs), components)
    assert report["version"] == 1
    before = coordinator.get_state()

    invalid = Snapshot(
        version=2,
        generated_at="2026-01-02T00:00:00Z",
        configs={"a": 9},
        checksum="sha256:not-the-real-digest",
    )
    with pytest.raises(DomainError) as invalid_error:
        coordinator.submit_snapshot(invalid, components)
    assert invalid_error.value.code == E_SNAPSHOT_INVALID

    stale = snapshot(1, {"a": 7})
    with pytest.raises(DomainError) as stale_error:
        coordinator.submit_snapshot(stale, components)
    assert stale_error.value.code == E_STALE_SNAPSHOT

    after = coordinator.get_state()
    assert after["current_version"] == 1
    assert after["snapshot"]["configs"] == before["snapshot"]["configs"]
    assert after["derived_artifacts"] == before["derived_artifacts"]
    assert after["component_states"] == before["component_states"]


def test_concurrent_increments_on_same_base_have_one_winner(tmp_path: Path) -> None:
    db = str(tmp_path / "race.db")
    first = Coordinator(db)
    first.submit_snapshot(
        snapshot(1, {"a": 1, "b": 2}),
        [component("svc", ["a", "b"], logical_component="svc")],
    )
    barrier = threading.Barrier(3)

    def submit(configs: dict) -> tuple[str, object]:
        coordinator = Coordinator(db)
        request = IncrementRequest(base_version=1, config_set=configs)
        barrier.wait()
        try:
            return "ok", coordinator.submit_increment(request)
        except DomainError as exc:
            return "error", exc.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(submit, {"a": 10}), pool.submit(submit, {"b": 20})]
        barrier.wait()
        results = [future.result(timeout=20) for future in futures]

    assert sorted(result[0] for result in results) == ["error", "ok"]
    assert [result[1] for result in results if result[0] == "error"] == [E_STALE_BASE]
    state = first.get_state()
    assert state["current_version"] == 2
    assert state["snapshot"]["version"] == 2
    assert (state["snapshot"]["configs"]["a"], state["snapshot"]["configs"]["b"]) in {
        (10, 2),
        (1, 20),
    }


def test_single_key_increment_cascades_to_direct_and_downstream_only(
    coordinator: Coordinator, endpoint_base: str
) -> None:
    components = sample_components(endpoint_base)
    v1 = Snapshot.model_validate(json.loads((ROOT / "samples" / "snapshot-v1.json").read_text()))
    coordinator.submit_snapshot(v1, components)
    request = IncrementRequest.model_validate(
        json.loads((ROOT / "samples" / "increment-feature-checkout.json").read_text())
    )
    report = coordinator.submit_increment(request)

    assert report["changed_config_keys"] == ["feature.checkout"]
    assert report["affected_components"] == [
        "east-gateway",
        "east-orders",
        "south-gateway",
        "south-orders",
        "west-gateway",
        "west-orders",
    ]
    assert "east-audit" not in report["affected_components"]
    assert report["batches"] == [
        ["east-gateway", "south-gateway", "west-gateway"],
        ["east-orders", "south-orders", "west-orders"],
    ]
    invalidated = report["rebuild_scope"]["invalidated_artifacts"]
    assert len(invalidated) == 18
    assert all("east-audit" not in key for key in invalidated)

    derived = coordinator.get_derived(2)
    expected = next(
        artifact
        for artifact in derived["artifacts"]
        if artifact["artifact_key"] == "expected_digest:east-gateway"
    )
    assert "config:feature.checkout" in expected["refs"]
    assert "component:east-gateway" in expected["refs"]

    unrelated = IncrementRequest(base_version=2, config_set={"telemetry.sample_rate": 0.5})
    unrelated_report = coordinator.submit_increment(unrelated)
    assert unrelated_report["version"] == 3
    assert unrelated_report["affected_components"] == []
    assert unrelated_report["batches"] == []
    assert unrelated_report["rebuild_scope"]["invalidated_artifacts"] == []


def test_dependency_cycle_is_reported_with_ids_and_never_claimed(
    coordinator: Coordinator,
) -> None:
    components = [
        component("a", ["x"], depends_on=["b"]),
        component("b", ["x"], depends_on=["a"]),
    ]
    report = coordinator.submit_snapshot(snapshot(1, {"x": 1}), components)
    assert report["dependency_error"]["code"] == E_DEPENDENCY_CYCLE
    assert set(report["dependency_error"]["cycle_ids"]) == {"a", "b"}

    with pytest.raises(DomainError) as error:
        coordinator.get_plan(1)
    assert error.value.code == E_DEPENDENCY_CYCLE
    assert set(error.value.details["cycle_ids"]) == {"a", "b"}
    with pytest.raises(DomainError) as impact_error:
        coordinator.get_impact(1)
    assert impact_error.value.code == E_DEPENDENCY_CYCLE

    import asyncio

    with pytest.raises(DomainError) as dispatch_error:
        asyncio.run(coordinator.dispatch(1))
    assert dispatch_error.value.code == E_DEPENDENCY_CYCLE
    with pytest.raises(DomainError):
        coordinator.get_dispatch(1)
    assert {item["status"] for item in coordinator.get_state()["component_states"]} == {"PENDING"}


def test_full_snapshot_v2_uses_shared_version_space(
    coordinator: Coordinator, endpoint_base: str
) -> None:
    components = sample_components(endpoint_base)
    coordinator.submit_snapshot(
        Snapshot.model_validate(json.loads((ROOT / "samples" / "snapshot-v1.json").read_text())),
        components,
    )
    report = coordinator.submit_snapshot(
        Snapshot.model_validate(json.loads((ROOT / "samples" / "snapshot-v2.json").read_text())),
        components,
    )
    assert report["version"] == 2
    assert report["base_version"] == 1
    assert set(report["changed_config_keys"]) == {"db.pool", "feature.search"}
    assert "east-audit" not in report["affected_components"]
    assert "east-orders" in report["affected_components"]
    assert "east-gateway" in report["affected_components"]

