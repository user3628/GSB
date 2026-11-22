from __future__ import annotations

import threading
from pathlib import Path
from unittest.mock import patch

import pytest

from app.db import SQLiteStore
from app.errors import WorkbenchError
from app.models import IncrementalRequest, Route, SnapshotRequest
from app.service import ConfigService


def make_route(
    route_id: str,
    *,
    upstream_port: int = 18081,
    timeout_ms: int = 600,
    path_prefix: str | None = None,
) -> Route:
    return Route.model_validate(
        {
            "route_id": route_id,
            "kind": "reverse",
            "methods": ["GET", "POST"],
            "path_prefix": path_prefix or f"/{route_id}",
            "upstream": f"http://127.0.0.1:{upstream_port}",
            "timeout_ms": timeout_ms,
            "retries": 1,
            "headers": {"X-Route": route_id},
            "health_status": "degraded",
            "priority": 100,
        }
    )


@pytest.fixture()
def service(tmp_path: Path) -> ConfigService:
    instance = ConfigService(tmp_path / "workbench.sqlite3")
    instance.startup()
    return instance


def submit_full(service: ConfigService, version: int, routes: list[Route]) -> dict:
    return service.submit_snapshot(SnapshotRequest(version=version, routes=routes))


def test_incremental_stale_base_does_not_change_committed_state(service: ConfigService) -> None:
    submit_full(service, 1, [make_route("alpha"), make_route("beta")])
    before = service.state()
    changed = make_route("alpha", upstream_port=18083)
    result = service.submit_incremental(
        IncrementalRequest(base_version=1, upserts=[changed])
    )
    assert result["version"] == 2
    current = service.state()
    assert current["version"] == 2

    with pytest.raises(WorkbenchError) as error:
        service.submit_incremental(IncrementalRequest(base_version=1, upserts=[changed]))
    assert error.value.business_code == "E_STALE_BASE"
    after_stale = service.state()
    assert after_stale["version"] == 2
    assert after_stale["digest"] == current["digest"]
    assert [route["route"]["route_id"] for route in after_stale["routes"]] == [
        "alpha",
        "beta",
    ]
    assert before["version"] == 1


def test_full_snapshot_replaces_all_routes_without_mixing(service: ConfigService) -> None:
    submit_full(service, 1, [make_route("old-a"), make_route("old-b")])
    submit_full(service, 4, [make_route("new-only")])
    state = service.state()
    assert state["version"] == 4
    assert state["route_count"] == 1
    assert [item["route"]["route_id"] for item in state["routes"]] == ["new-only"]
    assert service.store.budgets(4)[0]["route_id"] == "new-only"
    with pytest.raises(WorkbenchError) as error:
        submit_full(service, 4, [make_route("invalid-repeat")])
    assert error.value.business_code == "E_SNAPSHOT_INVALID"
    assert service.current is not None and service.current.version == 4


def test_cascade_trace_targets_changed_route_and_dependent_aggregates(
    service: ConfigService,
) -> None:
    submit_full(
        service,
        1,
        [make_route("alpha"), make_route("beta"), make_route("gamma")],
    )
    old_state = service.state(1)
    changed = make_route("alpha", upstream_port=18083, timeout_ms=777)
    result = service.submit_incremental(
        IncrementalRequest(base_version=1, upserts=[changed])
    )
    events = result["events"]
    assert any(
        event["route_id"] == "alpha"
        and event["artifact_kind"] == "forward_cache"
        and event["action"] == "INVALIDATED"
        and "upstream" in event["reason"]
        for event in events
    )
    assert any(
        event["artifact_kind"] == "match_index" and event["action"] == "REBUILT"
        for event in events
    )
    assert any(
        event["artifact_kind"] == "upstream_budget"
        and "18083" in event["reason"]
        for event in events
    )
    assert not any(event["route_id"] in {"beta", "gamma"} for event in events)

    new_state = service.state(2)
    old_beta = next(item for item in old_state["routes"] if item["route"]["route_id"] == "beta")
    new_beta = next(item for item in new_state["routes"] if item["route"]["route_id"] == "beta")
    assert {
        kind: artifact["digest"] for kind, artifact in old_beta["artifacts"].items()
    } == {
        kind: artifact["digest"] for kind, artifact in new_beta["artifacts"].items()
    }


def test_two_concurrent_increments_with_same_base_only_one_wins(
    service: ConfigService,
) -> None:
    submit_full(service, 1, [make_route("alpha"), make_route("beta")])
    barrier = threading.Barrier(2)
    outcomes: list[tuple[str, str | None]] = []

    def worker(route: Route) -> None:
        barrier.wait(timeout=3)
        try:
            result = service.submit_incremental(
                IncrementalRequest(base_version=1, upserts=[route])
            )
            outcomes.append(("ok", str(result["version"])))
        except WorkbenchError as exc:
            outcomes.append(("error", exc.business_code))

    threads = [
        threading.Thread(target=worker, args=(make_route("alpha", timeout_ms=700),)),
        threading.Thread(target=worker, args=(make_route("beta", timeout_ms=800),)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
    assert sorted(outcome[0] for outcome in outcomes) == ["error", "ok"]
    assert [code for status, code in outcomes if status == "error"] == ["E_STALE_BASE"]
    assert service.current is not None and service.current.version == 2


def test_failed_compilation_leaves_current_version_and_records_failed_intent(
    service: ConfigService,
) -> None:
    submit_full(service, 1, [make_route("alpha")])
    with patch.object(service.store, "commit_compiled", side_effect=RuntimeError("boom")):
        with pytest.raises(WorkbenchError) as error:
            service.submit_incremental(
                IncrementalRequest(
                    base_version=1,
                    upserts=[make_route("alpha", timeout_ms=999)],
                )
            )
    assert error.value.business_code == "E_SNAPSHOT_INVALID"
    assert service.current is not None and service.current.version == 1
    assert service.store.current_version() == 1
    intents = service.store.list_intents()
    assert intents[0]["state"] == "FAILED"


def test_startup_recovery_rolls_back_orphan_intent_to_last_complete_snapshot(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "recover.sqlite3"
    first = ConfigService(db_path)
    first.startup()
    submit_full(first, 1, [make_route("alpha")])
    orphan_id = first.store.create_intent(
        kind="INCREMENTAL",
        base_version=1,
        target_version=2,
        payload_digest="deadbeef",
        fingerprint=None,
        detail={"simulated": "crash-after-intent"},
    )

    restarted = ConfigService(db_path)
    restarted.startup()
    assert restarted.current is not None and restarted.current.version == 1
    report = restarted._startup_recovery
    assert report is not None
    assert report["last_complete_version"] == 1
    assert report["orphan_intent_ids"] == [orphan_id]
    assert report["rolled_back_intent_count"] == 1
    intents = restarted.store.list_intents()
    assert intents[0]["state"] == "ROLLED_BACK"
    assert restarted.store.current_version() == 1


def test_snapshot_budgets_and_artifacts_are_persisted_in_one_complete_version(
    service: ConfigService,
) -> None:
    submit_full(service, 1, [make_route("alpha"), make_route("beta")])
    store = SQLiteStore(service.store.db_path)
    budgets = store.budgets(1)
    artifacts = store.artifacts(1)
    assert [row["route_id"] for row in budgets] == ["alpha", "beta"]
    kinds = [row["kind"] for row in artifacts]
    for kind in (
        "match_entry",
        "pool_group",
        "health_plan",
        "forward_cache",
        "match_index",
        "upstream_budget",
    ):
        assert kind in kinds
    assert len([kind for kind in kinds if kind == "match_entry"]) == 2


def test_upstream_aggregate_is_insensitive_to_submission_order_and_batching(
    tmp_path: Path,
) -> None:
    alpha = make_route("alpha", timeout_ms=333)
    beta = make_route("beta", timeout_ms=997)
    gamma = make_route("gamma", timeout_ms=1237)
    base = make_route("base", timeout_ms=17)

    sequential_path = tmp_path / "sequential.sqlite3"
    sequential = ConfigService(sequential_path)
    sequential.startup()
    submit_full(sequential, 1, [base])
    sequential.submit_incremental(IncrementalRequest(base_version=1, upserts=[alpha]))
    sequential.submit_incremental(IncrementalRequest(base_version=2, upserts=[beta]))
    sequential.submit_incremental(IncrementalRequest(base_version=3, upserts=[gamma]))

    reordered_path = tmp_path / "reordered.sqlite3"
    reordered = ConfigService(reordered_path)
    reordered.startup()
    submit_full(reordered, 1, [base])
    reordered.submit_incremental(IncrementalRequest(base_version=1, upserts=[gamma]))
    reordered.submit_incremental(IncrementalRequest(base_version=2, upserts=[beta]))
    reordered.submit_incremental(IncrementalRequest(base_version=3, upserts=[alpha]))

    batched_path = tmp_path / "batched.sqlite3"
    batched = ConfigService(batched_path)
    batched.startup()
    submit_full(batched, 1, [base])
    batched.submit_incremental(
        IncrementalRequest(base_version=1, upserts=[beta, gamma, alpha])
    )

    def aggregate(state: dict) -> dict:
        return {
            item["upstream"]: (
                item["route_ids"],
                item["total_wait_q6"],
                item["health_penalty_q6"],
            )
            for item in state["upstream_aggregates"]
        }

    sequential_state = sequential.state()
    reordered_state = reordered.state()
    batched_state = batched.state()
    assert aggregate(sequential_state) == aggregate(reordered_state) == aggregate(
        batched_state
    )
    assert sequential_state["digest"] == reordered_state["digest"]
