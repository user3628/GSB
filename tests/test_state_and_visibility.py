from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from config_coordinator import Coordinator, DomainError, TransitionRequest
from config_coordinator.codec import expected_config_digest
from config_coordinator.errors import E_INVALID_TRANSITION

from conftest import component, snapshot


def test_legal_and_illegal_transitions_leave_no_partial_write(coordinator: Coordinator) -> None:
    coordinator.submit_snapshot(
        snapshot(1, {"a": 1}),
        [component("svc", ["a"], logical_component="svc")],
    )
    before = coordinator.get_state()
    with pytest.raises(DomainError) as error:
        coordinator.transition_component(
            "svc",
            TransitionRequest(
                snapshot_version=1,
                to_status="APPLIED",
                reason="illegal shortcut",
            ),
        )
    assert error.value.code == E_INVALID_TRANSITION
    after = coordinator.get_state()
    assert after["component_states"] == before["component_states"]
    assert after["state_events"] == before["state_events"]

    coordinator.transition_component(
        "svc",
        TransitionRequest(snapshot_version=1, to_status="DISPATCHED", reason="manual dispatch"),
    )
    with pytest.raises(DomainError) as illegal_back:
        coordinator.transition_component(
            "svc",
            TransitionRequest(snapshot_version=1, to_status="PENDING", reason="illegal reset"),
        )
    assert illegal_back.value.code == E_INVALID_TRANSITION
    state = coordinator.get_component_state("svc", 1)
    assert state["state"]["status"] == "DISPATCHED"

    coordinator.transition_component(
        "svc",
        TransitionRequest(snapshot_version=1, to_status="APPLIED", reason="manual apply"),
    )
    state = coordinator.get_component_state("svc", 1)
    assert state["state"]["status"] == "APPLIED"
    assert state["state"]["last_applied_digest"] == state["state"]["desired_digest"]
    assert [(event["from_status"], event["to_status"]) for event in state["events"]] == [
        (None, "PENDING"),
        ("PENDING", "DISPATCHED"),
        ("DISPATCHED", "APPLIED"),
    ]
    consistency = next(
        artifact
        for artifact in coordinator.get_derived(1)["artifacts"]
        if artifact["artifact_key"] == "consistency_score:svc"
    )
    assert consistency["value"]["score"] == 1.0


def test_concurrent_readers_never_observe_snapshot_derived_mismatch(
    coordinator: Coordinator, tmp_path: Path
) -> None:
    components = [
        component("a", ["x"], logical_component="a"),
        component("b", ["y"], logical_component="b"),
    ]
    coordinator.submit_snapshot(snapshot(1, {"x": 1, "y": 2}), components)
    stop = threading.Event()
    observations: list[tuple[int, dict]] = []
    errors: list[BaseException] = []

    def reader() -> None:
        reader_coordinator = Coordinator(str(tmp_path / "coordinator.db"))
        try:
            while not stop.is_set():
                state = reader_coordinator.get_state()
                observations.append((state["current_version"], state))
        except BaseException as exc:  # pragma: no cover - failure is asserted below
            errors.append(exc)

    thread = threading.Thread(target=reader, daemon=True)
    thread.start()
    for value in range(3, 13):
        from config_coordinator import IncrementRequest

        coordinator.submit_increment(
            IncrementRequest(base_version=coordinator.current_version(), config_set={"x": value})
        )
    time.sleep(0.15)
    stop.set()
    thread.join(timeout=5)
    assert not errors
    assert observations
    for version, state in observations:
        assert state["snapshot"]["version"] == version
        assert len(state["derived_artifacts"]) == len(state["snapshot"]["components"]) * 3
        for item in state["snapshot"]["components"]:
            expected = expected_config_digest(
                item["config_keys"],
                item["expected_config_version"],
                state["snapshot"]["configs"],
            )
            artifact = next(
                artifact
                for artifact in state["derived_artifacts"]
                if artifact["artifact_key"] == f"expected_digest:{item['component_id']}"
            )
            assert artifact["value"]["digest"] == expected

