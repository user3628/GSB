from __future__ import annotations

import json
from pathlib import Path

import pytest

from replication_guard.errors import AppError
from replication_guard.models import Observation
from replication_guard.service import ReplicationService
from replication_guard.state_machine import deduplicate_observations


SAMPLES = Path(__file__).resolve().parents[1] / "samples"


def _seed(service: ReplicationService) -> dict:
    service.import_policies(
        (SAMPLES / "policies.jsonl").read_bytes(), format_hint="jsonl", batch_id="state-policies"
    )
    payload = json.loads((SAMPLES / "observations.json").read_text("utf-8"))
    service.ingest_observations(payload["observations"], batch_id=payload["batchId"])
    return payload


def _healthy_payload(batch_id: str, base_time: int) -> dict:
    observations = []
    for index, slave_id in enumerate(
        ["replica-east-01", "replica-west-02", "replica-north-03"]
    ):
        version = {
            "replica-east-01": "8.0.32-24",
            "replica-west-02": "8.4.2-1",
            "replica-north-03": "5.7.43",
        }[slave_id]
        for offset in (0, 60000):
            observations.append(
                {
                    "slave_id": slave_id,
                    "sampled_at_ms": base_time + offset,
                    "binlog_gap": index + offset // 1000,
                    "delay_ms": 30,
                    "reported_version": version,
                }
            )
    return {"batchId": batch_id, "observations": observations}


def test_sample_verdicts_cover_cross_version_and_priority(service: ReplicationService) -> None:
    payload = _seed(service)
    result = service.recalculate(
        batch_id="state-calc-1", observation_batch_id=payload["batchId"], window_size=2
    )
    rows = {row["slaveId"]: row for row in result["results"]}

    assert rows["replica-east-01"]["stateAfter"] == "DELAYED"
    assert rows["replica-east-01"]["selectedObservation"]["binlog_gap"] == 80
    assert rows["replica-east-01"]["effectiveAction"] == "THROTTLE_WRITES"

    assert rows["replica-west-02"]["versionRelation"] == "ABOVE"
    assert rows["replica-west-02"]["stateAfter"] == "DELAYED"
    assert rows["replica-west-02"]["requiresManualReview"] is False

    assert rows["replica-north-03"]["versionRelation"] == "BELOW"
    assert rows["replica-north-03"]["stateAfter"] == "DEGRADED"
    assert rows["replica-north-03"]["effectiveAction"] == "FAILOVER"
    assert rows["replica-north-03"]["priorityBand"] == "LOW"

    assert rows["replica-south-04"]["versionRelation"] == "ABOVE"
    assert rows["replica-south-04"]["requiresManualReview"] is True
    assert rows["replica-south-04"]["businessCode"] == "E_VERSION_AHEAD"
    assert rows["replica-south-04"]["stateAfter"] == "HEALTHY"
    assert result["businessCode"] == "E_VERSION_AHEAD"
    assert result["unmatchedObservations"][0]["businessCode"] == "E_POLICY_MISSING"


def test_observation_selection_is_independent_of_arrival_order(tmp_path: Path) -> None:
    payload = json.loads((SAMPLES / "observations.json").read_text("utf-8"))
    first = ReplicationService(tmp_path / "order-a.sqlite3")
    second = ReplicationService(tmp_path / "order-b.sqlite3")
    first.startup()
    second.startup()
    text = (SAMPLES / "policies.jsonl").read_bytes()
    first.import_policies(text, format_hint="jsonl", batch_id="p")
    second.import_policies(text, format_hint="jsonl", batch_id="p")
    first.ingest_observations(payload["observations"], batch_id="o")
    second.ingest_observations(list(reversed(payload["observations"])), batch_id="o")
    result_a = first.recalculate(batch_id="r", observation_batch_id="o", window_size=2)
    result_b = second.recalculate(batch_id="r", observation_batch_id="o", window_size=2)
    row_a = next(row for row in result_a["results"] if row["slaveId"] == "replica-east-01")
    row_b = next(row for row in result_b["results"] if row["slaveId"] == "replica-east-01")
    assert row_a["stateAfter"] == row_b["stateAfter"] == "DELAYED"
    assert row_a["selectedObservation"]["binlog_gap"] == row_b["selectedObservation"]["binlog_gap"] == 80
    assert row_a["trigger"] == row_b["trigger"]


def test_state_machine_restores_then_returns_healthy(service: ReplicationService) -> None:
    payload = _seed(service)
    first = service.recalculate(batch_id="calc-degrade", observation_batch_id=payload["batchId"], window_size=2)
    first_states = {row["slaveId"]: row["stateAfter"] for row in first["results"]}
    assert first_states["replica-north-03"] == "DEGRADED"

    healthy_1 = _healthy_payload("healthy-1", 1763000300000)
    service.ingest_observations(healthy_1["observations"], batch_id=healthy_1["batchId"])
    restored = service.recalculate(batch_id="calc-restored", observation_batch_id="healthy-1", window_size=2)
    restored_states = {row["slaveId"]: row["stateAfter"] for row in restored["results"]}
    assert restored_states == {
        "replica-east-01": "RESTORED",
        "replica-north-03": "RESTORED",
        "replica-west-02": "RESTORED",
    }

    healthy_2 = _healthy_payload("healthy-2", 1763000400000)
    service.ingest_observations(healthy_2["observations"], batch_id="healthy-2")
    healthy = service.recalculate(batch_id="calc-healthy", observation_batch_id="healthy-2", window_size=2)
    healthy_states = {row["slaveId"]: row["stateAfter"] for row in healthy["results"]}
    assert set(healthy_states.values()) == {"HEALTHY"}
    assert all(
        row["trigger"]["code"] == "HEALTHY_CONFIRMED" for row in healthy["results"]
    )


def test_invalid_transition_does_not_change_persisted_state(service: ReplicationService) -> None:
    payload = _seed(service)
    service.recalculate(batch_id="calc-before-invalid", observation_batch_id=payload["batchId"], window_size=2)
    before = next(
        row for row in service.list_states()["states"] if row["slaveId"] == "replica-north-03"
    )
    with pytest.raises(AppError) as captured:
        service.transition_state("replica-north-03", "HEALTHY", batch_id="invalid-transition")
    assert captured.value.code == "E_INVALID_TRANSITION"
    after = next(
        row for row in service.list_states()["states"] if row["slaveId"] == "replica-north-03"
    )
    assert after["state"] == before["state"] == "DEGRADED"
    assert after["updatedAtMs"] == before["updatedAtMs"]


def test_deduplication_tie_break_is_deterministic() -> None:
    observations = [
        Observation("s", 1000, 10, 500, "8.0.2"),
        Observation("s", 1000, 10, 700, "8.0.1"),
        Observation("s", 1000, 10, 600, "8.0.3"),
    ]
    selected = deduplicate_observations(observations)[0]
    assert selected.delay_ms == 700

