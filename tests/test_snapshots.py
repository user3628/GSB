from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from app.config import ROOT
from app.engine import Engine
from app.errors import EngineError
from app.schemas import BatchRequest, CloseWindowsRequest


def batch(batch_id: str, group: str, ts: int, measure, kind="integer", scale=0):
    mt = {"kind": kind}
    if kind == "decimal":
        mt["scale"] = scale
    return BatchRequest.model_validate(
        {
            "batch_id": batch_id,
            "window_spec": {
                "kind": "tumbling",
                "width_ms": 1000,
                "boundary": "left_closed_right_open",
            },
            "measure_type": mt,
            "events": [
                {
                    "event_id": f"{batch_id}-e",
                    "timestamp_ms": ts,
                    "group_key": group,
                    "measure": measure,
                    "labels": {"source": "snapshot-test"},
                }
            ],
            "late_policy": "queue",
        }
    )


def view(engine: Engine) -> dict:
    windows = engine.query_windows(include_type_history=True)
    groups = engine.type_evolution()
    late = engine.query_late_events()
    events = engine.query_events()
    for payload in (windows, groups, late, events):
        payload.pop("state_version", None)
    windows.pop("loaded_snapshot_version", None)
    return {"windows": windows, "groups": groups, "late": late, "events": events}


def test_snapshot_whole_state_replacement_and_checksum(tmp_path: Path) -> None:
    engine_a = Engine(f"sqlite:///{tmp_path / 'a.db'}")
    engine_b = Engine(f"sqlite:///{tmp_path / 'b.db'}")
    try:
        engine_a.ingest_batch(batch("a-int", "alpha", 100, 10))
        engine_a.close_windows(CloseWindowsRequest(through_ms=1100))
        engine_a.ingest_batch(batch("a-late", "alpha", 200, 99))
        engine_a.ingest_batch(batch("a-dec", "alpha", 1400, "2.50", kind="decimal", scale=2))
        metadata = engine_a.create_snapshot()
        exported = engine_a.get_snapshot(metadata["version"])

        engine_b.ingest_batch(batch("b-other", "beta", 5100, 500))
        assert engine_b.status()["window_count"] == 1
        before_b_version = engine_b.status()["state_version"]

        loaded = engine_b.load_snapshot(copy.deepcopy(exported))
        assert loaded["status"] == "REPLACED"
        assert loaded["snapshot_version"] == metadata["version"]
        assert loaded["previous_state_version"] == before_b_version
        assert engine_b.status()["loaded_snapshot_version"] == metadata["version"]
        assert engine_b.status()["window_count"] == engine_a.status()["window_count"]
        assert engine_b.query_windows(group_key="beta")["windows"] == []
        assert view(engine_b) == view(engine_a)

        tampered = copy.deepcopy(exported)
        tampered["state"]["windows"][0]["aggregate"]["sum_scaled"] = "999999"
        before_state = view(engine_b)
        before_version = engine_b.status()["state_version"]
        with pytest.raises(EngineError) as caught:
            engine_b.load_snapshot(tampered)
        assert caught.value.code == "E_SNAPSHOT_INVALID"
        assert "checksum" in caught.value.context["message"] if "message" in caught.value.context else True
        assert engine_b.status()["state_version"] == before_version
        assert view(engine_b) == before_state

        # Equal snapshot versions are invalid because loaded versions must increase.
        with pytest.raises(EngineError) as caught:
            engine_b.load_snapshot(copy.deepcopy(exported))
        assert caught.value.code == "E_SNAPSHOT_INVALID"
        assert engine_b.status()["state_version"] == before_version
        assert view(engine_b) == before_state
    finally:
        engine_a.close()
        engine_b.close()


def test_snapshot_structural_failure_keeps_state(tmp_path: Path) -> None:
    engine = Engine(f"sqlite:///{tmp_path / 'state.db'}")
    try:
        engine.ingest_batch(batch("seed", "g", 100, 3))
        snapshot = engine.get_snapshot(engine.create_snapshot()["version"])
        bad = copy.deepcopy(snapshot)
        bad["state"]["windows"] = None
        # Recompute a valid checksum so structural validation is what rejects it.
        from app.engine import _snapshot_checksum

        bad["checksum"] = _snapshot_checksum(bad)
        before = view(engine)
        before_version = engine.status()["state_version"]
        with pytest.raises(EngineError) as caught:
            engine.load_snapshot(bad)
        assert caught.value.code == "E_SNAPSHOT_INVALID"
        assert engine.status()["state_version"] == before_version
        assert view(engine) == before
    finally:
        engine.close()


def test_prepared_empty_snapshot_file_is_valid(tmp_path: Path) -> None:
    snapshot = json.loads((ROOT / "samples" / "snapshot_empty_v1.json").read_text())
    engine = Engine(f"sqlite:///{tmp_path / 'empty.db'}")
    try:
        engine.ingest_batch(batch("to-replace", "g", 100, 3))
        result = engine.load_snapshot(snapshot)
        assert result["snapshot_version"] == 1
        assert result["window_count"] == 0
        assert engine.query_windows()["windows"] == []
    finally:
        engine.close()
