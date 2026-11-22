from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np
import pytest

from multiscale_fusion.checkpoint import CheckpointStore
from multiscale_fusion.engine import FusionEngine
from multiscale_fusion.errors import FusionError
from multiscale_fusion.util import pretty_json_bytes, sha256_json, write_bytes, write_json


def test_checkpoint_chain_is_normal_and_contiguous(engine: FusionEngine) -> None:
    result = engine.run("demo_fpn")
    assert result["head_sequence"] == 3
    verification = engine.verify_checkpoints("demo_fpn")
    assert verification["ok"] is True
    assert verification["classification"] == "normal"
    assert [record["sequence"] for record in verification["records"]] == [1, 2, 3]
    assert [record["layer"] for record in verification["records"]] == ["p5", "p4", "p3"]


def test_artifact_tamper_is_detected(engine: FusionEngine) -> None:
    engine.run("demo_fpn")
    store = engine.store("demo_fpn")
    state = store.scan()
    record = state.latest_for_layer("p3")
    assert record is not None
    raw_path = store.artifacts_dir / record["stage_version"] / "raw.npy"
    with open(raw_path, "ab") as handle:
        handle.write(b"tamper")
    with pytest.raises(FusionError) as excinfo:
        store.verify()
    error = excinfo.value
    assert error.code == "E_STALE_CHECKPOINT"
    assert error.details["classification"] == "tamper"
    assert error.details["reason"] == "artifact_digest_mismatch"


def test_sequence_rollback_is_distinguished(engine: FusionEngine) -> None:
    engine.run("demo_fpn")
    store = engine.store("demo_fpn")
    state = store.scan()
    last_path = Path(state.records[-1]["_path"])
    last_path.unlink()
    with pytest.raises(FusionError) as excinfo:
        store.verify()
    error = excinfo.value
    assert error.code == "E_STALE_CHECKPOINT"
    assert error.details["classification"] == "rollback"
    assert error.details["reason"] == "head_ahead_of_records"


def test_record_replacement_is_detected_as_tamper(engine: FusionEngine) -> None:
    engine.run("demo_fpn")
    store = engine.store("demo_fpn")
    state = store.scan()
    record_path = Path(state.records[-1]["_path"])
    value = json.loads(record_path.read_text(encoding="utf-8"))
    value["input_fingerprint"] = "0" * 64
    write_bytes(record_path, pretty_json_bytes(value))
    with pytest.raises(FusionError) as excinfo:
        store.verify()
    error = excinfo.value
    assert error.code == "E_STALE_CHECKPOINT"
    assert error.details["classification"] == "tamper"
    assert error.details["reason"] == "head_digest_mismatch"


def test_head_is_repaired_after_non_atomic_crash_window(engine: FusionEngine) -> None:
    engine.run("demo_fpn")
    store = engine.store("demo_fpn")
    store.head_path.unlink()
    state = store.scan(repair_head=True)
    assert state.head_sequence == 3
    assert store.verify()["classification"] == "normal"


def test_partial_temp_records_are_ignored(engine: FusionEngine) -> None:
    engine.run("demo_fpn")
    store = engine.store("demo_fpn")
    (store.checkpoints_dir / ".tmp-1234").write_text("{half", encoding="utf-8")
    (store.checkpoints_dir / "00000004.json.tmp").write_text("{half", encoding="utf-8")
    verification = store.verify()
    assert verification["record_count"] == 3
    assert [item["sequence"] for item in verification["records"]] == [1, 2, 3]


def test_strict_publication_rejects_duplicate_and_gap(engine: FusionEngine) -> None:
    engine.run("demo_fpn")
    store = engine.store("demo_fpn")
    kwargs = dict(
        layer="extra",
        stage_index=3,
        stage_version="f" * 64,
        own_fingerprint="e" * 64,
        dependency_version="p3-version",
        component_fingerprints={"input": "a" * 64},
        artifact_files={"raw.npy": b"not-used"},
    )
    with pytest.raises(FusionError) as excinfo:
        store.publish_stage(**kwargs, expected_sequence=3)
    assert excinfo.value.code == "E_STALE_CHECKPOINT"
    assert excinfo.value.details["reason"] == "publication_rollback_or_duplicate"
    with pytest.raises(FusionError) as excinfo:
        store.publish_stage(**kwargs, expected_sequence=99)
    assert excinfo.value.code == "E_STALE_CHECKPOINT"
    assert excinfo.value.details["reason"] == "publication_gap"


def test_cross_task_publication_and_missing_prerequisite(engine: FusionEngine) -> None:
    store = engine.store("demo_fpn")
    with pytest.raises(FusionError) as excinfo:
        store.publish_stage(
            layer="p5",
            stage_index=0,
            stage_version="a" * 64,
            own_fingerprint="b" * 64,
            dependency_version=None,
            component_fingerprints={"input": "c" * 64},
            artifact_files={"raw.npy": b"x"},
            task_id_claim="other_task",
        )
    assert excinfo.value.code == "E_STALE_CHECKPOINT"
    assert excinfo.value.details["reason"] == "cross_task_publish"
    with pytest.raises(FusionError) as excinfo:
        store.publish_stage(
            layer="p4",
            stage_index=1,
            stage_version="d" * 64,
            own_fingerprint="e" * 64,
            dependency_version="missing",
            component_fingerprints={"input": "f" * 64},
            artifact_files={"raw.npy": b"x"},
            prerequisites=[("p5", "missing")],
        )
    assert excinfo.value.code == "E_INVALID_STAGE"
    assert excinfo.value.layer == "p4"


def test_record_write_failure_leaves_no_partial_checkpoint(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import multiscale_fusion.checkpoint as checkpoint_module

    def fail_write(path: str, data: bytes, *, atomic: bool = True) -> None:
        target = Path(path)
        if target.parent.name == "checkpoints":
            raise OSError("simulated crash before record replace")
        from multiscale_fusion import util

        util.write_bytes(path, data, atomic=atomic)

    monkeypatch.setattr(checkpoint_module, "write_bytes", fail_write)
    engine = FusionEngine(workspace)
    with pytest.raises(OSError):
        engine.run("demo_fpn")
    store = engine.store("demo_fpn")
    state = store.scan()
    assert state.records == []
    assert state.head_sequence == 0
