from __future__ import annotations

import json
import multiprocessing
from pathlib import Path

import pytest

from image_workbench.atomic import atomic_write_bytes
from image_workbench.errors import E_SNAPSHOT_DIGEST, E_STALE_SNAPSHOT, WorkbenchError
from image_workbench.snapshots import SnapshotManager, finalize_snapshot


def _record(fragment_id: str = "fragment_a", stage: str = "source") -> dict:
    return {
        "fragment_id": fragment_id,
        "stage": stage,
        "input_fingerprint": "in-" + fragment_id,
        "config_fingerprint": "cfg-" + stage,
        "artifact_digest": "art-" + fragment_id + stage,
        "status": "rebuilt",
        "reason": "test",
    }


def test_snapshot_sequence_advance_rollback_and_duplicate(tmp_path: Path) -> None:
    manager = SnapshotManager(tmp_path)
    first = finalize_snapshot(1, [_record()])
    assert manager.publish(first)["status"] == "published"
    second = finalize_snapshot(2, [_record(stage="repair")])
    manager.publish(second)
    assert manager.current_status()["sequence"] == 2
    assert manager.inspect_candidate(first)["status"] == "rollback"
    with pytest.raises(WorkbenchError) as caught:
        manager.publish(second)
    assert caught.value.code == E_STALE_SNAPSHOT
    duplicate = finalize_snapshot(2, [])
    with pytest.raises(WorkbenchError) as caught:
        manager.publish(duplicate)
    assert caught.value.code == E_STALE_SNAPSHOT


def test_snapshot_gap_and_digest_mismatch(tmp_path: Path) -> None:
    manager = SnapshotManager(tmp_path)
    manager.publish(finalize_snapshot(1, []))
    gap = finalize_snapshot(3, [])
    assert manager.inspect_candidate(gap)["status"] == "gap"
    with pytest.raises(WorkbenchError) as caught:
        manager.publish(gap)
    assert caught.value.code == E_STALE_SNAPSHOT
    tampered = finalize_snapshot(2, [])
    tampered["digest"] = "0" * 64
    assert manager.inspect_candidate(tampered)["status"] == "digest_mismatch"
    with pytest.raises(WorkbenchError) as caught:
        manager.publish(tampered)
    assert caught.value.code == E_SNAPSHOT_DIGEST


def test_snapshot_non_finite_payload_is_digest_error(tmp_path: Path) -> None:
    manager = SnapshotManager(tmp_path)
    manager.publish(finalize_snapshot(1, []))
    candidate = {"snapshot_version": 1, "sequence": 2, "records": [], "payload": {"bad": float("nan")}, "digest": "x"}
    assert manager.inspect_candidate(candidate)["status"] == "digest_mismatch"
    with pytest.raises(WorkbenchError) as caught:
        manager.publish(candidate)
    assert caught.value.code == E_SNAPSHOT_DIGEST


def test_corrupt_current_snapshot_is_reported(tmp_path: Path) -> None:
    manager = SnapshotManager(tmp_path)
    manager.publish(finalize_snapshot(1, []))
    value = json.loads(manager.current_path.read_text(encoding="utf-8"))
    value["records"] = [{"tampered": True}]
    manager.current_path.write_text(json.dumps(value), encoding="utf-8")
    status = manager.current_status()
    assert status["status"] == "digest_mismatch"


def test_replaced_current_snapshot_rollback_is_detected_and_recoverable(tmp_path: Path) -> None:
    manager = SnapshotManager(tmp_path)
    first = finalize_snapshot(1, [])
    manager.publish(first)
    second_path_value = finalize_snapshot(2, [])
    manager.publish(second_path_value)
    manager.current_path.write_text(json.dumps(first), encoding="utf-8")
    status = manager.current_status()
    assert status["status"] == "rollback"
    assert status["high_watermark_sequence"] == 2
    assert manager.inspect_candidate(second_path_value)["status"] == "rollback"
    recovery = finalize_snapshot(3, [])
    assert manager.inspect_candidate(recovery)["status"] == "normal_advance"
    assert manager.publish(recovery)["status"] == "published"
    assert manager.current_status()["status"] == "normal"


def test_stage_record_atomic_write(tmp_path: Path) -> None:
    manager = SnapshotManager(tmp_path)
    path = manager.write_stage_record(1, _record())
    value = json.loads(path.read_text(encoding="utf-8"))
    assert value["snapshot_sequence"] == 1
    assert value["fragment_id"] == "fragment_a"


def test_aborted_atomic_write_leaves_no_partial_target(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = tmp_path / "record.json"
    import image_workbench.atomic as atomic_module

    def fail_replace(source, destination):
        raise RuntimeError("simulated interruption")

    monkeypatch.setattr(atomic_module.os, "replace", fail_replace)
    with pytest.raises(RuntimeError):
        atomic_write_bytes(target, b'{"complete": true}')
    assert not target.exists()
    assert list(tmp_path.glob(".record.json.*.tmp")) == []


def _publisher_worker(root: str, sequence: int, queue) -> None:
    try:
        manager = SnapshotManager(root)
        manager.publish(finalize_snapshot(sequence, []))
        queue.put("published")
    except WorkbenchError as error:
        queue.put(error.code)


def test_multiprocess_publishers_allow_only_one_initial_sequence(tmp_path: Path) -> None:
    context = multiprocessing.get_context("spawn")
    queue = context.Queue()
    processes = [
        context.Process(target=_publisher_worker, args=(str(tmp_path), 1, queue)),
        context.Process(target=_publisher_worker, args=(str(tmp_path), 1, queue)),
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=20)
        assert process.exitcode == 0
    outcomes = sorted(queue.get(timeout=2) for _ in range(2))
    assert outcomes == sorted(["published", E_STALE_SNAPSHOT])

