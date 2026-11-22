from __future__ import annotations

import json
import multiprocessing
from pathlib import Path

import pytest

from audioq.canonical import atomic_write_json, sha256_json
from audioq.checkpoints import CheckpointManager
from audioq.errors import AudioQError


def _concurrent_publish_worker(state_dir: str, shard_id: str, queue) -> None:
    try:
        manager = CheckpointManager(state_dir)
        record = manager.publish_checkpoint(
            shard_id=shard_id,
            stage="frame",
            input_fingerprint="shared-frame-fingerprint",
            config_fingerprint="shared-config",
            artifact={"shard_id": shard_id, "stage": "frame", "marker": "concurrent"},
        )
        queue.put({"ok": True, "shard_id": shard_id, "seq": record["seq"]})
    except AudioQError as exc:
        queue.put({"ok": False, "shard_id": shard_id, "error": exc.to_dict()})
    except Exception as exc:  # pragma: no cover - surfaced to the parent assertion
        queue.put({"ok": False, "shard_id": shard_id, "error": repr(exc)})


def _artifact(shard: str, stage: str, marker: str = "a"):
    return {"shard_id": shard, "stage": stage, "marker": marker}


def _publish_all(manager: CheckpointManager, shard: str = "s1"):
    frame = manager.publish_checkpoint(
        shard_id=shard,
        stage="frame",
        input_fingerprint="fp-frame",
        config_fingerprint="fp-config",
        artifact=_artifact(shard, "frame"),
    )
    metrics = manager.publish_checkpoint(
        shard_id=shard,
        stage="metrics",
        input_fingerprint="fp-metrics",
        config_fingerprint="fp-config",
        artifact=_artifact(shard, "metrics"),
        prerequisites={"frame": frame["artifact_digest"]},
    )
    fusion = manager.publish_checkpoint(
        shard_id=shard,
        stage="fusion",
        input_fingerprint="fp-fusion",
        config_fingerprint="fp-config",
        artifact=_artifact(shard, "fusion"),
        prerequisites={"frame": frame["artifact_digest"], "metrics": metrics["artifact_digest"]},
    )
    diagnostics = manager.publish_checkpoint(
        shard_id=shard,
        stage="diagnostics",
        input_fingerprint="fp-diag",
        config_fingerprint="fp-config",
        artifact=_artifact(shard, "diagnostics"),
        prerequisites={
            "frame": frame["artifact_digest"],
            "metrics": metrics["artifact_digest"],
            "fusion": fusion["artifact_digest"],
        },
    )
    return [frame, metrics, fusion, diagnostics]


def test_checkpoint_sequence_and_prerequisite_errors(tmp_path):
    manager = CheckpointManager(tmp_path / "state")
    with pytest.raises(AudioQError) as caught:
        manager.publish_checkpoint(
            shard_id="s1",
            stage="metrics",
            input_fingerprint="fp-metrics",
            config_fingerprint="fp-config",
            artifact=_artifact("s1", "metrics"),
        )
    assert caught.value.code == "E_INVALID_STAGE"
    assert caught.value.observed["missing_stage"] == "frame"

    records = _publish_all(manager)
    assert [record["seq"] for record in records] == [1, 2, 3, 4]

    with pytest.raises(AudioQError) as caught:
        manager.publish_checkpoint(
            shard_id="s1",
            stage="fusion",
            input_fingerprint="fp-fusion",
            config_fingerprint="fp-config",
            artifact=_artifact("s1", "fusion"),
            prerequisites={
                "frame": records[0]["artifact_digest"],
                "metrics": records[1]["artifact_digest"],
            },
        )
    assert caught.value.code == "E_STALE_CHECKPOINT"

    with pytest.raises(AudioQError) as caught:
        manager.publish_checkpoint(
            shard_id="s1",
            stage="diagnostics",
            input_fingerprint="new-diag",
            config_fingerprint="fp-config",
            artifact=_artifact("s1", "diagnostics", "new"),
            prerequisites={
                "frame": records[0]["artifact_digest"],
                "metrics": records[1]["artifact_digest"],
                "fusion": records[2]["artifact_digest"],
            },
            expected_seq=3,
        )
    assert caught.value.code == "E_STALE_CHECKPOINT"
    assert caught.value.observed["next_seq"] == 5


def test_cross_shard_artifact_is_rejected(tmp_path):
    manager = CheckpointManager(tmp_path / "state")
    with pytest.raises(AudioQError) as caught:
        manager.publish_checkpoint(
            shard_id="shard-a",
            stage="frame",
            input_fingerprint="fp",
            config_fingerprint="cfg",
            artifact=_artifact("shard-b", "frame"),
        )
    assert caught.value.code == "E_STALE_CHECKPOINT"
    assert caught.value.observed["reason"] == "cross_shard"


def test_corrupt_and_duplicate_records_are_ignored_and_quarantined(tmp_path):
    manager = CheckpointManager(tmp_path / "state")
    records = _publish_all(manager)
    metrics_path = manager._shard_dir("s1") / "00000002-metrics.json"
    metrics_path.write_text('{"half":', encoding="utf-8")
    scan = manager.scan_shard("s1", repair=False)
    assert scan.last_seq == 1
    assert any(item["reason"] == "unreadable_or_invalid_json" for item in scan.invalid)

    scan = manager.scan_shard("s1", repair=True)
    assert scan.last_seq == 1
    assert Path(scan.invalid[0]["path"]).exists() is False

    replacement_metrics = manager.publish_checkpoint(
        shard_id="s1",
        stage="metrics",
        input_fingerprint="fp-metrics-rebuilt",
        config_fingerprint="fp-config",
        artifact=_artifact("s1", "metrics", "rebuilt"),
        prerequisites={"frame": records[0]["artifact_digest"]},
    )
    assert replacement_metrics["seq"] == 2

    # A second valid record with a duplicate sequence makes the whole sequence unusable.
    original = json.loads(metrics_path.read_text(encoding="utf-8"))
    duplicate = dict(original)
    duplicate["stage"] = "fusion"
    duplicate["record_digest"] = sha256_json({k: v for k, v in duplicate.items() if k != "record_digest"})
    atomic_write_json(manager._shard_dir("s1") / "00000002-fusion.json", duplicate, pretty=False)
    duplicate_scan = manager.scan_shard("s1", repair=False)
    assert duplicate_scan.last_seq == 1
    assert 2 in duplicate_scan.duplicate_sequences


def test_lock_releases_after_exception_and_there_are_no_temp_files(tmp_path):
    manager = CheckpointManager(tmp_path / "state")
    _publish_all(manager)
    with pytest.raises(RuntimeError):
        with __import__("audioq.canonical", fromlist=["file_lock"]).file_lock(manager.lock_path, timeout=1):
            raise RuntimeError("boom")
    with __import__("audioq.canonical", fromlist=["file_lock"]).file_lock(manager.lock_path, timeout=1):
        pass
    assert not list(manager.root.rglob("*.tmp"))


def test_multi_process_publication_is_serialised_and_duplicates_rejected(tmp_path):
    context = multiprocessing.get_context("fork")
    queue = context.Queue()
    state = str(tmp_path / "state")
    processes = [
        context.Process(target=_concurrent_publish_worker, args=(state, shard, queue))
        for shard in ("a", "b", "c", "d")
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0
    results = [queue.get(timeout=1) for _ in range(4)]
    assert all(result["ok"] and result["seq"] == 1 for result in results)

    duplicate_state = str(tmp_path / "duplicate-state")
    duplicate_queue = context.Queue()
    duplicate_processes = [
        context.Process(target=_concurrent_publish_worker, args=(duplicate_state, "same", duplicate_queue))
        for _ in range(4)
    ]
    for process in duplicate_processes:
        process.start()
    for process in duplicate_processes:
        process.join(timeout=10)
        assert process.exitcode == 0
    duplicate_results = [duplicate_queue.get(timeout=1) for _ in range(4)]
    assert sum(1 for result in duplicate_results if result["ok"]) == 1
    rejected = [result for result in duplicate_results if not result["ok"]]
    assert len(rejected) == 3
    assert all(result["error"]["code"] == "E_STALE_CHECKPOINT" for result in rejected)
    assert CheckpointManager(duplicate_state).scan_shard("same").last_seq == 1
