from __future__ import annotations

import json
import multiprocessing
import shutil
import time
import pytest

import embedding_gather.digest as digest_module
from embedding_gather.checkpoint import CheckpointStore, FileLock
from embedding_gather.digest import atomic_write_bytes, canonical_json_bytes
from embedding_gather.errors import E_INVALID_STAGE, E_STALE_CHECKPOINT, BusinessError


def _publish(store, stage, *, version=2, marker="x", product="digest"):
    return store.publish_stage(
        stage=stage,
        components={"stage": stage, "marker": marker},
        input_fingerprint=f"input:{stage}:{marker}",
        manifest_fingerprint="manifest-fingerprint",
        manifest_version=version,
        config_version="config-v1",
        product_digest=product,
    )


def test_atomic_write_never_leaves_partial_target(tmp_path, monkeypatch):
    target = tmp_path / "record.json"

    def boom(source, destination):
        raise OSError("simulated crash during replace")

    monkeypatch.setattr(digest_module.os, "replace", boom)
    with pytest.raises(OSError):
        atomic_write_bytes(target, b"complete-record")
    assert not target.exists()
    assert list(tmp_path.glob(".record.json.*.tmp")) == []


def test_stage_sequence_and_duplicate_and_missing_prerequisite(tmp_path):
    store = CheckpointStore(tmp_path, "batch-a")
    with pytest.raises(BusinessError) as error:
        _publish(store, "gather")
    assert error.value.code == E_INVALID_STAGE
    _publish(store, "validate")
    _publish(store, "gather")
    with pytest.raises(BusinessError) as error:
        _publish(store, "gather")
    assert error.value.code == E_STALE_CHECKPOINT
    assert error.value.context["reason"] == "duplicate"
    records = store.read_records()
    assert [record["sequence"] for record in records] == [1, 2]
    assert records[0]["shard_id"] == "batch-a"
    assert records[0]["input_fingerprint"] == "input:validate:x"
    assert records[0]["manifest_fingerprint"] == "manifest-fingerprint"
    assert records[0]["config_version"] == "config-v1"
    assert records[0]["product_digest"] == "digest"


def test_manifest_version_regression_is_rejected(tmp_path):
    store = CheckpointStore(tmp_path, "batch-a")
    _publish(store, "validate", version=2)
    with pytest.raises(BusinessError) as error:
        _publish(store, "gather", version=1)
    assert error.value.code == E_STALE_CHECKPOINT
    assert error.value.context["reason"] == "rollback"


def test_tamper_rollback_and_cross_partition_are_distinguished(tmp_path):
    store = CheckpointStore(tmp_path, "batch-a")
    for stage in ("validate", "gather", "quantize", "verify"):
        _publish(store, stage)

    first = sorted(store.records_dir.glob("*.json"))[0]
    value = json.loads(first.read_text(encoding="utf-8"))
    value["product_digest"] = "tampered"
    first.write_bytes(canonical_json_bytes(value))
    with pytest.raises(BusinessError) as error:
        store.read_records()
    assert error.value.code == E_STALE_CHECKPOINT
    assert error.value.context["reason"] == "tamper"

    shutil.rmtree(store.records_dir)
    shutil.rmtree(store.anchor_path.parent)
    store = CheckpointStore(tmp_path, "batch-b")
    for stage in ("validate", "gather", "quantize", "verify"):
        _publish(store, stage)
    files = sorted(store.records_dir.glob("*.json"))
    files[-1].unlink()
    files[-2].unlink()
    with pytest.raises(BusinessError) as error:
        store.read_records()
    assert error.value.code == E_STALE_CHECKPOINT
    assert error.value.context["reason"] == "rollback"

    left = CheckpointStore(tmp_path, "left")
    right = CheckpointStore(tmp_path, "right")
    _publish(left, "validate")
    _publish(right, "validate")
    right_file = sorted(right.records_dir.glob("*.json"))[0]
    target = left.records_dir / "000002-validate.json"
    target.write_bytes(right_file.read_bytes())
    with pytest.raises(BusinessError) as error:
        left.read_records()
    assert error.value.code == E_STALE_CHECKPOINT
    assert error.value.context["reason"] == "cross_shard"


def _lock_worker(path: str, log_path: str, name: str, hold: float) -> None:
    with FileLock(path):
        with open(log_path, "a", encoding="utf-8") as handle:
            handle.write(f"{name}-start\n")
            handle.flush()
        time.sleep(hold)
        with open(log_path, "a", encoding="utf-8") as handle:
            handle.write(f"{name}-end\n")


def test_file_lock_is_process_exclusive(tmp_path):
    lock_path = tmp_path / "shared.lock"
    log_path = tmp_path / "log.txt"
    first = multiprocessing.Process(target=_lock_worker, args=(str(lock_path), str(log_path), "first", 0.4))
    second = multiprocessing.Process(target=_lock_worker, args=(str(lock_path), str(log_path), "second", 0.05))
    first.start()
    deadline = time.time() + 3
    while time.time() < deadline:
        if log_path.exists() and "first-start" in log_path.read_text(encoding="utf-8"):
            break
        time.sleep(0.02)
    second.start()
    first.join(timeout=5)
    second.join(timeout=5)
    assert first.exitcode == 0
    assert second.exitcode == 0
    lines = log_path.read_text(encoding="utf-8").strip().splitlines()
    assert lines == ["first-start", "first-end", "second-start", "second-end"]
