import json
import os
import threading

import pytest

from evalbench.checkpoint import CheckpointStore
from evalbench.errors import InvalidStageError, StaleCheckpointError

PUB = dict(record_fingerprint="r", config_fingerprint="c", artifact={"x": 1})


def test_sequential_publish_and_status(tmp_path):
    store = CheckpointStore(tmp_path / "ws")
    store.publish("s1", "parse", **PUB)
    store.publish("s1", "calibrate", **PUB)
    store.publish("s1", "diagnose", **PUB)
    status = store.status()
    assert status["s1"]["latest_seq"] == 3
    assert status["s1"]["stages"]["diagnose"]["artifact_digest"]


def test_duplicate_publish_rejected(tmp_path):
    store = CheckpointStore(tmp_path / "ws")
    store.publish("s1", "parse", **PUB)
    with pytest.raises(StaleCheckpointError) as exc:
        store.publish("s1", "parse", **PUB)
    assert exc.value.code == "E_STALE_CHECKPOINT"


def test_stage_jump_rejected(tmp_path):
    store = CheckpointStore(tmp_path / "ws")
    with pytest.raises(InvalidStageError) as exc:
        store.publish("s1", "calibrate", **PUB)
    assert exc.value.code == "E_INVALID_STAGE"


def test_cross_shard_body_rejected(tmp_path):
    store = CheckpointStore(tmp_path / "ws")
    rec = {"shard_id": "other", "stage": "parse", "seq": 1,
           "record_fingerprint": "r", "config_fingerprint": "c", "artifact": {}}
    with pytest.raises(StaleCheckpointError):
        store.publish_record(rec, target_shard_id="s1")


def test_atomic_write_leaves_no_partial_file(tmp_path):
    store = CheckpointStore(tmp_path / "ws")
    store.publish("s1", "parse", **PUB)
    shard_dir = tmp_path / "ws" / "checkpoints" / "s1"
    leftovers = [p for p in shard_dir.iterdir() if ".tmp" in p.name]
    assert leftovers == []
    # simulate a crash mid-write: a stray tmp file must be ignored by readers
    (shard_dir / "001_parse.json.tmp.999").write_text('{"half":')
    assert store.latest_seq("s1") == 1
    assert store.get("s1", "parse")["artifact"] == {"x": 1}


def test_concurrent_publishers_keep_sequence_consistent(tmp_path):
    ws = tmp_path / "ws"
    errors = []
    ok = []

    def worker(shard):
        store = CheckpointStore(ws)
        try:
            store.publish(shard, "parse", **PUB)
            ok.append(shard)
        except StaleCheckpointError:
            errors.append(shard)

    threads = [threading.Thread(target=worker, args=(f"s{i}",)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(ok) == 8  # distinct shards all publish fine

    # same-shard concurrent duplicate: exactly one wins
    errors.clear()
    wins = []

    def dup_worker():
        store = CheckpointStore(ws)
        try:
            store.publish("shared", "parse", **PUB)
            wins.append(1)
        except StaleCheckpointError:
            errors.append(1)

    threads = [threading.Thread(target=dup_worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(wins) == 1
    assert len(errors) == 5
    assert CheckpointStore(ws).latest_seq("shared") == 1


def test_invalidate_from_removes_downstream(tmp_path):
    store = CheckpointStore(tmp_path / "ws")
    for stage in ("parse", "calibrate", "diagnose"):
        store.publish("s1", stage, **PUB)
    removed = store.invalidate_from("s1", "calibrate")
    assert removed == ["calibrate", "diagnose"]
    assert store.latest_seq("s1") == 1
    store.publish("s1", "calibrate", **PUB)  # consecutive again
    assert store.latest_seq("s1") == 2
