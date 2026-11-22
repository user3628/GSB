import json
import os
import threading

import pytest

from xfpipe.atomic import atomic_write_bytes
from xfpipe.checkpoint import CheckpointStore
from xfpipe.errors import E_INVALID_STAGE, E_STALE_CHECKPOINT, PipelineError


def _store(tmp_path):
    artifacts = str(tmp_path / "artifacts")
    os.makedirs(artifacts, exist_ok=True)
    return CheckpointStore(str(tmp_path / "checkpoints"), artifacts)


def _touch_artifacts(tmp_path, *stages):
    for stage in stages:
        open(os.path.join(str(tmp_path / "artifacts"), f"{stage}.json"), "w").write("{}")


def test_publish_and_continuous_sequence(tmp_path):
    store = _store(tmp_path)
    _touch_artifacts(tmp_path, "infer")
    rec1 = store.publish("infer", "sf", "cv", "dg", expected_seq=store.next_seq())
    assert rec1["seq"] == 1
    _touch_artifacts(tmp_path, "infer", "cross")
    rec2 = store.publish("cross", "sf", "cv", "dg2", expected_seq=store.next_seq())
    assert rec2["seq"] == 2
    assert rec2["prev_digest"] == rec1["record_digest"]
    assert store.classify_snapshot()["status"] == "normal"


def test_stale_or_duplicate_publish_rejected(tmp_path):
    store = _store(tmp_path)
    _touch_artifacts(tmp_path, "infer")
    store.publish("infer", "sf", "cv", "dg", expected_seq=1)
    with pytest.raises(PipelineError) as exc:
        store.publish("infer", "sf", "cv", "dg", expected_seq=1)  # duplicate
    assert exc.value.code == E_STALE_CHECKPOINT
    assert exc.value.details["kind"] == "rollback"


def test_concurrent_publish_only_continuous_advances(tmp_path):
    store = _store(tmp_path)
    _touch_artifacts(tmp_path, "infer")
    results, errors = [], []

    def worker(digest):
        try:
            results.append(store.publish("infer", "sf", "cv", digest, expected_seq=1))
        except PipelineError as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(f"dg{i}",)) for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(results) == 1  # exactly one worker wins the sequence slot
    assert all(e.code == E_STALE_CHECKPOINT for e in errors)
    assert store.classify_snapshot()["status"] == "normal"


def test_missing_upstream_artifact_raises_e_invalid_stage(tmp_path):
    store = _store(tmp_path)
    with pytest.raises(PipelineError) as exc:
        store.publish("reduce", "sf", "cv", "dg", expected_seq=1)
    assert exc.value.code == E_INVALID_STAGE
    assert exc.value.details["missing_upstream"] == "infer"


def test_rollback_detection(tmp_path):
    store = _store(tmp_path)
    _touch_artifacts(tmp_path, "infer")
    store.publish("infer", "sf", "cv", "dg", expected_seq=1)
    # Simulate a wholesale snapshot replacement rolling the pointer back.
    atomic_write_bytes(store._latest_path, json.dumps({"seq": 0, "record_digest": "x" * 64}).encode())
    assert store.classify_snapshot()["status"] == "rollback"
    with pytest.raises(PipelineError) as exc:
        store.verify_or_raise()
    assert exc.value.code == E_STALE_CHECKPOINT
    assert exc.value.details["kind"] == "rollback"


def test_tamper_detection(tmp_path):
    store = _store(tmp_path)
    _touch_artifacts(tmp_path, "infer")
    store.publish("infer", "sf", "cv", "dg", expected_seq=1)
    path = store._record_path(1)
    rec = json.loads(open(path).read())
    rec["artifact_digest"] = "f" * 64  # tamper with the stored record
    open(path, "w").write(json.dumps(rec))
    assert store.classify_snapshot()["status"] == "tamper"
    with pytest.raises(PipelineError) as exc:
        store.verify_or_raise()
    assert exc.value.code == E_STALE_CHECKPOINT
    assert exc.value.details["kind"] == "tamper"


def test_interrupted_write_leaves_no_half_record(tmp_path):
    store = _store(tmp_path)
    _touch_artifacts(tmp_path, "infer")
    store.publish("infer", "sf", "cv", "dg", expected_seq=1)
    # A crash mid-write can only leave a temp file behind, never a partial record.
    open(os.path.join(store.directory, ".tmp-abc.part"), "w").write('{"seq": 2, "sta')
    assert store.classify_snapshot()["status"] == "normal"
    assert store.read_record(2) is None
