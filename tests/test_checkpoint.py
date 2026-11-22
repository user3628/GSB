"""Checkpoint store: atomicity, sequence protection, replacement detection."""
import json
import multiprocessing

import pytest

from sft_workbench.checkpoint import (
    STAGES,
    CheckpointRecord,
    CheckpointStore,
    compute_version_fingerprint,
)
from sft_workbench.errors import E_STALE_CHECKPOINT, WorkbenchError
from sft_workbench.util import digest_obj


def make_record(store, stage, seq, prev, artifacts=None, config="cfg"):
    artifacts = artifacts if artifacts is not None else {}
    stage_digests = {r.stage: r.artifact_digest for r in store.list_records()}
    artifact_digest = digest_obj(artifacts)
    stage_digests[stage] = artifact_digest
    return CheckpointRecord(
        stage=stage,
        seq=seq,
        input_fingerprint="in",
        config_fingerprint=config,
        artifacts=artifacts,
        artifact_digest=artifact_digest,
        version_fingerprint=compute_version_fingerprint(stage_digests, config, prev),
        prev_fingerprint=prev,
    )


def test_publish_and_verify_normal(tmp_path):
    store = CheckpointStore(tmp_path / "ws")
    head = ""
    for idx, stage in enumerate(STAGES[:2], start=1):
        record = make_record(store, stage, idx, head)
        store.publish(record)
        head = record.version_fingerprint
    result = store.verify()
    assert result.kind == "normal"
    assert [r.seq for r in store.list_records()] == [1, 2]


def test_publish_rejects_skipped_sequence(tmp_path):
    store = CheckpointStore(tmp_path / "ws")
    record = make_record(store, "dedup", 2, "")
    with pytest.raises(WorkbenchError) as excinfo:
        store.publish(record)
    assert excinfo.value.code == E_STALE_CHECKPOINT
    assert excinfo.value.details["expected_seq"] == 1


def test_publish_rejects_out_of_order_stage(tmp_path):
    store = CheckpointStore(tmp_path / "ws")
    record = make_record(store, "score", 1, "")
    with pytest.raises(WorkbenchError) as excinfo:
        store.publish(record)
    assert excinfo.value.code == E_STALE_CHECKPOINT
    assert excinfo.value.details["expected_stage"] == "import"


def test_publish_rejects_broken_chain(tmp_path):
    store = CheckpointStore(tmp_path / "ws")
    first = make_record(store, "import", 1, "")
    store.publish(first)
    stale = make_record(store, "dedup", 2, "not-the-head")
    with pytest.raises(WorkbenchError) as excinfo:
        store.publish(stale)
    err = excinfo.value
    assert err.code == E_STALE_CHECKPOINT
    assert err.details["expected_fingerprint"] == first.version_fingerprint
    assert err.details["actual_fingerprint"] == "not-the-head"


def test_publish_rejects_duplicate_sequence(tmp_path):
    store = CheckpointStore(tmp_path / "ws")
    first = make_record(store, "import", 1, "")
    store.publish(first)
    again = make_record(store, "import", 1, "")
    with pytest.raises(WorkbenchError) as excinfo:
        store.publish(again)
    assert excinfo.value.code == E_STALE_CHECKPOINT


def test_atomic_write_leaves_no_partial_files(tmp_path):
    store = CheckpointStore(tmp_path / "ws")
    record = make_record(store, "import", 1, "")
    store.publish(record)
    leftovers = list((tmp_path / "ws").rglob("*.tmp-*"))
    assert leftovers == []
    loaded = json.loads((tmp_path / "ws/checkpoints/000001-import.json").read_text())
    assert loaded["stage"] == "import"


def test_verify_detects_tampered_artifact(tmp_path):
    store = CheckpointStore(tmp_path / "ws")
    artifact = store.artifacts_dir / "kept.jsonl"
    artifact.write_text("hello\n")
    record = make_record(store, "import", 1, "", artifacts={"kept.jsonl": __import__("hashlib").sha256(b"hello\n").hexdigest()})
    store.publish(record)
    assert store.verify().kind == "normal"
    artifact.write_text("hello tampered\n")
    result = store.verify()
    assert result.kind == "tamper"
    with pytest.raises(WorkbenchError) as excinfo:
        store.verify_or_raise()
    assert excinfo.value.code == E_STALE_CHECKPOINT


def test_verify_detects_rollback(tmp_path):
    store = CheckpointStore(tmp_path / "ws")
    head = ""
    for idx, stage in enumerate(STAGES[:3], start=1):
        record = make_record(store, stage, idx, head)
        store.publish(record)
        head = record.version_fingerprint
    # Roll the head pointer back to seq 1 while the journal still shows seq 3.
    first = store.list_records()[0]
    (tmp_path / "ws/HEAD.json").write_text(json.dumps({
        "seq": 1,
        "stage": "import",
        "version_fingerprint": first.version_fingerprint,
        "artifact_digest": first.artifact_digest,
    }))
    result = store.verify()
    assert result.kind == "rollback"
    assert result.expected_fingerprint != result.actual_fingerprint


def _attempt(workspace, seq, stage, prev, queue):
    store = CheckpointStore(workspace)
    record = make_record(store, stage, seq, prev)
    try:
        store.publish(record)
        queue.put("ok")
    except WorkbenchError as exc:
        queue.put(exc.code)


def test_concurrent_publish_only_one_wins(tmp_path):
    workspace = str(tmp_path / "ws")
    CheckpointStore(workspace)  # initialize dirs
    ctx = multiprocessing.get_context("fork")
    queue = ctx.Queue()
    procs = [ctx.Process(target=_attempt, args=(workspace, 1, "import", "", queue)) for _ in range(4)]
    for proc in procs:
        proc.start()
    for proc in procs:
        proc.join()
    outcomes = sorted(queue.get() for _ in procs)
    assert outcomes.count("ok") == 1
    assert outcomes.count(E_STALE_CHECKPOINT) == 3


def test_invalidate_from_drops_downstream(tmp_path):
    store = CheckpointStore(tmp_path / "ws")
    head = ""
    for idx, stage in enumerate(STAGES[:4], start=1):
        record = make_record(store, stage, idx, head)
        store.publish(record)
        head = record.version_fingerprint
    removed = store.invalidate_from("dedup")
    assert removed == ["dedup", "score", "sample"]
    assert [r.stage for r in store.list_records()] == ["import"]
    assert store.head()["seq"] == 1
    assert store.verify().kind == "normal"
