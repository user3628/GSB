from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from dialogue_engine.checkpoints import CheckpointStore
from dialogue_engine.errors import E_INVALID_STAGE, E_STALE_CHECKPOINT, EngineError
from dialogue_engine.serialization import (
    atomic_write_bytes,
    atomic_write_json,
    digest_json,
    read_json,
)


def _publish_features(
    store: CheckpointStore,
    *,
    shard_id: str,
    fingerprint: str | None = None,
    expected_sequence: int | None = None,
    shard_complete: bool = False,
    shard_order: int = 0,
) -> dict[str, object]:
    fingerprint = fingerprint or digest_json({"probe": shard_id})
    artifact = {"probe": shard_id, "vector": [1.0, 2.0]}
    digest, _ = store.write_artifact(
        shard_id, 0, "features", artifact, input_fingerprint=fingerprint
    )
    return store.publish_checkpoint(
        shard_id=shard_id,
        turn_index=0,
        stage="features",
        work_order=0,
        shard_order=shard_order,
        input_fingerprint=fingerprint,
        config_version="probe-config",
        artifact_digest=digest,
        artifact_summary={"feature_dim": 2},
        shard_complete=shard_complete,
        expected_sequence=expected_sequence,
    )


def test_stale_sequence_and_cross_shard_rejection(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path / "workspace")
    _publish_features(store, shard_id="shard_a", expected_sequence=1)
    with pytest.raises(EngineError) as stale:
        _publish_features(store, shard_id="shard_a", expected_sequence=1)
    assert stale.value.code == E_STALE_CHECKPOINT
    assert stale.value.details["kind"] == "duplicate"

    with pytest.raises(EngineError) as cross:
        _publish_features(store, shard_id="shard_b", expected_sequence=2)
    assert cross.value.code == E_STALE_CHECKPOINT
    assert cross.value.details["kind"] == "cross_shard"


def test_cross_shard_skip_is_rejected(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path / "workspace")
    _publish_features(
        store,
        shard_id="shard_a",
        expected_sequence=1,
        shard_complete=True,
        shard_order=0,
    )
    with pytest.raises(EngineError) as skip:
        _publish_features(
            store,
            shard_id="shard_c",
            expected_sequence=2,
            shard_order=2,
        )
    assert skip.value.code == E_STALE_CHECKPOINT
    assert skip.value.details["kind"] == "cross_shard"


def test_illegal_stage_jump_is_rejected(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path / "workspace")
    with pytest.raises(EngineError) as exc:
        store.publish_checkpoint(
            shard_id="shard_a",
            turn_index=0,
            stage="behavior",
            work_order=2,
            shard_order=0,
            input_fingerprint=digest_json({"probe": "illegal"}),
            config_version="probe-config",
            artifact_digest=digest_json({"artifact": "missing"}),
            artifact_summary={},
            shard_complete=False,
        )
    assert exc.value.code == E_INVALID_STAGE
    assert exc.value.stage == "behavior"
    assert exc.value.details["missing_stage"] == "emotion"


def test_fingerprint_mismatch_and_artifact_tamper_are_distinguished(
    tmp_path: Path,
) -> None:
    store = CheckpointStore(tmp_path / "workspace")
    _publish_features(store, shard_id="shard_a", expected_sequence=1)
    with pytest.raises(EngineError) as mismatch:
        _publish_features(
            store,
            shard_id="shard_a",
            fingerprint=digest_json({"probe": "changed"}),
            expected_sequence=2,
        )
    assert mismatch.value.code == E_STALE_CHECKPOINT
    assert mismatch.value.details["kind"] == "fingerprint_mismatch"

    record = store.scan().records[0]
    artifact_path = store.artifact_path(
        "shard_a", 0, "features", str(record["input_fingerprint"])
    )
    original = artifact_path.read_bytes()
    atomic_write_bytes(artifact_path, b"{\"tampered\":true}\n")
    status = store.status()
    assert not status["healthy"]
    assert any(issue["kind"] == "artifact_tamper" for issue in status["issues"])
    with pytest.raises(EngineError) as tamper:
        store.read_artifact(record)
    assert tamper.value.code == E_STALE_CHECKPOINT
    assert tamper.value.details["kind"] == "tamper"
    atomic_write_bytes(artifact_path, original)


def test_record_hash_tamper_and_recovery(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path / "workspace")
    _publish_features(store, shard_id="shard_a", expected_sequence=1)
    record_path = store.journal_dir / "000000000001.json"
    record = read_json(record_path)
    record["artifact_summary"] = {"feature_dim": 999}
    atomic_write_json(record_path, record)
    status = store.status()
    assert not status["healthy"]
    issue_kinds = {issue["kind"] for issue in status["issues"]}
    assert "record_hash_mismatch" in issue_kinds
    assert "rollback" in issue_kinds
    recovery = store.recover()
    assert recovery["recovered"]
    assert store.scan().sequence == 0


def test_head_rollback_is_reported_and_recovered(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path / "workspace")
    _publish_features(store, shard_id="shard_a", expected_sequence=1)
    head = read_json(store.head_path)
    head["sequence"] = 99
    atomic_write_json(store.head_path, head)
    status = store.status()
    assert any(issue["kind"] == "rollback" for issue in status["issues"])
    store.recover()
    assert store.scan().sequence == 1


def test_duplicate_sequence_is_ignored_then_sequence_continues(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path / "workspace")
    _publish_features(store, shard_id="shard_a", expected_sequence=1)
    feature = store.scan().records[0]
    emotion_fp = digest_json({"emotion": "probe"})
    emotion_artifact = {"label": "平静", "scores": {}}
    emotion_digest, _ = store.write_artifact(
        "shard_a", 0, "emotion", emotion_artifact, input_fingerprint=emotion_fp
    )
    store.publish_checkpoint(
        shard_id="shard_a",
        turn_index=0,
        stage="emotion",
        work_order=1,
        shard_order=0,
        input_fingerprint=emotion_fp,
        config_version="probe-config",
        artifact_digest=emotion_digest,
        artifact_summary={"label": "平静"},
        shard_complete=False,
        expected_sequence=2,
    )
    assert feature["sequence"] == 1
    shutil.copyfile(
        store.journal_dir / "000000000002.json",
        store.journal_dir / "duplicate-sequence.json",
    )
    scan = store.scan()
    assert scan.sequence == 1
    assert any(issue["kind"] == "duplicate_sequence" for issue in scan.issues)
    store.recover()
    assert store.scan().sequence == 1
    store.publish_checkpoint(
        shard_id="shard_a",
        turn_index=0,
        stage="emotion",
        work_order=1,
        shard_order=0,
        input_fingerprint=emotion_fp,
        config_version="probe-config",
        artifact_digest=emotion_digest,
        artifact_summary={"label": "平静"},
        shard_complete=False,
        expected_sequence=2,
    )
    assert store.scan().sequence == 2


def test_two_worker_processes_only_one_publishes(tmp_path: Path) -> None:
    workspace = tmp_path / "concurrent"
    store = CheckpointStore(workspace)
    fingerprint = digest_json({"probe": "concurrent"})
    artifact = {"probe": "concurrent", "vector": [1.0, 2.0]}
    digest, _ = store.write_artifact(
        "probe_shard", 0, "features", artifact, input_fingerprint=fingerprint
    )
    project_root = Path(__file__).resolve().parents[1]
    script = r"""
import json, sys
from dialogue_engine.checkpoints import CheckpointStore
from dialogue_engine.errors import EngineError
workspace, fingerprint, digest = sys.argv[1], sys.argv[2], sys.argv[3]
try:
    record = CheckpointStore(workspace).publish_checkpoint(
        shard_id="probe_shard", turn_index=0, stage="features",
        work_order=0, shard_order=0, input_fingerprint=fingerprint,
        config_version="probe-config", artifact_digest=digest,
        artifact_summary={"feature_dim": 2}, shard_complete=False,
        expected_sequence=1,
    )
    print(json.dumps({"accepted": True, "sequence": record["sequence"]}))
except EngineError as exc:
    print(json.dumps({"accepted": False, "code": exc.code, "kind": exc.details.get("kind")}))
"""
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", script, str(workspace), fingerprint, digest],
            cwd=project_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(2)
    ]
    results = []
    for process in processes:
        stdout, stderr = process.communicate(timeout=30)
        assert process.returncode == 0, stderr
        results.append(json.loads(stdout.strip().splitlines()[-1]))
    accepted = [result for result in results if result["accepted"]]
    rejected = [result for result in results if not result["accepted"]]
    assert len(accepted) == 1
    assert accepted[0]["sequence"] == 1
    assert len(rejected) == 1
    assert rejected[0]["code"] == E_STALE_CHECKPOINT
    assert rejected[0]["kind"] == "duplicate"
    assert store.scan().sequence == 1

