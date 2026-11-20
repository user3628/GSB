from __future__ import annotations

import json
from pathlib import Path

import pytest

from sampling_engine.checkpoints import RequestStore
from sampling_engine.errors import (
    EngineError,
    E_INVALID_STAGE,
    E_STALE_CHECKPOINT,
)


def _record(request_id: str, stage: str, sequence: int) -> dict:
    return {
        "request_id": request_id,
        "stage": stage,
        "sequence": sequence,
        "input_fingerprint": f"input-{stage}",
        "config_version": "config-v1",
        "artifact_digest": f"logical-{stage}",
        "algorithm_version": f"algo-{stage}",
        "stage_metadata": {"stage": stage},
    }


def _artifact(tmp_path: Path, name: str, content: bytes = b"artifact") -> Path:
    path = tmp_path / "staging" / f"{name}.bin"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def test_publish_enforces_predecessor_sequence_and_duplicate_rules(tmp_path):
    store = RequestStore(tmp_path / "workspace", "req_a", lock_timeout=5.0)
    with pytest.raises(EngineError) as info:
        store.publish_checkpoint(
            _record("req_a", "normalize", 1), _artifact(tmp_path, "n0")
        )
    assert info.value.code == E_INVALID_STAGE
    assert info.value.context["missing_stage"] == "postprocess"

    with pytest.raises(EngineError) as info:
        store.publish_checkpoint(
            _record("req_a", "postprocess", 2), _artifact(tmp_path, "p2")
        )
    assert info.value.code == E_STALE_CHECKPOINT

    checkpoint = store.publish_checkpoint(
        _record("req_a", "postprocess", 1), _artifact(tmp_path, "p1")
    )
    assert checkpoint["sequence"] == 1
    assert store.next_sequence() == 2

    with pytest.raises(EngineError) as info:
        store.publish_checkpoint(
            _record("req_a", "postprocess", 2), _artifact(tmp_path, "pdup")
        )
    assert info.value.code == E_STALE_CHECKPOINT

    with pytest.raises(EngineError) as info:
        store.publish_checkpoint(
            _record("req_a", "normalize", 3), _artifact(tmp_path, "n3")
        )
    assert info.value.code == E_STALE_CHECKPOINT

    normalize = store.publish_checkpoint(
        _record("req_a", "normalize", 2), _artifact(tmp_path, "n2")
    )
    assert normalize["sequence"] == 2
    active = store.load_active_checkpoints()
    assert set(active) == {"postprocess", "normalize"}
    assert store.history_summary()["contiguous"] is True


def test_cross_request_publication_is_stale(tmp_path):
    store_a = RequestStore(tmp_path / "workspace", "req_a", lock_timeout=5.0)
    store_b = RequestStore(tmp_path / "workspace", "req_b", lock_timeout=5.0)
    store_a.publish_checkpoint(
        _record("req_a", "postprocess", 1), _artifact(tmp_path, "a1")
    )
    with pytest.raises(EngineError) as info:
        store_b.publish_checkpoint(
            _record("req_a", "postprocess", 1), _artifact(tmp_path, "wrong")
        )
    assert info.value.code == E_STALE_CHECKPOINT


def test_interrupted_staging_write_leaves_no_checkpoint_record(tmp_path):
    workspace = tmp_path / "workspace"
    store = RequestStore(workspace, "req_a", lock_timeout=5.0)
    staging = store.staging_dir / "postprocess.incomplete"
    staging.write_bytes(b"partial")
    assert store.next_sequence() == 1
    assert list(store.checkpoints_dir.glob("*.json")) == []
    store.publish_checkpoint(
        _record("req_a", "postprocess", 1), _artifact(tmp_path, "complete")
    )
    record = json.loads(next(store.checkpoints_dir.glob("*.json")).read_text())
    assert record["sequence"] == 1
    assert record["request_id"] == "req_a"
    assert record["artifact_digest"] == "logical-postprocess"
