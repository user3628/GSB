from __future__ import annotations

import json

import pytest

from qoplib.checkpoint import (
    CheckpointStore,
    deterministic_sequence,
    build_record,
)
from qoplib.errors import (
    E_INVALID_STAGE,
    E_STALE_CHECKPOINT,
    InvalidStageError,
    StaleCheckpointError,
)


def _record(sequence, shard_id="shard_00", shard_index=0, stage="validate"):
    return build_record(
        sequence, shard_id, shard_index, stage, "in_fp", 1,
        f"fp-{sequence}", {"k": sequence}, "run_fp")


def test_deterministic_sequence_ordering():
    assert deterministic_sequence(0, "validate") == 0
    assert deterministic_sequence(0, "verify") == 3
    assert deterministic_sequence(1, "validate") == 4
    assert deterministic_sequence(3, "verify") == 15


def test_commit_requires_contiguous_monotonic_sequence(tmp_path):
    store = CheckpointStore(tmp_path)
    store.reserve("shard_00", 0, "validate")
    store.commit(_record(0))
    with pytest.raises(StaleCheckpointError) as exc:
        store.commit(_record(5, stage="quantize"))
    assert exc.value.code == E_STALE_CHECKPOINT
    assert exc.value.details["required_sequence"] == 1


def test_duplicate_publish_is_rejected(tmp_path):
    store = CheckpointStore(tmp_path)
    store.commit(_record(0))
    with pytest.raises(StaleCheckpointError) as exc:
        store.commit(_record(0))
    assert exc.value.code == E_STALE_CHECKPOINT


def test_wrong_sequence_for_shard_stage_is_stale(tmp_path):
    store = CheckpointStore(tmp_path)
    store.commit(_record(0))
    # shard_00 quantize must be sequence 1, deliberately send 2.
    with pytest.raises(StaleCheckpointError) as exc:
        store.commit(build_record(
            2, "shard_00", 0, "quantize", "in_fp", 1, "fp", {}, "run_fp"))
    assert exc.value.code == E_STALE_CHECKPOINT
    assert exc.value.details["expected_sequence"] == 1


def test_unknown_stage_raises_invalid_stage(tmp_path):
    store = CheckpointStore(tmp_path)
    with pytest.raises(InvalidStageError) as exc:
        store.stage_dir("shard_00", "no_such_stage")
    assert exc.value.code == E_INVALID_STAGE


def test_records_are_atomic_and_recoverable(tmp_path):
    store = CheckpointStore(tmp_path)
    store.commit(_record(0))
    target = store.stage_record_path("shard_00", "validate")
    payload = json.loads(target.read_text())
    assert payload["sequence"] == 0
    assert store.latest_sequence() == 0
    assert store.next_sequence() == 1
    # No tmp leftovers after a clean commit.
    leftovers = list(tmp_path.rglob("*.tmp"))
    assert leftovers == []


def test_full_four_stage_sequence_per_shard(tmp_path):
    store = CheckpointStore(tmp_path)
    stages = ("validate", "quantize", "linear", "verify")
    for idx, stage in enumerate(stages):
        store.reserve("shard_00", 0, stage)
        store.commit(build_record(
            idx, "shard_00", 0, stage, "in_fp", 1, f"fp{idx}", {}, "run_fp"))
    counter = json.loads((tmp_path / "counter.json").read_text())
    assert [item["sequence"] for item in counter["committed"]] == [0, 1, 2, 3]
