"""Requirement 4 & 6: atomic checkpoints, resume, publish guards."""
from __future__ import annotations

import json

import pytest

from depworkbench.checkpoint import CheckpointRecord, CheckpointStore
from depworkbench.errors import (E_CORRUPT_CHECKPOINT, E_INVALID_STAGE,
                                 E_STALE_CHECKPOINT, WorkbenchError)


def _record(index: int) -> CheckpointRecord:
    return CheckpointRecord.create(
        index, index, f"in{index}", "model", "cfg", {"tokens": index + 1})


def test_publish_requires_contiguous_indices(tmp_path):
    store = CheckpointStore(tmp_path / "cp")
    store.publish(_record(0))
    with pytest.raises(WorkbenchError) as exc:
        store.publish(_record(2))
    assert exc.value.code == E_STALE_CHECKPOINT


def test_duplicate_publish_is_stale(tmp_path):
    store = CheckpointStore(tmp_path / "cp")
    store.publish(_record(0))
    with pytest.raises(WorkbenchError) as exc:
        store.publish(_record(0))
    assert exc.value.code == E_STALE_CHECKPOINT


def test_checksum_roundtrip(tmp_path):
    store = CheckpointStore(tmp_path / "cp")
    store.publish(_record(0))
    loaded = store.load(0)
    assert loaded is not None
    assert loaded.input_fingerprint == "in0"
    assert loaded.checksum


def test_corrupted_record_is_ignored_and_prefix_preserved(tmp_path):
    store = CheckpointStore(tmp_path / "cp")
    store.publish(_record(0))
    store.publish(_record(1))
    target = store._shard_path(1)
    envelope = json.loads(target.read_text(encoding="utf-8"))
    envelope["record"]["result_summary"]["tokens"] = 999
    target.write_text(json.dumps(envelope), encoding="utf-8")
    records, warnings = store.load_consistent_prefix()
    assert [r.shard_index for r in records] == [0]
    assert warnings
    assert warnings[0].code == E_CORRUPT_CHECKPOINT


def test_truncated_temporary_file_leaves_no_half_record(tmp_path):
    store = CheckpointStore(tmp_path / "cp")
    store.publish(_record(0))
    leftovers = list(store.root.glob("*.tmp"))
    assert leftovers == []


def test_require_prefix_rejects_missing_predecessor(tmp_path):
    store = CheckpointStore(tmp_path / "cp")
    store.publish(_record(0))
    with pytest.raises(WorkbenchError) as exc:
        store.require_prefix(2)
    assert exc.value.code == E_INVALID_STAGE
