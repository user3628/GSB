from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from chunk_engine.codec import decode_full_block
from chunk_engine.errors import BusinessError, E_BLOCK_CORRUPT, E_SNAPSHOT_INVALID
from chunk_engine.snapshot import SnapshotManager, load_snapshot


def test_three_encodings_nulls_and_row_offsets(sample_suite: dict) -> None:
    snapshot = load_snapshot(sample_suite["v1_small"]["root"])
    expected_encodings = {"id": "fixed", "region": "dict", "shard": "run", "score": "fixed", "label": "dict"}
    decoded = {
        name: decode_full_block(snapshot.block_path(snapshot.columns[name].blocks[0]), snapshot.columns[name].blocks[0])
        for name in snapshot.columns
    }
    assert {name: block.encoding for name, block in decoded.items()} == expected_encodings
    assert decoded["id"].values[:3] == [1, 2, 3]
    assert decoded["id"].row_offsets[:3] == [0, 1, 2]
    assert decoded["score"].null_mask[88] is True and decoded["score"].values[88] is None
    assert decoded["region"].null_mask[96] is True and decoded["region"].values[96] is None
    assert decoded["label"].null_mask[112] is True and decoded["label"].values[112] is None
    assert all(len(block.values) == len(block.null_mask) == len(block.row_offsets) for block in decoded.values())
    assert decoded["score"].dtype == "float64"
    assert decoded["region"].null_bitmap_bytes


def test_corrupt_block_returns_block_number(sample_suite: dict) -> None:
    snapshot = load_snapshot(sample_suite["v3_corrupt"]["root"])
    block = snapshot.column_block("label", 1)
    with pytest.raises(BusinessError) as raised:
        decode_full_block(snapshot.block_path(block), block)
    assert raised.value.code == E_BLOCK_CORRUPT
    assert raised.value.context["block_no"] == 1
    assert raised.value.context["column"] == "label"
    assert "checksum" in raised.value.context["reason"]


def test_snapshot_digest_failure_keeps_old_active(
    tmp_path: Path, sample_suite: dict
) -> None:
    manager = SnapshotManager()
    first = manager.load(sample_suite["v1_small"]["root"])
    bad_root = tmp_path / "bad-snapshot"
    shutil.copytree(sample_suite["v1_small"]["root"], bad_root)
    manifest_path = bad_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["generated_at"] = "2026-02-01T00:00:00Z"
    manifest_path.write_text(json.dumps(manifest, separators=(",", ":")), encoding="utf-8")
    with pytest.raises(BusinessError) as raised:
        manager.load(bad_root)
    assert raised.value.code == E_SNAPSHOT_INVALID
    assert manager.active().digest == first.digest
    assert manager.active().version == first.version


def test_version_must_increase_and_failure_is_atomic(sample_suite: dict) -> None:
    manager = SnapshotManager()
    manager.load(sample_suite["v1_small"]["root"])
    second = manager.load(sample_suite["v2_large"]["root"])
    with pytest.raises(BusinessError) as raised:
        manager.load(sample_suite["v1_small"]["root"])
    assert raised.value.code == E_SNAPSHOT_INVALID
    assert manager.active().version == second.version
    assert manager.active().digest == second.digest
