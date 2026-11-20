from __future__ import annotations

import json
from pathlib import Path

import pytest

from kvstore.codec_binary import encode_binary
from kvstore.errors import ConcurrentWrite, FormatInvalid, KeyNotFound, VersionConflict
from kvstore.model import Record
from kvstore.segment import read_header
from kvstore.store import KVStore


def test_write_read_delete_rewrite_and_reopen(tmp_path: Path) -> None:
    store = KVStore(tmp_path / "data")
    assert store.write("k", {"v": 1}, version=1)["version"] == 1
    assert store.get("k")["value"] == {"v": 1}
    deleted = store.delete("k")
    assert deleted["deleted"] is True
    with pytest.raises(KeyNotFound) as missing:
        store.get("k")
    assert missing.value.code == "E_KEY_NOT_FOUND"
    with pytest.raises(KeyNotFound):
        store.delete("k")
    assert store.write("k", 2)["version"] == 3
    assert store.get("k")["value"] == 2

    reopened = KVStore(tmp_path / "data")
    assert reopened.get("k")["value"] == 2
    summary = reopened.index_summary()
    assert summary["indexed_keys"] == 1
    assert summary["live_keys"] == 1
    assert summary["last_txn_seq"] == 3


def test_write_requires_monotonic_version(store: KVStore) -> None:
    store.write("k", 1, version=5)
    with pytest.raises(VersionConflict):
        store.write("k", 2, version=5)


def test_expiry_heap_cleanup_tombstone_and_reuse(tmp_path: Path) -> None:
    now = [4_100_000_000_000_000]
    store = KVStore(tmp_path / "data", clock_us=lambda: now[0])
    store.write("k", 1, version=1, expires_at_us=now[0] + 10_000)
    assert store.get("k")["value"] == 1
    now[0] += 10_001
    with pytest.raises(KeyNotFound) as expired:
        store.get("k")
    assert expired.value.details["expired"] is True
    result = store.expire_due()
    assert result["expired_keys"] == ["k"]
    assert result["expired_count"] == 1
    summary = store.index_summary()
    assert summary["live_keys"] == 0
    assert summary["indexed_keys"] == 1
    assert summary["expiry_heap_entries"] == 0
    assert store.write("k", 2)["version"] == 3
    assert store.get("k")["value"] == 2


def test_ttl_zero_is_immediately_expired(tmp_path: Path) -> None:
    now = [4_100_000_000_000_000]
    store = KVStore(tmp_path / "data", clock_us=lambda: now[0])
    store.write("k", 1, version=1, ttl_ms=0)
    with pytest.raises(KeyNotFound):
        store.get("k")
    assert store.expire_due()["expired_count"] == 1


def test_read_revalidates_record_crc(store: KVStore) -> None:
    store.write("k", {"value": "ok"}, version=1)
    state = store._acquire_state()
    try:
        entry = state.index["k"]
        path = Path(entry.segment_path)
    finally:
        store._release_state(state)
    with path.open("r+b") as handle:
        handle.seek(entry.offset + 6)
        original = handle.read(1)
        handle.seek(entry.offset + 6)
        handle.write(bytes((original[0] ^ 0x01,)))
        handle.flush()
    with pytest.raises(FormatInvalid) as corrupt:
        store.get("k")
    assert corrupt.value.code == "E_FORMAT_INVALID"


def test_binary_import_crc_failure_imports_no_prefix(tmp_path: Path) -> None:
    first = Record("first", 1, 1, 4_100_000_000_000_000, None)
    second = Record("second", 2, 1, 4_100_000_000_100_000, None)
    encoded = bytearray(encode_binary([first, second]))
    first_len = int.from_bytes(encoded[12:16], "little")
    second_offset = 12 + 4 + first_len + 4
    encoded[second_offset + 5] ^= 0xFF
    store = KVStore(tmp_path / "data")
    with pytest.raises(FormatInvalid):
        store.import_data(bytes(encoded), expected_format="compact_binary")
    assert store.index_summary()["live_keys"] == 0
    assert list((tmp_path / "data" / "intents").glob("*.intent")) == []
    with pytest.raises(KeyNotFound):
        store.get("first")


def test_reads_do_not_wait_for_writer_lock(store: KVStore) -> None:
    store.write("visible", 1, version=1)
    store._begin_write()
    try:
        assert store.get("visible")["value"] == 1
        with pytest.raises(ConcurrentWrite):
            store.write("blocked", 2, version=1)
    finally:
        store._end_write()
    store.write("blocked", 2, version=1)
    assert store.get("blocked")["value"] == 2


def test_reader_captured_state_is_pre_write_snapshot(store: KVStore) -> None:
    store.write("before", 1, version=1)
    captured = store._acquire_state()
    try:
        store.write("after", 2, version=1)
        assert "after" not in captured.index
        assert "after" in store._state.index
    finally:
        store._release_state(captured)
    assert store.get("after")["value"] == 2


def test_segment_header_and_meta_maintain_active_pointer(store: KVStore) -> None:
    store.write("a", 1, version=1)
    summary = store.index_summary()
    path = Path(store.data_dir) / summary["segment"]
    header = read_header(path)
    assert header.segment_id == summary["segment_id"]
    meta = json.loads((Path(store.data_dir) / "meta.json").read_text(encoding="utf-8"))
    assert meta["active_segment"] == summary["segment"]
    assert meta["last_txn_seq"] == summary["last_txn_seq"]
