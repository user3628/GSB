from __future__ import annotations

import pytest

from kvstore.errors import StoreError
from kvstore.heap import ExpiryIndex
from kvstore.store import Store


def test_indexed_heap_replaces_old_entry_without_zombie() -> None:
    heap = ExpiryIndex()
    heap.upsert("k", 100, 1, "token-v1")
    heap.upsert("k", 50, 2, "token-v2")
    assert len(heap) == 1
    assert heap.peek().token == "token-v2"
    assert heap.token_for("k") == "token-v2"
    assert heap.duplicate_keys() == []
    assert heap.remove("k").token == "token-v2"
    assert len(heap) == 0


def test_write_delete_recreate_invalidates_heap_and_version(tmp_path) -> None:
    now = [10_000]
    store = Store(tmp_path, clock=lambda: now[0])
    try:
        first = store.put("hot", "v1", write_ts_ms=100, expires_at_ms=2_000)
        store.put("hot", "v2", write_ts_ms=200, expires_at_ms=3_000)
        check = store.heap_check()
        assert check["ok"] is True
        assert check["heap_size"] == 1
        assert check["entries"][0]["expires_ms"] == 3_000
        ref_v2 = store.read_view().ref("hot")
        assert ref_v2 is not None and ref_v2.version == 2
        assert ref_v2.token != first["token"]
        store.delete("hot", write_ts_ms=300)
        assert store.heap_check()["heap_size"] == 0
        recreated = store.put("hot", "v3", write_ts_ms=400, expires_at_ms=4_000)
        assert recreated["version"] == 4
        check = store.heap_check()
        assert check["ok"] is True
        assert len(check["entries"]) == 1
        assert check["stale_entries"] == []
    finally:
        store.close()


def test_expire_records_tombstones_and_cleans_exact_due_keys(tmp_path) -> None:
    store = Store(tmp_path, clock=lambda: 10_000)
    try:
        store.put("a", "one", write_ts_ms=100, expires_at_ms=2_000)
        store.put("b", "two", write_ts_ms=101, expires_at_ms=20_000)
        store.put("c", "three", write_ts_ms=102, expires_at_ms=2_000)
        result = store.expire(now_ms=3_000)
        assert result["cleaned_count"] == 2
        assert result["cleaned_keys"] == ["a", "c"]
        assert result["tombstones_recorded"] == 2
        assert store.stats()["state"]["tombstone_count"] == 2
        assert store.get("b")["value"] == "two"
        with pytest.raises(StoreError) as caught:
            store.get("a")
        assert caught.value.code == "E_KEY_NOT_FOUND"
        assert store.heap_check()["ok"] is True
    finally:
        store.close()

