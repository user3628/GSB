from __future__ import annotations

import json
from pathlib import Path

import pytest

from kvstore.errors import ConcurrentWrite, FormatInvalid, KeyNotFound
from kvstore.store import KVStore


def _jsonl(*items: dict) -> bytes:
    return b"\n".join(
        json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        for item in items
    ) + b"\n"


def test_fixed_samples_produce_expected_conflict_report(
    store: KVStore, sample_binary: bytes, sample_jsonl: bytes
) -> None:
    binary_result = store.import_data(sample_binary, expected_format="compact_binary")
    assert binary_result["negotiation"]["checksums_verified"] is True
    assert binary_result["summary"]["committed_records"] == 4

    result = store.import_data(sample_jsonl, expected_format="json_lines")
    summary = result["summary"]
    assert summary["input_records"] == 7
    assert summary["overwrite"] == 3
    assert summary["skip"] == 2
    assert summary["reject"] == 2
    assert summary["committed_records"] == 3
    assert summary["rejected_keys"] == ["dup:key"]
    assert summary["skipped_keys"] == ["config:region", "device:mode"]
    assert result["negotiation"]["expiry_semantics"] == "relative_ttl"
    assert result["negotiation"]["unknown_fields"] == {"source": 1}
    assert result["negotiation"]["records_with_filled_fields"]["2"] == ["expires_at_us"]

    assert store.get("device:threshold")["value"] == 20
    assert store.get("device:threshold")["expires_at_us"] == 4_100_000_600_500_000
    assert store.get("feature:beta")["expires_at_us"] == 4_100_000_904_000_000
    assert store.get("sensor:temp")["value"] == 22.5
    assert store.get("config:region")["value"] == "cn-east"
    with pytest.raises(KeyNotFound):
        store.get("dup:key")


def test_reject_does_not_rollback_other_successful_import_keys(store: KVStore) -> None:
    payload = _jsonl(
        {"key": "accepted", "value": 1, "version": 2, "written_at_us": 4_100_000_000_000_000},
        {"key": "tie", "value": "a", "version": 2, "written_at_us": 4_100_000_001_000_000},
        {"key": "tie", "value": "b", "version": 2, "written_at_us": 4_100_000_001_000_000},
    )
    result = store.import_data(payload, expected_format="json_lines")
    assert result["summary"]["committed_records"] == 1
    assert result["summary"]["rejected_keys"] == ["tie"]
    assert store.get("accepted")["value"] == 1
    with pytest.raises(KeyNotFound):
        store.get("tie")


def test_last_write_time_breaks_equal_version_tie(store: KVStore) -> None:
    store.write("k", "old", version=2, written_at_us=4_100_000_000_000_000)
    newer = _jsonl({"key": "k", "value": "new", "version": 2, "written_at_us": 4_100_000_000_000_001})
    result = store.import_data(newer, expected_format="json_lines")
    assert result["records"][0]["outcome"] == "overwrite"
    assert result["records"][0]["reason"] == "newer_write_time"
    assert store.get("k")["value"] == "new"

    older = _jsonl({"key": "k", "value": "older", "version": 2, "written_at_us": 4_100_000_000_000_000})
    result = store.import_data(older, expected_format="json_lines")
    assert result["records"][0]["outcome"] == "skip"
    assert result["records"][0]["reason"] == "older_write_time"
    assert store.get("k")["value"] == "new"


def test_same_version_same_time_different_value_is_rejected(store: KVStore) -> None:
    written = 4_100_000_000_000_000
    store.write("k", "existing", version=2, written_at_us=written)
    payload = _jsonl({"key": "k", "value": "incoming", "version": 2, "written_at_us": written})
    result = store.import_data(payload, expected_format="json_lines")
    assert result["records"][0]["outcome"] == "reject"
    assert result["summary"]["rejected_keys"] == ["k"]
    assert store.get("k")["value"] == "existing"


def test_duplicate_key_in_same_file_uses_version_then_time(store: KVStore) -> None:
    payload = _jsonl(
        {"key": "k", "value": "v1", "version": 1, "written_at_us": 4_100_000_000_000_000},
        {"key": "k", "value": "v2", "version": 2, "written_at_us": 4_100_000_000_000_000},
    )
    result = store.import_data(payload, expected_format="json_lines")
    assert [item["outcome"] for item in result["records"]] == ["skip", "overwrite"]
    assert store.get("k")["value"] == "v2"


def test_ambiguous_late_json_line_imports_nothing(store: KVStore) -> None:
    payload = _jsonl({"key": "first", "value": 1, "version": 1, "written_at_us": 1}) + b"{bad json}\n"
    with pytest.raises(FormatInvalid):
        store.import_data(payload, expected_format="json_lines")
    assert store.index_summary()["live_keys"] == 0


def test_same_batch_id_has_only_one_success(store: KVStore) -> None:
    payload = _jsonl({"key": "batch", "value": 1, "version": 1, "written_at_us": 4_100_000_000_000_000})
    first = store.import_data(payload, expected_format="json_lines", batch_id="fixed-batch")
    assert first["summary"]["committed_records"] == 1
    with pytest.raises(ConcurrentWrite) as conflict:
        store.import_data(payload, expected_format="json_lines", batch_id="fixed-batch")
    assert conflict.value.code == "E_CONCURRENT_WRITE"
