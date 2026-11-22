from __future__ import annotations

from pathlib import Path

import pytest

from kvstore.errors import StoreError
from kvstore.formats import encode_binary, encode_jsonl
from kvstore.model import Record
from kvstore.store import Store


ROOT = Path(__file__).resolve().parents[1]


def test_dual_format_interoperability_from_fixed_samples(tmp_path) -> None:
    binary_store = Store(tmp_path / "binary")
    json_store = Store(tmp_path / "json")
    try:
        binary_report = binary_store.import_data(
            (ROOT / "samples/sample.kvs.bin").read_bytes(), batch_id="binary-full"
        )
        json_report = json_store.import_data(
            (ROOT / "samples/sample.jsonl").read_bytes(), batch_id="json-full"
        )
        assert binary_report["status"] == "applied"
        assert json_report["status"] == "applied"
        binary_keys = {
            key: (ref.version, ref.write_ts_ms, ref.expires_at_ms)
            for key, ref in binary_store.read_view().state.index.items()
        }
        json_keys = {
            key: (ref.version, ref.write_ts_ms, ref.expires_at_ms)
            for key, ref in json_store.read_view().state.index.items()
        }
        assert binary_keys == json_keys
        assert json_report["cumulative_rounding_error_us"] < 0
        decoded = binary_store.export_binary()
        assert len(decoded) > 0
    finally:
        binary_store.close()
        json_store.close()


def test_import_conflict_outcomes_and_atomic_rollback(tmp_path) -> None:
    store = Store(tmp_path)
    try:
        store.put("a", "existing", version=2, write_ts_ms=2_000)
        store.put("b", "untouched", version=1, write_ts_ms=1_000)
        records = [
            Record("a", b"different", 2, 2_000, None),
            Record("new", b"new-value", 1, 3_000, None),
        ]
        with pytest.raises(StoreError) as caught:
            store.import_data(encode_jsonl(records), batch_id="ambiguous")
        assert caught.value.code == "E_IMPORT_REJECTED"
        report = caught.value.context["report"]
        assert report["summary"]["reject"] == 1
        assert report["summary"]["applied_records"] == 0
        assert store.get("a")["value"] == "existing"
        assert store.get("b")["value"] == "untouched"
        with pytest.raises(StoreError):
            store.get("new")
    finally:
        store.close()


def test_import_higher_lower_and_newer_tiebreak(tmp_path) -> None:
    store = Store(tmp_path)
    try:
        store.put("a", "v2", version=2, write_ts_ms=2_000)
        records = [
            Record("a", b"v3", 3, 1_000, None),
            Record("b", b"old", 1, 1_000, None),
            Record("b", b"newer", 1, 2_000, None),
        ]
        report = store.import_data(encode_jsonl(records), batch_id="rules")
        outcomes = [(item["key"], item["outcome"], item["reason"]) for item in report["records"]]
        assert ("a", "overwrite", "higher_version") in outcomes
        assert ("b", "skip", "file_duplicate_lower_priority") in outcomes
        assert ("b", "overwrite", "new_key") in outcomes
        assert store.get("a")["value"] == "v3"
        assert store.get("b")["value"] == "newer"
        assert store.get("b")["version"] == 1
    finally:
        store.close()


def test_import_corruption_has_no_partial_effect(tmp_path) -> None:
    store = Store(tmp_path)
    try:
        with pytest.raises(StoreError) as caught:
            store.import_data(
                (ROOT / "samples/sample_corrupt.kvs.bin").read_bytes(),
                batch_id="corrupt",
            )
        assert caught.value.code == "E_RECORD_CORRUPT"
        assert caught.value.context["record_index"] == 4
        assert store.stats()["state"]["key_count"] == 0
        assert store.stats()["state"]["segment_count"] == 0
    finally:
        store.close()


def test_stale_base_and_batch_reuse_are_rejected(tmp_path) -> None:
    store = Store(tmp_path)
    try:
        store.put("a", "one", write_ts_ms=1_000)
        stale_version = store.stats()["state"]["state_version"] - 1
        with pytest.raises(StoreError) as caught:
            store.import_data(
                encode_jsonl([Record("b", b"x", 1, 1_100, None)]),
                batch_id="stale",
                base_state_version=stale_version,
            )
        assert caught.value.code == "E_STALE_BASE"
        report = store.import_data(
            encode_jsonl([Record("b", b"x", 1, 1_100, None)]), batch_id="once"
        )
        assert report["status"] == "applied"
        with pytest.raises(StoreError) as caught:
            store.import_data(
                encode_jsonl([Record("c", b"y", 1, 1_200, None)]), batch_id="once"
            )
        assert caught.value.code == "E_CONFLICTING_BATCH"
    finally:
        store.close()

