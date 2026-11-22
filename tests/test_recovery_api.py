from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import threading
from pathlib import Path

import pytest
import requests

from kvstore.api import make_server
from kvstore.errors import StoreError
from kvstore.formats import decode_binary, encode_binary, encode_jsonl
from kvstore.model import Record
from kvstore.store import Store


ROOT = Path(__file__).resolve().parents[1]


def _run_crash_worker(data_dir: Path, crash_point: str, *args: str) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT)
    return subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/crash_worker.py"),
            str(data_dir),
            "--crash-point",
            crash_point,
            *args,
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
    )


def test_sigkill_after_durable_segment_recovers_complete_batch(tmp_path) -> None:
    store = Store(tmp_path)
    store.put("base", "ok", write_ts_ms=1_000)
    store.close()
    sample = tmp_path / "crash-import.jsonl"
    sample.write_bytes(
        encode_jsonl(
            [
                Record("crash-a", b"one", 1, 2_000, None),
                Record("crash-b", b"two", 1, 2_001, None),
            ]
        )
    )
    completed = _run_crash_worker(
        tmp_path,
        "after_segment",
        "--import-file",
        str(sample),
        "--batch-id",
        "crash-after-segment",
    )
    assert completed.returncode in (-9, 137)
    reopened = Store(tmp_path)
    try:
        assert reopened.get("crash-a")["value"] == "one"
        assert reopened.get("crash-b")["value"] == "two"
        report = reopened.recovery_report()["report"]
        assert report["facts_only"] is True
        assert report["process_exit_report_available"] is False
        assert report["recovered_count"] == 1
        assert report["intents"][0]["action"] == "recovered_from_complete_segment"
    finally:
        reopened.close()


def test_sigkill_after_intent_and_before_segment_rolls_back(tmp_path) -> None:
    sample = tmp_path / "rollback.jsonl"
    sample.write_bytes(encode_jsonl([Record("never", b"x", 1, 2_000, None)]))
    completed = _run_crash_worker(
        tmp_path,
        "after_intent",
        "--import-file",
        str(sample),
        "--batch-id",
        "crash-before-segment",
    )
    assert completed.returncode in (-9, 137)
    reopened = Store(tmp_path)
    try:
        with pytest.raises(StoreError):
            reopened.get("never")
        report = reopened.recovery_report()["report"]
        assert report["rolled_back_count"] == 1
        assert report["intents"][0]["action"] == "rolled_back_no_complete_segment"
    finally:
        reopened.close()


def test_sigkill_during_snapshot_switch_recovers_durable_snapshot(tmp_path) -> None:
    baseline = Store(tmp_path)
    baseline.put("old", "before-snapshot", write_ts_ms=1_000)
    baseline.close()
    completed = _run_crash_worker(
        tmp_path,
        "after_segment",
        "--snapshot-file",
        str(ROOT / "samples/sample_snapshot.json"),
        "--batch-id",
        "snapshot-crash",
    )
    assert completed.returncode in (-9, 137)
    reopened = Store(tmp_path)
    try:
        assert reopened.stats()["state"]["last_snapshot_seq"] == 7
        assert reopened.get("alpha")["value"] == "domain-alpha-v1"
        with pytest.raises(StoreError):
            reopened.get("old")
        report = reopened.recovery_report()["report"]
        assert report["recovered_count"] == 1
        assert report["intents"][0]["kind"] == "snapshot"
        assert report["intents"][0]["action"] == "recovered_from_complete_segment"
    finally:
        reopened.close()


def test_read_view_is_isolated_during_inflight_write(tmp_path) -> None:
    store = Store(tmp_path)
    original_write = store.segments.write_segment
    entered = threading.Event()
    release = threading.Event()
    failure: list[BaseException] = []

    def delayed_write(*args, **kwargs):
        entered.set()
        if not release.wait(5):
            raise RuntimeError("test did not release writer")
        return original_write(*args, **kwargs)

    store.put("key", "old", write_ts_ms=1_000)
    store.segments.write_segment = delayed_write  # type: ignore[method-assign]

    def writer() -> None:
        try:
            store.put("key", "new", write_ts_ms=2_000)
        except BaseException as exc:  # pragma: no cover - surfaced below
            failure.append(exc)

    thread = threading.Thread(target=writer)
    thread.start()
    assert entered.wait(5)
    # The write has a durable SQLite intent and is currently creating a segment,
    # but the published state has not changed. A reader sees the old snapshot.
    assert store.get("key")["value"] == "old"
    release.set()
    thread.join(5)
    assert not thread.is_alive()
    assert not failure
    assert store.get("key")["value"] == "new"
    store.close()


def test_concurrent_import_gets_conflicting_batch(tmp_path) -> None:
    store = Store(tmp_path)
    original_plan = store._plan_import
    entered = threading.Event()
    release = threading.Event()
    results: list[object] = []

    def delayed_plan(*args, **kwargs):
        entered.set()
        if not release.wait(5):
            raise RuntimeError("test did not release import")
        return original_plan(*args, **kwargs)

    store._plan_import = delayed_plan  # type: ignore[method-assign]

    def first_import() -> None:
        try:
            results.append(
                store.import_data(
                    encode_jsonl([Record("a", b"one", 1, 1_000, None)]),
                    batch_id="first",
                )
            )
        except BaseException as exc:  # pragma: no cover
            results.append(exc)

    thread = threading.Thread(target=first_import)
    thread.start()
    assert entered.wait(5)
    with pytest.raises(StoreError) as caught:
        store.import_data(
            encode_jsonl([Record("b", b"two", 1, 1_100, None)]),
            batch_id="second",
        )
    assert caught.value.code == "E_CONFLICTING_BATCH"
    release.set()
    thread.join(5)
    assert not thread.is_alive()
    assert isinstance(results[0], dict)
    store.close()


@pytest.fixture()
def http_server(tmp_path):
    store = Store(tmp_path / "http")
    server = make_server(store, "127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    try:
        yield store, f"http://{host}:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)
        store.close()


def test_http_api_end_to_end_and_error_codes(http_server) -> None:
    store, base = http_server
    put = requests.put(
        f"{base}/keys/api-key",
        json={
            "value": "api-value",
            "version": 1,
            "write_timestamp_ms": 1_900_000_000_000,
            "expires_at_ms": 1_900_000_100_000,
        },
        timeout=5,
    )
    assert put.status_code == 200
    assert put.json()["record"]["version"] == 1
    get = requests.get(f"{base}/keys/api-key", timeout=5)
    assert get.status_code == 200
    assert get.json()["record"]["value"] == "api-value"

    binary_export = requests.get(f"{base}/export/binary", timeout=5)
    assert binary_export.status_code == 200
    assert decode_binary(binary_export.content)[0].record.key == "api-key"
    jsonl_export = requests.get(f"{base}/export/jsonl?shards=2", timeout=5)
    assert jsonl_export.status_code == 200
    assert jsonl_export.headers["X-KV-Shard-Count"] == "2"

    raw_jsonl = encode_jsonl([Record("imported", b"yes", 1, 1_900_000_000_100, None)])
    imported = requests.post(
        f"{base}/import",
        params={"batch_id": "api-import", "format": "jsonl"},
        data=raw_jsonl,
        headers={"Content-Type": "application/octet-stream"},
        timeout=5,
    )
    assert imported.status_code == 200
    assert imported.json()["report"]["status"] == "applied"
    report = requests.get(f"{base}/imports/api-import", timeout=5)
    assert report.status_code == 200

    corrupt = requests.post(
        f"{base}/import",
        params={"batch_id": "api-corrupt"},
        data=(ROOT / "samples/sample_corrupt.kvs.bin").read_bytes(),
        headers={"Content-Type": "application/octet-stream"},
        timeout=5,
    )
    assert corrupt.status_code == 422
    assert corrupt.json()["code"] == "E_RECORD_CORRUPT"
    assert corrupt.json()["context"]["record_index"] == 4

    stale = requests.put(
        f"{base}/keys/api-key",
        json={"value": "stale", "base_state_version": 0},
        timeout=5,
    )
    assert stale.status_code == 409
    assert stale.json()["code"] == "E_STALE_BASE"

    snapshot_response = requests.get(f"{base}/snapshot/export", timeout=5)
    snapshot = snapshot_response.json()["snapshot"]
    snapshot["records"][0]["value"] = "bad"
    invalid = requests.post(f"{base}/snapshot/switch", json=snapshot, timeout=5)
    assert invalid.status_code == 422
    assert invalid.json()["code"] == "E_SNAPSHOT_INVALID"

    valid_snapshot = requests.get(f"{base}/snapshot/export", timeout=5).json()["snapshot"]
    switched = requests.post(f"{base}/snapshot/switch", json=valid_snapshot, timeout=5)
    assert switched.status_code == 200
    stale_snapshot = requests.post(f"{base}/snapshot/switch", json=valid_snapshot, timeout=5)
    assert stale_snapshot.status_code == 409
    assert stale_snapshot.json()["code"] == "E_SNAPSHOT_STALE"

    recovery = requests.get(f"{base}/recovery/report", timeout=5)
    assert recovery.status_code == 200
    assert recovery.json()["report"]["facts_only"] is True

    duplicate = requests.post(
        f"{base}/import",
        params={"batch_id": "api-import", "format": "jsonl"},
        data=raw_jsonl,
        headers={"Content-Type": "application/octet-stream"},
        timeout=5,
    )
    assert duplicate.status_code == 409
    assert duplicate.json()["code"] == "E_CONFLICTING_BATCH"


def test_http_import_wrapper_base64_and_delete(http_server) -> None:
    store, base = http_server
    raw = encode_binary([Record("wrapped", b"\xff\x00", 1, 1_900_000_000_000, None)])
    wrapped = requests.post(
        f"{base}/import",
        json={"batch_id": "wrapped", "format": "binary", "data_base64": base64.b64encode(raw).decode()},
        timeout=5,
    )
    assert wrapped.status_code == 200
    assert store.get("wrapped")["value_encoding"] == "base64"
    deleted = requests.delete(
        f"{base}/keys/wrapped", json={"version": 2}, timeout=5
    )
    assert deleted.status_code == 200
    missing = requests.get(f"{base}/keys/wrapped", timeout=5)
    assert missing.status_code == 404

