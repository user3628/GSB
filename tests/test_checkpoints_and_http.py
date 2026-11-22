from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from sonic_sep.checkpoints import CheckpointStore
from sonic_sep.errors import AppError
from sonic_sep.server import make_server
from sonic_sep.util import atomic_write_json, digest_json, read_json

from conftest import imported_workspace


def _record(block_index: int = 0, product_byte: str = "a") -> dict:
    return {
        "schema_version": 1,
        "slice_id": "slice-001",
        "stage": "stft",
        "block_index": block_index,
        "frame_start": 0,
        "frame_end": 1,
        "bands": [0, 15],
        "input_fingerprint": "1" * 64,
        "config_version": "2" * 64,
        "product_digest": product_byte * 64,
        "round": None,
        "stage_complete": True,
    }


def test_checkpoint_publish_is_lock_protected_and_rejects_stale_sequences(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path / "state")
    first = store.publish(_record(block_index=0))
    assert first["sequence"] == 1
    with pytest.raises(AppError) as stale_sequence:
        store.publish(_record(block_index=1), sequence=1)
    assert stale_sequence.value.code == "E_STALE_CHECKPOINT"
    with pytest.raises(AppError) as duplicate:
        store.publish(_record(block_index=0))
    assert duplicate.value.code == "E_STALE_CHECKPOINT"
    second = store.publish(_record(block_index=1))
    assert second["sequence"] == 2
    assert second["block_sequence"] == 1
    assert not list((tmp_path / "state" / "checkpoints").glob("*.tmp"))


def test_checkpoint_recovery_ignores_corruption_and_duplicate_sequences(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path / "state")
    store.publish(_record(block_index=0))
    store.publish(_record(block_index=1))
    directory = tmp_path / "state" / "checkpoints"
    # A duplicated sequence with an alternate filename is ignored wholesale.
    (directory / "000000000001-copy.json").write_text((directory / "000000000001.json").read_text(), encoding="utf-8")
    # A truncated/corrupt record and a non-record JSON file are ignored.
    (directory / "000000000003.json").write_text('{"sequence": 3', encoding="utf-8")
    (directory / "notes.json").write_text('{"hello": "world"}', encoding="utf-8")
    scan = store.scan()
    assert scan["latest_sequence"] == 2
    assert scan["recoverable_sequence"] == 2
    assert [item["sequence"] for item in scan["recoverable_prefix"]] == [1, 2]
    assert scan["diagnostics"]
    recovered = store.publish(_record(block_index=2, product_byte="c"))
    assert recovered["sequence"] == 3
    assert store.scan()["recoverable_sequence"] == 3


def test_parallel_publishers_get_unique_contiguous_sequences(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path / "state")
    results: list[int] = []
    failures: list[Exception] = []
    barrier = threading.Barrier(8)

    def worker(index: int) -> None:
        try:
            barrier.wait()
            record = store.publish(_record(block_index=index, product_byte=hex(index % 16)[-1]))
            results.append(record["sequence"])
        except Exception as exc:  # pragma: no cover - diagnostic path
            failures.append(exc)

    threads = [threading.Thread(target=worker, args=(index,)) for index in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
    assert not failures
    assert sorted(results) == list(range(1, 9))
    scan = store.scan()
    assert scan["recoverable_sequence"] == scan["latest_sequence"] == 8
    assert scan["diagnostics"] == []


def _request_json(base_url: str, path: str, payload: dict | None = None) -> tuple[int, dict]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        base_url + path,
        data=data,
        headers={"Content-Type": "application/json"},
        method="GET" if payload is None else "POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def test_http_import_build_status_incremental_and_audio(dataset_root: Path, tmp_path: Path) -> None:
    workspace = imported_workspace(dataset_root, tmp_path / "state")
    server = make_server(workspace, "127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{server.server_port}"
    try:
        status, health = _request_json(base_url, "/api/health")
        assert status == 200 and health["ok"] is True
        status, imported = _request_json(
            base_url,
            "/api/import",
            {
                "manifest_path": str(dataset_root / "samples" / "manifest.json"),
                "config_path": str(dataset_root / "configs" / "default.yaml"),
            },
        )
        assert status == 200 and imported["slice_count"] == 3
        status, built = _request_json(base_url, "/api/build", {"slice_id": "slice-001"})
        assert status == 200
        assert built["results"][0]["status"] == "complete"
        status, status_payload = _request_json(base_url, "/api/status?slice_id=slice-001")
        assert status == 200 and status_payload["slices"][0]["complete"] is True
        status, version = _request_json(base_url, "/api/version?slice_id=slice-001")
        assert status == 200 and version["slices"][0]["matches_persisted"] is True
        status, bad = _request_json(
            base_url,
            "/api/incremental",
            {"slice_id": "slice-001", "source_id": 99, "bands": [1, 2]},
        )
        assert status == 404 and bad["error"]["code"] == "E_NOT_FOUND"
        status, incremental = _request_json(
            base_url,
            "/api/incremental",
            {"slice_id": "slice-001", "source_id": 0, "bands": [2, 3], "f0_min_hz": 170.0, "f0_max_hz": 200.0},
        )
        assert status == 200 and incremental["full_equivalent"] is True
        with urllib.request.urlopen(base_url + "/api/audio/slice-001/source-000.wav", timeout=30) as response:
            assert response.status == 200
            assert response.headers.get_content_type() == "audio/wav"
            assert len(response.read()) > 1000
        status, stopped = _request_json(base_url, "/api/stop", {})
        assert status == 200 and stopped["stop_requested"] is True
        status, resumed = _request_json(base_url, "/api/resume", {"slice_id": "slice-001"})
        assert status == 200 and resumed["status"] == "complete"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)

