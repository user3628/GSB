from __future__ import annotations

import json
import threading
from concurrent.futures import ProcessPoolExecutor
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from storypipe.checkpoints import CheckpointStore
from storypipe.errors import PipelineError
from storypipe.http_api import ApiHandler


def _publish_once(arguments: tuple[str, str]) -> str:
    workspace, fingerprint = arguments
    try:
        CheckpointStore(Path(workspace)).publish(
            stage="schedule",
            input_fingerprint=fingerprint,
            config_version="1",
            artifact_digest="sha256:" + "a" * 64,
            artifact_path="x.json",
            components={"x": "y"},
        )
        return "ok"
    except PipelineError as exc:
        return exc.code


def test_checkpoint_sequence_duplicate_and_order(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path / "order")
    schedule = store.publish(
        stage="schedule",
        input_fingerprint="fp-schedule",
        config_version="1",
        artifact_digest="digest-schedule",
        artifact_path="schedule.json",
        components={},
    )
    assert schedule["sequence"] == 1
    compose = store.publish(
        stage="compose",
        input_fingerprint="fp-compose",
        config_version="1",
        artifact_digest="digest-compose",
        artifact_path="compose.json",
        components={},
    )
    assert compose["sequence"] == 2
    with pytest.raises(PipelineError) as duplicate:
        store.publish(
            stage="schedule",
            input_fingerprint="fp-schedule",
            config_version="1",
            artifact_digest="digest-schedule",
            artifact_path="schedule.json",
            components={},
        )
    assert duplicate.value.code == "E_STALE_CHECKPOINT"
    with pytest.raises(PipelineError) as stale:
        store.publish(
            stage="schedule",
            input_fingerprint="fp-new",
            config_version="1",
            artifact_digest="digest-new",
            artifact_path="schedule.json",
            components={},
            sequence=1,
        )
    assert stale.value.code == "E_STALE_CHECKPOINT"
    assert not list(store.directory.rglob("*.tmp"))


def test_out_of_order_stage_publication(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path / "invalid-stage")
    with pytest.raises(PipelineError) as caught:
        store.publish(
            stage="transition",
            input_fingerprint="fp",
            config_version="1",
            artifact_digest="digest",
            artifact_path="transition.json",
            components={},
        )
    assert caught.value.code == "E_INVALID_STAGE"


def test_concurrent_duplicate_publication_uses_lock(tmp_path: Path) -> None:
    workspace = tmp_path / "concurrent"
    workspace.mkdir()
    arguments = [(str(workspace), "same-fingerprint")] * 2
    with ProcessPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(_publish_once, arguments))
    assert sorted(results) == ["E_STALE_CHECKPOINT", "ok"]
    record = CheckpointStore(workspace).read("schedule")
    assert record is not None
    assert record["sequence"] == 1


@pytest.fixture()
def http_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), ApiHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _get_json(url: str) -> tuple[int, dict]:
    with urlopen(url, timeout=20) as response:
        return response.status, json.loads(response.read())


def _post_json(url: str, payload: dict) -> tuple[int, dict]:
    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=30) as response:
        return response.status, json.loads(response.read())


def test_http_import_run_status_export_control(
    sample_root: Path, tmp_path: Path, http_server: str
) -> None:
    workspace = tmp_path / "http-workspace"
    status, imported = _post_json(
        f"{http_server}/v1/import",
        {"sample_dir": str(sample_root / "small"), "workspace": str(workspace)},
    )
    assert status == 200 and imported["ok"]
    status, ran = _post_json(f"{http_server}/v1/run", {"workspace": str(workspace)})
    assert status == 200 and ran["ok"]
    status, inspected = _get_json(f"{http_server}/v1/status?workspace={workspace}")
    assert status == 200 and inspected["ok"]
    status, fingerprints = _get_json(f"{http_server}/v1/fingerprints?workspace={workspace}")
    assert status == 200 and fingerprints["overall_fingerprint"]
    status, exported = _get_json(f"{http_server}/v1/export/manifest?workspace={workspace}")
    assert status == 200 and exported["manifest"]["frame_count"] > 0
    status, stopped = _post_json(f"{http_server}/v1/stop", {"workspace": str(workspace)})
    assert status == 200 and stopped["ok"]
    status, resumed = _post_json(f"{http_server}/v1/resume", {"workspace": str(workspace)})
    assert status == 200 and resumed["ok"]


def test_http_business_error_mapping(tmp_path: Path, http_server: str) -> None:
    workspace = tmp_path / "http-error"
    workspace.mkdir()
    request = Request(
        f"{http_server}/v1/checkpoints/publish",
        data=json.dumps(
            {
                "workspace": str(workspace),
                "stage": "transition",
                "input_fingerprint": "fp",
                "artifact_digest": "digest",
            }
        ).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with pytest.raises(HTTPError) as caught:
        urlopen(request, timeout=20)
    assert caught.value.code == 409
    payload = json.loads(caught.value.read())
    assert payload["error"]["code"] == "E_INVALID_STAGE"
