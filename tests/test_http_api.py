from __future__ import annotations

import base64
import json
import urllib.error
import urllib.request
from pathlib import Path

from cabin_voice.http_api import start_background_server
from cabin_voice.storage import Workspace


def _request(base: str, method: str, route: str, payload: dict | None = None):
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        base + route,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"} if data else {},
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read().decode("utf-8"))


def test_http_workbench_import_build_query_and_config(tmp_path: Path, generated_demo: dict) -> None:
    workspace = Workspace(tmp_path / "http_workspace").ensure()
    server, thread = start_background_server(workspace, port=0)
    host, port = server.server_address
    base = f"http://{host}:{port}"
    try:
        status, payload = _request(base, "GET", "/health")
        assert status == 200 and payload["ok"]
        status, payload = _request(base, "POST", "/import", {"manifest_path": generated_demo["manifest"]})
        assert status == 200 and payload["chunks"] == 3
        status, payload = _request(base, "POST", "/build", {"mode": "full", "verify": True})
        assert status == 200 and payload["status"] == "complete"
        assert payload["verification"]["equal"] is True
        status, payload = _request(base, "GET", "/status")
        assert status == 200 and payload["status"] == "complete"
        status, payload = _request(base, "GET", "/fingerprints")
        assert status == 200 and payload["chunks"]["chunk_001"]["detection"]["status"] == "published"
        status, payload = _request(
            base,
            "POST",
            "/config",
            {"path": "attribution.review_gap_threshold", "value": 0.05, "verify": True},
        )
        assert status == 200 and payload["build"]["verification"]["equal"] is True
        assert payload["build"]["invalidation"]["chunks"]["chunk_002"]["earliest_stage"] == "attribution"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_http_sample_replacement_and_error_mapping(tmp_path: Path, generated_demo: dict) -> None:
    workspace = Workspace(tmp_path / "http_replace").ensure()
    workspace.import_bundle(generated_demo["manifest"])
    from cabin_voice.engine import Engine

    Engine(workspace).build(mode="full")
    server, thread = start_background_server(workspace, port=0)
    host, port = server.server_address
    base = f"http://{host}:{port}"
    try:
        source = next((Path(generated_demo["output"]) / "audio").glob("*.wav"))
        encoded = base64.b64encode(source.read_bytes()).decode("ascii")
        status, payload = _request(
            base,
            "POST",
            "/samples/replace",
            {"chunk_id": "chunk_001", "wav_base64": encoded, "verify": True},
        )
        assert status == 200 and payload["build"]["verification"]["equal"] is True
        status, payload = _request(
            base, "POST", "/config", {"path": "detection.hop_ms", "value": 0, "build": False}
        )
        assert status == 400
        assert payload["code"] == "E_INVALID_CONFIG"
        assert payload["details"]["parameter"] == "detection.hop_ms"
        status, payload = _request(base, "GET", "/does-not-exist")
        assert status == 404 and payload["code"] == "E_NOT_FOUND"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_http_stop_and_recover(tmp_path: Path, generated_demo: dict) -> None:
    workspace = Workspace(tmp_path / "http_stop").ensure()
    workspace.import_bundle(generated_demo["manifest"])
    server, thread = start_background_server(workspace, port=0)
    host, port = server.server_address
    base = f"http://{host}:{port}"
    try:
        status, payload = _request(
            base, "POST", "/build", {"mode": "full", "stop_after_units": 3, "verify": False}
        )
        assert status == 200
        assert payload["status"] == "stopped"
        assert payload["last_seq"] == 3
        status, payload = _request(base, "POST", "/recover", {"verify": True})
        assert status == 200
        assert payload["status"] == "complete"
        assert payload["last_seq"] == 12
        assert payload["verification"]["equal"] is True
        status, payload = _request(base, "GET", "/status")
        assert status == 200 and payload["status"] == "complete"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
