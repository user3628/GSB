"""Requirement 8: local HTTP interface end-to-end."""
from __future__ import annotations

import json
import urllib.request

import pytest

from depworkbench.http_app import build_server


@pytest.fixture()
def server(tmp_path, ensure_samples):
    httpd = build_server("127.0.0.1", 0, tmp_path / "ws",
                         tmp_path / "cp")
    thread = __import__("threading").Thread(target=httpd.serve_forever,
                                            daemon=True)
    thread.start()
    host, port = httpd.server_address
    base = f"http://{host}:{port}"

    def call(method: str, path: str, payload=None):
        data = None
        headers = {}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(base + path, data=data,
                                         headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status, json.loads(
                    response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    yield call
    httpd.shutdown()
    httpd.server_close()
    thread.join(timeout=5)


def test_full_workflow_over_http(server, ensure_samples):
    status, body = server("GET", "/health")
    assert status == 200 and body["status"] == "ok"

    status, body = server("POST", "/import", {
        "shards_dir": str(ensure_samples),
        "manifest": str(ensure_samples / "manifest.json"),
        "config": str(ensure_samples / "config.yaml"),
    })
    assert status == 200 and body["shard_count"] == 3

    status, plan = server("GET", "/plan")
    assert status == 200
    assert all(d["admitted"] for d in plan["decisions"])

    status, report = server("POST", "/run", {})
    assert status == 200 and report["status"] == "completed"

    status, status_body = server("GET", "/status")
    assert status == 200
    assert len(status_body["shards"]) == 3
    assert all(s["state"] == "checkpointed" for s in status_body["shards"])

    status, stats = server("GET", "/stats")
    assert status == 200
    assert stats["stats"]["sentence_count"] == 19

    status, exported = server("GET", "/export", )
    assert status == 200 and "exported" in exported


def test_http_surfaces_invalid_config(server, ensure_samples):
    status, body = server("POST", "/import", {
        "shards_dir": str(ensure_samples),
        "manifest": str(ensure_samples / "manifest.json"),
        "config": str(ensure_samples / "config_bad_distance.yaml"),
    })
    assert status == 400
    assert body["error"]["code"] == "E_INVALID_CONFIG"


def test_http_stop_and_resume(server, ensure_samples):
    server("POST", "/import", {
        "shards_dir": str(ensure_samples),
        "manifest": str(ensure_samples / "manifest.json"),
        "config": str(ensure_samples / "config.yaml"),
    })
    status, body = server("POST", "/stop", {})
    assert status == 200 and body["stop_requested"]
    status, report = server("POST", "/run", {"sequential": True})
    assert status == 200 and report["status"] == "stopped"
    status, report = server("POST", "/resume", {})
    assert status == 200 and report["status"] == "completed"
