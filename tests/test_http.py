from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request

from tonelab.server import create_server


def _request(base: str, method: str, path: str, payload: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = urllib.request.Request(
        base + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def test_local_http_workbench(isolated_workspace) -> None:
    workspace, _engine, full = isolated_workspace
    server = create_server(workspace.root, host="127.0.0.1", port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.server_address[1]
    base = f"http://127.0.0.1:{port}"
    try:
        with urllib.request.urlopen(base + "/", timeout=10) as response:
            html = response.read().decode("utf-8")
            assert "ToneLab" in html
            assert "/api/build" in html
        status, health = _request(base, "GET", "/api/health")
        assert status == 200 and health["ok"] is True
        status, state = _request(base, "GET", "/api/status")
        assert status == 200
        assert state["project_id"] == "test-demo"
        assert len(state["shards"]) == 4
        status, checkpoints = _request(base, "GET", "/api/checkpoints")
        assert status == 200 and checkpoints["continuous"] is True
        assert checkpoints["last_sequence"] == 16
        status, incremental = _request(base, "POST", "/api/build", {"mode": "incremental"})
        assert status == 200
        assert incremental["deterministic_fingerprint"] == full["deterministic_fingerprint"]
        status, roundtrip = _request(base, "POST", "/api/roundtrip", {"format": "wav"})
        assert status == 200 and roundtrip["stable_export"] is True
        shard_id = next(iter(state["shards"]))
        status, error = _request(
            base,
            "POST",
            "/api/edit/unit",
            {"shard_id": shard_id, "unit_index": 999, "end_ms": 10},
        )
        assert status == 404
        assert error["error"]["code"] == "E_NOT_FOUND"
        status, stopped = _request(base, "POST", "/api/stop", {"reason": "http test"})
        assert status == 200 and stopped["requested"] is True
        status, resumed = _request(base, "POST", "/api/resume", {})
        assert status == 200 and resumed["stopped"] is False
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)

