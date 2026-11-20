from __future__ import annotations

import json
import threading
from pathlib import Path
from urllib.request import Request, urlopen

from panopipe.httpapi import create_server
from panopipe.workspace import Workspace


def _request(url: str, *, data: dict | None = None) -> tuple[int, bytes, str]:
    payload = None
    headers = {}
    if data is not None:
        payload = json.dumps(data).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = Request(url, data=payload, headers=headers, method="POST" if data is not None else "GET")
    with urlopen(request, timeout=10) as response:
        return response.status, response.read(), response.headers.get("Content-Type", "")


def test_http_import_status_build_export_and_stop(tmp_path: Path, demo_dir: Path) -> None:
    workspace = Workspace(tmp_path / "http-workspace")
    server = create_server(workspace, "127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    base = f"http://{host}:{port}"
    try:
        status, body, _ = _request(f"{base}/api/health")
        assert status == 200
        assert json.loads(body)["workspace_imported"] is False

        status, body, _ = _request(
            f"{base}/api/import",
            data={
                "manifest_path": str(demo_dir / "manifest_v2.json"),
                "parse_options": {"confidence_threshold": 0.6},
            },
        )
        assert status == 200
        assert len(json.loads(body)["shards"]) == 4

        status, body, _ = _request(f"{base}/api/build", data={})
        build = json.loads(body)
        assert status == 200
        assert build["ok"] is True
        assert build["checkpoint_sequence"] == 16

        status, body, content_type = _request(f"{base}/api/status?shard_id=shard_1_indexed")
        assert status == 200
        assert content_type.startswith("application/json")
        assert json.loads(body)["shards"]["shard_1_indexed"]["stages"]["decode"]["status"] == "ready"

        status, body, content_type = _request(
            f"{base}/api/export?shard_id=shard_1_indexed&format=png"
        )
        assert status == 200
        assert content_type == "image/png"
        assert body.startswith(b"\x89PNG\r\n\x1a\n")

        status, body, _ = _request(f"{base}/api/stop", data={"reason": "test"})
        assert status == 200
        assert workspace.stop_requested() is True

        status, body, _ = _request(f"{base}/api/resume", data={})
        assert status == 200
        assert workspace.stop_requested() is False
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
