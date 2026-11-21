from __future__ import annotations

import json
import threading
import time
import urllib.request
from pathlib import Path

from gridpipe.http_api import create_server


def _request(host: str, port: int, method: str, path: str, body=None):
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        f"http://{host}:{port}{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.status, json.loads(response.read().decode("utf-8"))


def _wait(host: str, port: int) -> dict:
    for _ in range(200):
        status = _request(host, port, "GET", "/api/status")[1]["status"]
        if not status["http_running"]:
            return status
        time.sleep(0.02)
    raise AssertionError("HTTP pipeline did not finish")


def test_http_import_run_stop_resume_export(sample_project: Path, manifest_path: Path) -> None:
    work = sample_project / "work" / "http"
    output = sample_project / "output" / "http"
    server = create_server(sample_project, manifest_path, work, output, port=0)
    host, port = server.server_address[:2]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        assert _request(host, port, "GET", "/health")[1]["ok"] is True
        imported = _request(
            host,
            port,
            "POST",
            "/api/import",
            {"manifest": str(manifest_path), "work_dir": str(work), "output_dir": str(output)},
        )[1]["import"]
        assert imported["shard_count"] == 5
        _request(host, port, "POST", "/api/run", {"stop_after": 2, "workers": 2})
        stopped = _wait(host, port)
        assert stopped["status"] == "stopped"
        assert stopped["checkpoint_sequence"] == 2
        _request(host, port, "POST", "/api/resume", {"workers": 2})
        completed = _wait(host, port)
        assert completed["status"] == "completed"
        assert completed["checkpoint_sequence"] == 16
        budget = _request(host, port, "GET", "/api/budget")[1]["budget"]
        assert budget["within_total_budget"] is True
        fingerprints = _request(host, port, "GET", "/api/fingerprints")[1]["fingerprints"]
        assert "shard-0001" in fingerprints["shards"]
        shard = _request(host, port, "GET", "/api/shards/shard-0001")[1]["shard"]
        assert shard["status"] == "complete"
        exported = _request(host, port, "POST", "/api/export")[1]["export"]
        assert exported["summary"]["cell_row_col_accuracy_mean"] == 1.0
        assert (output / "export.json").is_file()
    finally:
        server.shutdown()
        server.server_close()
