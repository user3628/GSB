from __future__ import annotations

import csv
import json
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

from feature_workbench import Workbench
from feature_workbench.http_server import make_server


def test_export_contains_only_selected_features(workspace: Path) -> None:
    workbench = Workbench(root=workspace, state_dir=workspace / "state", export_dir=workspace / "exports")
    result = workbench.run(worker_mode="sequential")
    summary = workbench.read_selection_summary()
    assert summary is not None
    for tier, info in summary["tiers"].items():
        selected_path = workspace / info["exports"]["selected_csv"]
        with selected_path.open("r", encoding="utf-8", newline="") as handle:
            header = next(csv.reader(handle))
        assert header == info["selected"]
        assert set(info["eliminated"]).isdisjoint(header)
        encoding_path = workspace / info["exports"]["encoding_summaries"]
        encoding = json.loads(encoding_path.read_text(encoding="utf-8"))
        assert encoding["shards"]
        first = encoding["shards"][0]
        assert first["mapping"]
        assert first["source_domains"]
        assert first["feature_domains"]
    assert result["result_digest"] == summary["result_digest"]


def test_cli_status_and_version(workspace: Path) -> None:
    command = [
        sys.executable,
        "-m",
        "feature_workbench",
        "--root",
        str(workspace),
        "status",
    ]
    completed = subprocess.run(command, cwd=Path(__file__).resolve().parent.parent, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr
    status = json.loads(completed.stdout)
    assert status["ok"] is True
    assert "pipeline_version" in status
    assert "budget" in status


def test_import_samples_copies_declaration_config_and_shards(workspace: Path, tmp_path: Path) -> None:
    target_root = tmp_path / "import-target"
    target = Workbench(root=target_root)
    imported = target.import_samples(workspace / "data")
    assert imported["ok"] is True
    assert len(imported["copied_files"]) == 5
    assert (target_root / "data" / "manifest.json").is_file()
    assert (target_root / "data" / "config.yaml").is_file()
    status = target.status()
    assert status["status"] == "idle"
    assert status["pipeline_version"]


def test_http_status_config_and_staged_run(workspace: Path) -> None:
    workbench = Workbench(
        root=workspace,
        state_dir=workspace / "http-state",
        export_dir=workspace / "http-exports",
    )
    workbench.run(worker_mode="sequential")
    server = make_server(workbench, port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.server_address[1]

    def request(method: str, path: str, body=None):
        data = None if body is None else json.dumps(body).encode("utf-8")
        request_object = urllib.request.Request(
            f"http://127.0.0.1:{port}{path}",
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request_object, timeout=10) as response:
            return response.status, json.loads(response.read()), dict(response.headers)

    try:
        assert request("GET", "/api/health")[1]["ok"] is True
        status_code, status, headers = request("GET", "/api/status")
        assert status_code == 200
        assert "X-Query-Latency-Ms" in headers
        assert status["status"] == "complete"
        updated = request("PUT", "/api/config", {"changes": {"selection.threshold": 0.02}})
        assert updated[1]["config"]["selection"]["threshold"] == 0.02
        version_before_run = request("GET", "/api/version")[1]
        assert version_before_run["affected_earliest_stage"] == "selection"
        accepted = request(
            "POST",
            "/api/run",
            {"until_stage": "type_inference", "worker_mode": "sequential"},
        )
        assert accepted[0] == 202
        final_status = None
        for _ in range(200):
            final_status = request("GET", "/api/status")[1]
            if not final_status["background"]["running"]:
                break
            time.sleep(0.02)
        assert final_status is not None
        assert final_status["background"]["running"] is False
        assert final_status["stages"]["type_inference"]["complete"] is True
        version = request("GET", "/api/version")[1]
        assert version["affected_earliest_stage"] == "selection"
    finally:
        server.shutdown()
        server.server_close()

