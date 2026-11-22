from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from multiscale_fusion.http_api import make_http_server
from multiscale_fusion.samplegen import generate_demo_bundle

from .conftest import run_cli


def _json_request(url: str, *, method: str = "GET", payload: dict[str, Any] | None = None) -> tuple[int, dict[str, Any]]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def test_cli_generate_import_run_status_and_verify(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    workspace = tmp_path / "workspace"
    generated = run_cli("generate", str(bundle), "--force")
    assert generated.returncode == 0, generated.stderr
    assert json.loads(generated.stdout)["fixed_seed"] == 20261123

    imported = run_cli("import", "--workspace", str(workspace), "--bundle", str(bundle), "--force")
    assert imported.returncode == 0, imported.stderr
    assert json.loads(imported.stdout)["tasks"] == ["demo_fpn"]

    run = run_cli("run", "--workspace", str(workspace), "--task", "demo_fpn", "--verify-full")
    assert run.returncode == 0, run.stderr
    payload = json.loads(run.stdout)
    assert payload["status"] == "completed"
    assert payload["verification"]["result"] == "incremental_matches_full_rebuild"

    status = run_cli("status", "--workspace", str(workspace), "--task", "demo_fpn")
    assert status.returncode == 0, status.stderr
    status_payload = json.loads(status.stdout)
    assert status_payload["sensitivity"]["current"] is True
    assert all(item["current"] for item in status_payload["layers"].values())

    sensitivity = run_cli("sensitivity", "--workspace", str(workspace), "--task", "demo_fpn")
    assert sensitivity.returncode == 0, sensitivity.stderr
    sensitivity_payload = json.loads(sensitivity.stdout)
    assert sensitivity_payload["current"] is True
    assert sensitivity_payload["report"]["additivity_ok"] is True

    checkpoints = run_cli("verify-checkpoints", "--workspace", str(workspace), "--task", "demo_fpn")
    assert checkpoints.returncode == 0, checkpoints.stderr
    assert json.loads(checkpoints.stdout)["classification"] == "normal"

    incremental = run_cli("verify-incremental", "--workspace", str(workspace), "--task", "demo_fpn")
    assert incremental.returncode == 0, incremental.stderr
    assert json.loads(incremental.stdout)["result"] == "incremental_matches_full_rebuild"


def test_local_http_import_stage_run_stop_resume_and_queries(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    workspace = tmp_path / "workspace"
    generate_demo_bundle(bundle, force=True)
    server = make_http_server(workspace, port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    base = f"http://{host}:{port}"
    try:
        status, health = _json_request(base + "/health")
        assert status == 200 and health["ok"] is True

        status, imported = _json_request(
            base + "/v1/import",
            method="POST",
            payload={"bundle_dir": str(bundle), "force": True},
        )
        assert status == 200, imported
        assert imported["tasks"] == ["demo_fpn"]

        status, staged = _json_request(
            base + "/v1/stage",
            method="POST",
            payload={"task_id": "demo_fpn", "layer": "p4"},
        )
        assert status == 200, staged
        assert staged["status"] == "partial"
        assert staged["completed_layers"] == ["p5", "p4"]

        status, status_payload = _json_request(base + "/v1/status?task_id=demo_fpn")
        assert status == 200
        assert status_payload["layers"]["p5"]["current"] is True
        assert status_payload["layers"]["p4"]["current"] is True
        assert status_payload["layers"]["p3"]["current"] is False

        status, completed = _json_request(
            base + "/v1/run",
            method="POST",
            payload={"task_id": "demo_fpn", "verify_full": True},
        )
        assert status == 200, completed
        assert completed["status"] == "completed"
        assert completed["reused_layers"] == ["p5", "p4"]
        assert completed["verification"]["result"] == "incremental_matches_full_rebuild"

        status, sensitivity = _json_request(base + "/v1/sensitivity?task_id=demo_fpn")
        assert status == 200
        assert sensitivity["current"] is True
        assert sensitivity["report"]["additivity_ok"] is True

        status, quantization = _json_request(base + "/v1/quantization?task_id=demo_fpn")
        assert status == 200
        assert set(quantization["report"]["layers"]) == {"p3", "p4", "p5"}

        status, invalidation = _json_request(base + "/v1/invalidation?task_id=demo_fpn")
        assert status == 200
        assert invalidation["report"]["mode"] == "incremental"

        status, stopped = _json_request(base + "/v1/stop", method="POST", payload={"task_id": "demo_fpn"})
        assert status == 200 and stopped["stop_requested"] is True
        status, stopped_run = _json_request(base + "/v1/run", method="POST", payload={"task_id": "demo_fpn"})
        assert status == 200
        assert stopped_run["status"] == "stopped"
        status, resumed = _json_request(base + "/v1/resume", method="POST", payload={"task_id": "demo_fpn"})
        assert status == 200
        assert resumed["status"] == "completed"
        assert resumed["reused_layers"] == ["p5", "p4", "p3"]

        status, checkpoints = _json_request(base + "/v1/checkpoints?task_id=demo_fpn")
        assert status == 200
        assert checkpoints["classification"] == "normal"
        assert checkpoints["head_sequence"] == 3
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
