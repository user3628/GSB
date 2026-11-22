from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request

from tracklab import cli
from tracklab.http_api import build_server


def test_full_and_stopped_resumed_runs_are_byte_identical(demo_workspace):
    settings, demo = demo_workspace
    assert demo["verification_passed"] is True
    assert demo["byte_identical"] is True
    assert demo["checkpoint_trees_identical"] is True
    assert demo["replayed_frames"] == 0
    assert demo["sequence_count"] == 3
    assert len(demo["stop_events"]) == 3
    assert all(event["status"] == "stopped" for event in demo["stop_events"])
    assert any(
        item["recovered_occlusion_count"] > 0 for item in demo["sequences"]
    )
    assert (settings.reports_dir / "demo_summary.json").exists()
    assert (settings.snapshots_dir / "full" / "run_bundle.json").exists()
    assert (settings.snapshots_dir / "resume" / "run_bundle.json").exists()


def test_demo_cli_short_output_reads_existing_summary_key(workspace, monkeypatch, capsys):
    fake_summary = {
        "verification_passed": True,
        "byte_identical": True,
        "checkpoint_trees_identical": True,
        "replayed_frames": 0,
        "baseline_summary_sha256": "a" * 64,
        "demo_summary_sha256": "b" * 64,
    }
    monkeypatch.setattr(cli, "run_demo", lambda _settings: fake_summary)
    exit_code = cli.main(
        [
            "--root",
            str(workspace.root),
            "--config",
            str(workspace.config_path),
            "demo",
        ]
    )
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["baseline_summary_sha256"] == "a" * 64
    assert payload["demo_summary_sha256"] == "b" * 64


def _request(url: str, method: str = "GET", body=None):
    data = None if body is None else json.dumps(body).encode("utf-8")
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


def test_http_dashboard_api_and_stage_errors(demo_workspace):
    settings, _ = demo_workspace
    server = build_server(settings, host="127.0.0.1", port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    base = f"http://{host}:{port}"
    try:
        status, health = _request(base + "/api/health")
        assert status == 200 and health["ok"] is True

        status, sequences = _request(base + "/api/sequences")
        assert status == 200
        assert sequences["sequence_count"] == 3
        sequence_id = sequences["sequences"][0]["sequence_id"]

        status, frame = _request(base + f"/api/sequences/{sequence_id}/frame/0")
        assert status == 200
        assert frame["shape"] == [72, 96]
        assert frame["pixels_base64"]

        status, diagnostics = _request(base + f"/api/sequences/{sequence_id}/diagnostics/0")
        assert status == 200
        assert diagnostics["candidate"]["top_candidates"]
        assert diagnostics["trajectory"]["logical_time_ms"] == 0

        status, result = _request(base + f"/api/sequences/{sequence_id}/result")
        assert status == 200
        assert result["bundle"]["tracking_frames"]

        status, invalid = _request(base + f"/api/sequences/{sequence_id}/diagnostics/999")
        assert status == 400
        assert invalid["error"]["code"] == "E_INVALID_STAGE"

        status, invalid = _request(
            base + "/api/run",
            method="POST",
            body={"sequence_id": sequence_id, "stage": "not-a-stage"},
        )
        assert status == 400
        assert invalid["error"]["code"] == "E_INVALID_STAGE"

        status, stopped = _request(
            base + "/api/stop",
            method="POST",
            body={"sequence_id": sequence_id, "reason": "test"},
        )
        assert status == 200 and stopped["ok"] is True

        status, resumed = _request(
            base + "/api/resume",
            method="POST",
            body={"sequence_id": sequence_id},
        )
        assert status == 200
        assert resumed["status"] == "complete"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)
