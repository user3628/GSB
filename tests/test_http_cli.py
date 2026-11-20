from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from conv_workbench.server import create_server


def _request(base: str, path: str, *, method: str = "GET", body: dict | None = None):
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        base + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"} if data is not None else {},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return response.status, response.headers, response.read()


@pytest.fixture
def live_server(demo_workspace: Path, tmp_path: Path):
    server = create_server(demo_workspace, tmp_path / "http_run", host="127.0.0.1", port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    try:
        yield f"http://{host}:{port}", server.workbench
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_http_health_browser_run_status_export_and_errors(live_server):
    base, workbench = live_server
    status, _headers, body = _request(base, "/api/health")
    assert status == 200 and json.loads(body)["status"] == "healthy"
    status, headers, body = _request(base, "/")
    assert status == 200 and "Convolution / Interpolation Workbench" in body.decode()

    with pytest.raises(urllib.error.HTTPError) as raised:
        _request(
            base,
            "/api/stage",
            method="POST",
            body={"shard_id": "medium_odd_dilated", "stage_index": 2},
        )
    error = json.loads(raised.value.read())
    assert raised.value.code == 400
    assert error["error"]["code"] == "E_INVALID_STAGE"

    status, _headers, body = _request(base, "/api/run", method="POST", body={})
    result = json.loads(body)
    assert status == 200
    assert result["status"] == "complete"
    assert result["executed_stages"] == 9

    status, _headers, body = _request(base, "/api/status")
    assert status == 200
    status_document = json.loads(body)
    assert all(shard["complete"] for shard in status_document["shards"].values())

    status, _headers, output = _request(base, "/api/outputs/small_odd_pad")
    assert status == 200 and output.startswith(b"\x93NUMPY")
    status, _headers, trace = _request(base, "/api/trace")
    assert status == 200 and "shape_trajectory" in json.loads(trace)["shards"]["small_odd_pad"]

    status, _headers, body = _request(base, "/api/export", method="POST", body={})
    assert status == 200 and json.loads(body)["status"] == "complete"
    status, _headers, body = _request(
        base,
        "/api/fingerprints?shard=small_odd_pad",
    )
    assert status == 200
    assert json.loads(body)["chain"][0]["active_checkpoint"]["checkpoint_seq"] == 1

    status, _headers, body = _request(base, "/api/stop", method="POST", body={"reason": "http test"})
    assert status == 202 and json.loads(body)["status"] == "stop_requested"
    assert workbench.run_dir.joinpath("STOP_REQUEST.json").is_file()
    workbench.run_dir.joinpath("STOP_REQUEST.json").unlink()


def test_http_import_rolls_back_invalid_chain(live_server, demo_workspace: Path):
    base, workbench = live_server
    manifest_before = (demo_workspace / "manifest.json").read_bytes()
    source = demo_workspace / "tensors" / "small_odd_pad.npy"
    payload = {
        "id": "bad_import",
        "source_path": str(source),
        "operators": [
            {"name": "conv2d", "params": {"stride": [1, 1], "padding": "same", "dilation": [1, 1]}}
        ],
    }
    with pytest.raises(urllib.error.HTTPError) as raised:
        _request(base, "/api/import", method="POST", body=payload)
    error = json.loads(raised.value.read())
    assert raised.value.code == 400
    assert error["error"]["code"] == "E_INVALID_STAGE"
    assert (demo_workspace / "manifest.json").read_bytes() == manifest_before
    assert not (demo_workspace / "tensors" / "bad_import.npy").exists()


def _cli(root: Path, workspace: Path, run_dir: Path, *args: str) -> subprocess.CompletedProcess:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(root)
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "conv_workbench",
            "--workspace",
            str(workspace),
            "--run-dir",
            str(run_dir),
            *args,
        ],
        cwd=root,
        env=environment,
        text=True,
        capture_output=True,
        timeout=30,
    )


def test_cli_self_check_stop_resume_and_export(demo_workspace: Path, tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    run_dir = tmp_path / "cli_run"
    self_check = _cli(root, demo_workspace, run_dir, "self-check")
    assert self_check.returncode == 0, self_check.stderr
    assert len(json.loads(self_check.stdout)["shards"]) == 3

    stopped = _cli(root, demo_workspace, run_dir, "run", "--stop-after", "2")
    assert stopped.returncode == 0, stopped.stderr
    assert json.loads(stopped.stdout)["status"] == "stopped"
    resumed = _cli(root, demo_workspace, run_dir, "resume")
    assert resumed.returncode == 0, resumed.stderr
    assert json.loads(resumed.stdout)["status"] == "complete"

    destination = tmp_path / "small_export.npy"
    exported = _cli(
        root,
        demo_workspace,
        run_dir,
        "export",
        "--shard",
        "small_odd_pad",
        "--output",
        str(destination),
    )
    assert exported.returncode == 0, exported.stderr
    assert destination.read_bytes().startswith(b"\x93NUMPY")

