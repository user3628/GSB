from __future__ import annotations

import json
import subprocess
import sys
import threading
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from chunkops.http_server import create_server
from chunkops.pipeline import Pipeline


def _request(
    method: str,
    url: str,
    payload: dict | None = None,
    *,
    expect_error: bool = False,
) -> tuple[int, dict]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        try:
            body = json.loads(exc.read().decode("utf-8"))
        finally:
            exc.close()
        if expect_error:
            return exc.code, body
        raise


@pytest.fixture()
def local_server():
    server = create_server("127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    try:
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_http_health_import_stage_stop_resume_reports(
    local_server: str,
    tmp_path: Path,
    fragment_paths: dict[str, str],
) -> None:
    workspace = tmp_path / "http_ws"
    status, health = _request("GET", local_server + "/health")
    assert status == 200
    assert health["result"]["status"] == "ok"
    status, imported = _request(
        "POST",
        local_server + "/api/fragments/import",
        {
            "workspace": str(workspace),
            "fragment": fragment_paths["small_base"],
            "fragment_id": "small_base",
            "op": "softmax",
            "axis": 1,
        },
    )
    assert status == 200
    assert imported["result"]["status"]["state"] == "imported"
    status, planned = _request(
        "POST",
        local_server + "/api/runs/stage",
        {"workspace": str(workspace), "stage": "plan"},
    )
    assert status == 200
    assert planned["result"]["state"] == "planned"
    status, plan_view = _request(
        "GET", local_server + f"/api/runs/plan?workspace={workspace}"
    )
    assert status == 200
    assert plan_view["result"]["plan"]["plan"]["row_block"] >= 1
    status, stopped = _request(
        "POST",
        local_server + "/api/runs/stop",
        {"workspace": str(workspace), "reason": "test"},
    )
    assert status == 200
    assert stopped["result"]["status"]["state"] == "stopped"
    status, resumed = _request(
        "POST",
        local_server + "/api/runs/resume",
        {"workspace": str(workspace)},
    )
    assert status == 200
    assert resumed["result"]["state"] == "complete"
    status, report = _request(
        "GET", local_server + f"/api/runs/errors?workspace={workspace}"
    )
    assert status == 200
    assert report["result"]["status"] == "ok"
    status, summary = _request(
        "GET", local_server + f"/api/runs/summary?workspace={workspace}"
    )
    assert status == 200
    assert summary["result"]["operation"]["op"] == "softmax"
    status, versions = _request(
        "GET", local_server + f"/api/runs/versions?workspace={workspace}"
    )
    assert status == 200
    assert versions["result"]["plans"]["latest_seq"] == 1
    assert versions["result"]["results"]["latest_seq"] == 1


def test_http_business_errors(local_server: str, tmp_path: Path) -> None:
    status, body = _request(
        "POST",
        local_server + "/api/fragments/import",
        {
            "workspace": str(tmp_path / "missing"),
            "fragment": str(tmp_path / "does-not-exist.npz"),
            "op": "softmax",
            "axis": 1,
        },
        expect_error=True,
    )
    assert status == 404
    assert body["code"] == "E_MISSING_FRAGMENT"
    status, body = _request(
        "GET",
        local_server + "/api/runs/status?workspace=" + str(tmp_path / "never-imported"),
        expect_error=True,
    )
    assert status == 409
    assert body["code"] == "E_INVALID_STATE"
    status, body = _request(
        "GET", local_server + "/api/unknown", expect_error=True
    )
    assert status == 404
    assert body["code"] == "E_NOT_FOUND"


def test_cli_status_and_export(
    tmp_path: Path, fragment_paths: dict[str, str]
) -> None:
    workspace = tmp_path / "cli_ws"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "chunkops",
            "--compact",
            "import",
            "--workspace",
            str(workspace),
            "--fragment",
            fragment_paths["small_base"],
            "--op",
            "softmax",
            "--axis",
            "1",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    run = subprocess.run(
        [sys.executable, "-m", "chunkops", "--compact", "run", "--workspace", str(workspace)],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    status = json.loads(run.stdout)
    assert status["state"] == "complete"
    status_cmd = subprocess.run(
        [
            sys.executable,
            "-m",
            "chunkops",
            "--compact",
            "status",
            "--workspace",
            str(workspace),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert json.loads(status_cmd.stdout)["state"] == "complete"
    report_out = tmp_path / "reported-errors.json"
    errors_cmd = subprocess.run(
        [
            sys.executable,
            "-m",
            "chunkops",
            "--compact",
            "errors",
            "--workspace",
            str(workspace),
            "--output",
            str(report_out),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert report_out.exists()
    assert json.loads(report_out.read_text(encoding="utf-8"))["status"] == "ok"
    summary_out = tmp_path / "exported-summary.json"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "chunkops",
            "--compact",
            "summary",
            "--workspace",
            str(workspace),
            "--output",
            str(summary_out),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert json.loads(summary_out.read_text(encoding="utf-8"))["operation"]["op"] == "softmax"
    compare = subprocess.run(
        [
            sys.executable,
            "-m",
            "chunkops",
            "--compact",
            "compare",
            "--left-workspace",
            str(workspace),
            "--right-workspace",
            str(workspace),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert json.loads(compare.stdout)["byte_identical"] is True
