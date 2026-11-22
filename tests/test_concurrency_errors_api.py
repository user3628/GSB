from __future__ import annotations

import copy
import json
import multiprocessing
import os
import subprocess
import sys
import threading
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from density_trace.canonical import canonical_json_bytes, sha256_bytes
from density_trace.engine import Engine
from density_trace.errors import BusinessError
from density_trace.http_server import create_server
from density_trace.workspace import Workspace

from .conftest import make_engine


def _publish_worker(workspace_path: str, record: dict, barrier, queue) -> None:

    barrier.wait()
    try:
        Workspace(workspace_path).publish_record(record)
        queue.put(("ok", record["sequence"]))
    except BusinessError as exc:
        queue.put(("error", exc.code, exc.details))
    except Exception as exc:
        queue.put(("exception", repr(exc)))


def test_multiprocess_publish_rejects_stale_checkpoint(
    tmp_path: Path, data_copy: Path
) -> None:
    engine = make_engine(tmp_path / "workspace", data_copy)
    engine.run(batch_id=0)
    first = Engine.from_workspace(engine.workspace.root)
    second = Engine.from_workspace(engine.workspace.root)
    record_one = first.prepare_batch_record(1, reconcile=False)
    record_two = second.prepare_batch_record(1, reconcile=False)
    assert canonical_json_bytes(record_one) == canonical_json_bytes(record_two)

    context = multiprocessing.get_context("fork")
    barrier = context.Barrier(2)
    queue = context.Queue()
    processes = [
        context.Process(
            target=_publish_worker,
            args=(str(engine.workspace.root), record_one, barrier, queue),
        ),
        context.Process(
            target=_publish_worker,
            args=(str(engine.workspace.root), record_two, barrier, queue),
        ),
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(20)
        assert process.exitcode == 0
    results = [queue.get(timeout=5), queue.get(timeout=5)]
    assert sum(1 for result in results if result[0] == "ok") == 1
    errors = [result for result in results if result[0] == "error"]
    assert len(errors) == 1
    assert errors[0][1] == "E_STALE_CHECKPOINT"
    expected = canonical_json_bytes(record_one) + b"\n"
    assert engine.workspace.checkpoint_path(2).read_bytes() == expected
    assert sha256_bytes(
        engine.workspace.checkpoint_path(2).read_bytes()
    ) == sha256_bytes(expected)


def test_business_error_codes_and_rejected_publish_do_not_mutate(
    tmp_path: Path, data_copy: Path
) -> None:
    engine = make_engine(tmp_path / "workspace", data_copy)
    with pytest.raises(BusinessError) as jump:
        engine.run(batch_id=2)
    assert jump.value.code == "E_INVALID_STAGE"
    with pytest.raises(BusinessError) as missing_predecessor:
        engine.get_verdict(3)
    assert missing_predecessor.value.code == "E_INVALID_STAGE"
    with pytest.raises(BusinessError) as stale_version:
        engine.run(expected_version=99)
    assert stale_version.value.code == "E_STALE_VERSION"

    engine.run(batch_id=0)
    stale_record = engine.prepare_batch_record(1, reconcile=False)
    before = engine.workspace.record_bytes(1)
    engine.run(batch_id=1)
    accepted = engine.workspace.record_bytes(2)
    with pytest.raises(BusinessError) as stale_checkpoint:
        engine.workspace.publish_record(stale_record)
    assert stale_checkpoint.value.code == "E_STALE_CHECKPOINT"
    crossed = copy.deepcopy(stale_record)
    crossed["sequence"] = 3
    with pytest.raises(BusinessError) as cross_batch:
        engine.workspace.publish_record(crossed)
    assert cross_batch.value.code == "E_STALE_CHECKPOINT"
    assert engine.workspace.record_bytes(1) == before
    assert engine.workspace.record_bytes(2) == accepted
    with pytest.raises(BusinessError):
        engine.run(batch_id=0)


def test_http_api_import_run_query_stop_resume(tmp_path: Path, data_copy: Path) -> None:
    workspace = tmp_path / "http-workspace"
    server = create_server(workspace, host="127.0.0.1", port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"

    def request(
        method: str, path: str, payload: dict | None = None
    ) -> tuple[int, dict]:
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            base + path,
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    try:
        status, health = request("GET", "/health")
        assert status == 200 and health["ok"] is True
        status, error = request("GET", "/api/verdict?batch=0")
        assert status == 409
        assert error["error"]["code"] == "E_INVALID_STAGE"
        status, imported = request(
            "POST",
            "/api/import",
            {
                "manifest_path": str(data_copy / "manifest.json"),
                "config_path": str(data_copy / "config.json"),
                "reference_path": str(data_copy / "reference_quantiles.json"),
            },
        )
        assert status == 200 and imported["ok"] is True
        status, run = request("POST", "/api/run", {"max_batches": 2})
        assert status == 200 and run["result"]["status"] == "bounded"
        assert run["result"]["published_batches"] == [0, 1]
        status, verdict = request("GET", "/api/verdict?batch=1")
        assert status == 200 and verdict["result"]["state_version"] == 2
        status, version = request("GET", "/api/version")
        assert status == 200 and version["result"]["next_batch_id"] == 2
        status, errors = request("GET", "/api/errors?batch=0")
        assert status == 200 and errors["result"]["error_bound_density"] > 0
        status, stopped_request = request("POST", "/api/stop", {"reason": "http-test"})
        assert status == 200 and stopped_request["result"]["reason"] == "http-test"
        status, stopped = request("POST", "/api/resume", {})
        assert status == 200 and stopped["result"]["status"] == "stopped"
        status, completed = request("POST", "/api/resume", {})
        assert status == 200 and completed["result"]["status"] == "complete"
        status, curve = request("GET", "/api/curve?batch=3")
        assert status == 200 and curve["result"]["state_summary"]["state_version"] == 4
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_cli_import_run_status_and_error(tmp_path: Path, data_copy: Path) -> None:
    workspace = tmp_path / "cli-workspace"
    env = {"PYTHONPATH": str(Path.cwd() / "src")}
    import_result = subprocess.run(
        [
            sys.executable,
            "-m",
            "density_trace.cli",
            "import",
            "--workspace",
            str(workspace),
            "--manifest",
            str(data_copy / "manifest.json"),
            "--config",
            str(data_copy / "config.json"),
            "--reference",
            str(data_copy / "reference_quantiles.json"),
        ],
        capture_output=True,
        text=True,
        env={**os.environ, **env},
        check=False,
    )
    assert import_result.returncode == 0, import_result.stderr
    run_result = subprocess.run(
        [
            sys.executable,
            "-m",
            "density_trace.cli",
            "run",
            "--workspace",
            str(workspace),
            "--batch",
            "0",
        ],
        capture_output=True,
        text=True,
        env={**os.environ, **env},
        check=False,
    )
    assert run_result.returncode == 0, run_result.stderr
    payload = json.loads(run_result.stdout)
    assert payload["status"] == "published"
    status_result = subprocess.run(
        [
            sys.executable,
            "-m",
            "density_trace.cli",
            "status",
            "--workspace",
            str(workspace),
        ],
        capture_output=True,
        text=True,
        env={**os.environ, **env},
        check=False,
    )
    assert status_result.returncode == 0
    assert json.loads(status_result.stdout)["record_count"] == 1
    error_result = subprocess.run(
        [
            sys.executable,
            "-m",
            "density_trace.cli",
            "verdict",
            "--workspace",
            str(workspace),
            "--batch",
            "3",
        ],
        capture_output=True,
        text=True,
        env={**os.environ, **env},
        check=False,
    )
    assert error_result.returncode == 2
    assert json.loads(error_result.stderr)["error"]["code"] == "E_INVALID_STAGE"
