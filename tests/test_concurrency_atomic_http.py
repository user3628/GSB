from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

import pytest

from qpipe.checkpoints import CheckpointStore
from qpipe.errors import InvalidStage, QPipeIOError, StaleCheckpoint
from qpipe.http_api import make_server
from qpipe.pipeline import Pipeline


def _run_cli(args: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(cwd)
    return subprocess.run(
        [sys.executable, "-m", "qpipe", *args],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_real_file_lock_concurrent_publish(pipeline: Pipeline, tmp_path: Path) -> None:
    repository_root = Path(__file__).resolve().parents[1]
    common = [
        "publish-layer",
        "--workspace",
        str(tmp_path),
        "--state-dir",
        ".qpipe-test",
        "--run-id",
        "race",
        "--stage",
        "calibration",
        "--layer-seq",
        "1",
        "--layer-id",
        "l0",
    ]
    env = dict(os.environ)
    env["PYTHONPATH"] = str(repository_root)
    processes = [
        subprocess.Popen(
            [
                sys.executable,
                "-m",
                "qpipe",
                *common,
                "--input-fingerprint",
                f"fp-{index}",
                "--artifact-digest",
                f"artifact-{index}",
            ],
            cwd=tmp_path,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for index in range(4)
    ]
    results = [process.communicate(timeout=20) for process in processes]
    codes = sorted(process.returncode for process in processes)
    assert codes == [0, 5, 5, 5], results
    ledger = json.loads((pipeline.root / "runs" / "race" / "publish_ledger.json").read_text())
    assert ledger["stages"]["calibration"]["next_layer_seq"] == 2

    with pytest.raises(StaleCheckpoint):
        pipeline.publish_layer_probe(
            run_id="race",
            stage="calibration",
            layer_seq=1,
            layer_id="l0",
            input_fingerprint="duplicate",
            artifact_digest="duplicate",
        )
    with pytest.raises(StaleCheckpoint):
        pipeline.publish_layer_probe(
            run_id="race",
            stage="calibration",
            layer_seq=2,
            layer_id="l2",
            input_fingerprint="cross",
            artifact_digest="cross",
        )
    with pytest.raises(InvalidStage):
        pipeline.publish_layer_probe(
            run_id="race",
            stage="forward",
            layer_seq=1,
            layer_id="l0",
            input_fingerprint="forward",
            artifact_digest="forward",
        )


def test_atomic_checkpoint_does_not_leave_half_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = CheckpointStore(tmp_path / "run")
    original_replace = os.replace

    def failing_replace(source: object, destination: object) -> None:
        destination_path = Path(destination)
        if destination_path.name == "00000001.json":
            raise OSError("injected crash before rename")
        original_replace(source, destination)

    monkeypatch.setattr("qpipe.util.os.replace", failing_replace)
    with pytest.raises(QPipeIOError):
        store.write(
            stage="calibration",
            layer="l0",
            unit="layer",
            input_fingerprint="input",
            config_version="v1",
            artifact_digest="artifact",
            stage_fingerprint="fingerprint",
        )
    assert list(store.directory.glob("*.json")) == []
    assert store.next_sequence() == 1


def test_http_interface_runs_and_queries(pipeline: Pipeline) -> None:
    server = make_server(pipeline, port=0)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    def call(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
        data = None if body is None else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            f"http://127.0.0.1:{port}{path}",
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read())

    try:
        status, health = call("GET", "/api/health")
        assert status == 200 and health["ok"] is True and health["loopback"] is True
        status, accepted = call("POST", "/api/import", {"manifest": "samples/operator_manifest.json", "shapes": "samples/shape_declarations.json", "version": "http"})
        assert status == 201 and accepted["ok"] is True
        status, started = call("POST", "/api/run", {"run_id": "http-run", "version": "http"})
        assert status == 202 and started["job"]["status"] == "running"
        job_id = started["job"]["job_id"]
        for _ in range(200):
            _, job = call("GET", f"/api/jobs/{job_id}")
            if job["job"]["status"] != "running":
                break
            time.sleep(0.02)
        assert job["job"]["status"] == "pass"
        status, status_payload = call("GET", "/api/status?run_id=http-run&version=http")
        assert status == 200
        assert status_payload["status"]["stages"]["validation"]["ledger_status"] == "complete"
        _, layers = call("GET", "/api/layers?run_id=http-run")
        assert len(layers["layers"]["layers"]) == 4
        _, error_report = call("GET", "/api/errors?run_id=http-run")
        assert error_report["error_report"]["status"] == "pass"
        _, versions = call("GET", "/api/versions")
        assert "http" in versions["versions"]["versions"]
        _, verification = call("POST", "/api/verify", {"run_id": "http-run"})
        assert verification["verification"]["ok"] is True
        _, staged = call(
            "POST",
            "/api/stage",
            {"stage": "calibration", "run_id": "http-staged", "version": "http"},
        )
        staged_job = staged["job"]["job_id"]
        for _ in range(200):
            _, job = call("GET", f"/api/jobs/{staged_job}")
            if job["job"]["status"] != "running":
                break
            time.sleep(0.02)
        assert job["job"]["status"] == "stage_complete"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
