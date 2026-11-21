from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import yaml
from fastapi.testclient import TestClient

from headworkbench.config import current_config_path, install_config, load_config
from headworkbench.demo import create_mismatch_fixture, reset_stage_cache, reset_versions
from headworkbench.service import WorkbenchService, create_app


def test_http_workflow_and_graceful_control(workspace: Path) -> None:
    reset_versions(workspace)
    reset_stage_cache(workspace)
    client = TestClient(create_app(workspace))

    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["status"] == "ok"
    assert health.json()["current_version"] is None

    diagnostics = client.get("/api/diagnostics")
    assert diagnostics.status_code == 200
    assert diagnostics.json()["dataset"]["head_count"] == 6
    assert diagnostics.json()["layout"]["file_lock_available"] is True

    run = client.post("/api/run", json={"window_name": "compact", "mode": "incremental"})
    assert run.status_code == 200
    run_body = run.json()
    assert run_body["suggested_head_widths"]
    assert run_body["recomputed_stages"] == ["reference", "probe", "adjudication", "allocation"]
    assert run_body["consistency"]["match"] is True

    adjudication = client.get("/api/adjudication?window=compact&query_id=test-query")
    assert adjudication.status_code == 200
    adjudication_body = adjudication.json()
    assert adjudication_body["query"]["query_id"] == "test-query"
    assert adjudication_body["query"]["cache_hit"] is False
    assert adjudication_body["query"]["cache_key"]["config_version"] == "demo-2026-11-22-v1"
    assert len(adjudication_body["adjudication"]["ranking"]) == 6
    cached = client.get("/api/adjudication?window=compact&query_id=cached-query")
    assert cached.status_code == 200
    assert cached.json()["query"]["cache_hit"] is True

    widths = client.get("/api/widths?window=compact")
    assert widths.status_code == 200
    assert widths.json()["head_widths"] == run_body["suggested_head_widths"]

    commit = client.post(
        "/api/versions/commit",
        json={
            "head_widths": run_body["suggested_head_widths"],
            "expected_base_version": 0,
            "window_name": "compact",
            "note": "http test",
            "verify_full": True,
        },
    )
    assert commit.status_code == 200
    assert commit.json()["version"] == 1
    assert commit.json()["status"] == "active"

    versions = client.get("/api/versions")
    assert versions.status_code == 200
    assert versions.json()["current_version"] == 1

    detail = client.get("/api/versions/1")
    assert detail.status_code == 200
    assert detail.json()["head_widths"] == run_body["suggested_head_widths"]

    rollback = client.post(
        "/api/versions/rollback",
        json={"target_version": 1, "expected_base_version": 1, "note": "manual test rollback"},
    )
    assert rollback.status_code == 200
    assert rollback.json()["version"] == 2
    assert rollback.json()["status"] == "manual_rollback"

    assert client.post("/api/recover").status_code == 200
    stop = client.post("/api/control/stop")
    assert stop.status_code == 200
    assert stop.json()["graceful"] is True


def test_http_head_mismatch_and_latency_budget(workspace: Path) -> None:
    reset_versions(workspace)
    manifest, _ = create_mismatch_fixture(workspace)
    client = TestClient(create_app(workspace))
    response = client.post(
        "/api/import",
        json={"manifest_path": str(manifest.relative_to(workspace))},
    )
    assert response.status_code == 400
    error = response.json()["error"]
    assert error["code"] == "E_HEAD_MISMATCH"
    assert error["head_id"] == 0
    assert error["projection_shape"] == [2, 7, 8]

    config_path = current_config_path(workspace)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    payload["version"] = "latency-zero-v2"
    payload["latency_budget_ms"] = 0.0
    modified = workspace / "config" / "latency-zero.yaml"
    modified.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    install_config(workspace, modified)
    latency_client = TestClient(create_app(workspace))
    latency = latency_client.get("/api/adjudication?window=compact&query_id=latency-query")
    assert latency.status_code == 408
    body = latency.json()["error"]
    assert body["code"] == "E_LATENCY_BUDGET"
    assert body["query_id"] == "latency-query"
    assert body["elapsed_ms"] > 0
    assert body["budget_ms"] == 0.0


def test_cache_key_contains_window_and_config_and_invalidates(workspace: Path) -> None:
    reset_versions(workspace)
    service = WorkbenchService(workspace)
    config = service.config()
    compact = service.query_key(config, "compact")
    full = service.query_key(config, "full")
    assert compact["cache_key"] != full["cache_key"]
    assert compact["window_digest"] != full["window_digest"]
    assert compact["config_version"] == config.version

    payload = yaml.safe_load(current_config_path(workspace).read_text(encoding="utf-8"))
    payload["version"] = "cache-key-v2"
    target = workspace / "config" / "cache-key-v2.yaml"
    target.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    install_config(workspace, target)
    changed = service.query_key(service.config(), "compact")
    assert changed["cache_key"] != compact["cache_key"]
    assert changed["config_version"] == "cache-key-v2"


def test_cli_diagnostics(workspace: Path) -> None:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    result = subprocess.run(
        [sys.executable, "-m", "headworkbench", "--workspace", str(workspace), "diagnostics"],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["head_count"] == 6
    assert payload["layout"]["workspace_writable"] is True

