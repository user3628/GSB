from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from rag_orchestrator.engine import Orchestrator, stable_bytes
from rag_orchestrator.web import WorkspaceService, create_app


def test_http_workbench_and_diagnostics(sample_env, tmp_path) -> None:
    service = WorkspaceService(
        config_path=sample_env.config_path,
        docs_path=sample_env.docs_path,
        requests_path=sample_env.requests_path,
        workspace=tmp_path / "http-ws",
    )
    app = create_app(service)
    client = app.test_client()

    dashboard = client.get("/")
    assert dashboard.status_code == 200
    assert "并行编排与预算裁决工作台" in dashboard.get_data(as_text=True)
    health = client.get("/api/health").get_json()
    assert health["status"] == "ok" and health["offline"] is True
    workspace = client.get("/api/workspace").get_json()
    assert workspace["counts"]["document_records"] >= 3
    assert workspace["counts"]["requests"] >= 5

    result = client.post("/api/run", json={"query_id": "q-001", "mode": "parallel"}).get_json()
    assert result["status"] == "ok"
    assert len(result["version"]["stage_fingerprints"]) == 4
    assert result["answer"]["citation_coverage"]["fully_covered"] is True
    assert client.get("/api/results/q-001").get_json()["answer"] == result["answer"]
    checkpoints = client.get("/api/checkpoints/q-001").get_json()
    assert [item["seq"] for item in checkpoints["records"]] == [1, 2, 3, 4]
    diagnostics = client.get("/api/diagnostics/q-001").get_json()
    assert diagnostics["result"]["status"] == "ok"
    assert diagnostics["workspace"]["engine_version"]

    invalid = client.post("/api/run", json={"query_id": "q-006", "mode": "parallel"}).get_json()
    assert invalid["error"]["code"] == "E_QUERY_INVALID"
    missing = client.post("/api/run", json={"query_id": "missing", "mode": "parallel"}).get_json()
    assert missing["error"]["code"] == "E_REQUEST_NOT_FOUND"
    stop = client.post("/api/stop").get_json()
    assert stop["stop_requested"] is True


def test_http_run_all(sample_env, tmp_path) -> None:
    service = WorkspaceService(
        config_path=sample_env.config_path,
        docs_path=sample_env.docs_path,
        requests_path=sample_env.requests_path,
        workspace=tmp_path / "http-all-ws",
    )
    result = create_app(service).test_client().post("/api/run-all", json={"mode": "serial"}).get_json()
    assert result["request_count"] == 7
    assert result["summary_sha256"]
    assert any(item["status"] == "error" for item in result["results"])


def test_cli_stable_output_matches_engine(sample_env, tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    cli_workspace = tmp_path / "cli-ws"
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root)
    process = subprocess.run(
        [
            sys.executable,
            "-m",
            "rag_orchestrator.cli",
            "--workspace",
            str(cli_workspace),
            "run",
            "q-001",
            "--stable",
        ],
        cwd=root,
        env=env,
        capture_output=True,
        check=True,
    )
    cli_bytes = process.stdout.rstrip(b"\n")
    cfg, docs, _ = sample_env.load()
    engine_result = Orchestrator(cfg, docs, str(tmp_path / "engine-ws"), mode="parallel").run(sample_env.request("q-001"))
    assert cli_bytes == stable_bytes(engine_result)
    assert json.loads(cli_bytes)["answer"]["summary_sha256"] == engine_result["answer"]["summary_sha256"]
