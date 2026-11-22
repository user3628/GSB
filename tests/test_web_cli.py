from __future__ import annotations

import json

from audioq.cli import main
from audioq.web import create_app


def test_http_workbench_end_to_end(workspace):
    root = workspace["root"]
    app = create_app(
        root,
        workspace["manifest"],
        workspace["config"],
        state_dir="state_http",
    )
    app.config.update(TESTING=True)
    client = app.test_client()

    page = client.get("/")
    assert page.status_code == 200
    assert b"AudioQ" in page.data and b"Workbench" in page.data

    health = client.get("/api/health")
    assert health.status_code == 200
    assert health.get_json()["ok"] is True

    run = client.post("/api/run", json={})
    assert run.status_code == 200
    assert run.get_json()["status"] == "complete"

    status = client.get("/api/status").get_json()
    assert len(status["shards"]) == 4
    assert status["shards"][0]["last_diagnostics"]["fused_score"] is not None

    report = client.get("/api/report")
    assert report.status_code == 200
    assert report.get_json()["summary"]["shard_count"] == 4

    shard = client.get("/api/shards/clean")
    assert shard.status_code == 200
    assert shard.get_json()["diagnostics"]["metrics"]["scores"]["snr"] is not None

    export = client.post("/api/report/export", json={"path": "state_http/report_api.json"})
    assert export.status_code == 200
    assert export.get_json()["summary"]["shard_count"] == 4

    converted = client.post(
        "/api/convert",
        json={
            "input_path": str(workspace["legacy_config"]),
            "target_version": 2,
            "snr_threshold_rule": "weight_scaled",
        },
    )
    assert converted.status_code == 200
    payload = converted.get_json()
    assert payload["derived_snr_threshold_db"] == 8.0
    assert payload["exported"]["schema_version"] == 2

    invalid = client.post(
        "/api/convert",
        json={
            "config": {"schema_version": 2, "weights": {"snr": 0, "spectral": 0, "clipping": 0}},
            "target_version": 2,
        },
    )
    assert invalid.status_code == 422
    assert invalid.get_json()["code"] == "E_INVALID_WEIGHT"

    stop = client.post("/api/stop", json={"reason": "http-test"})
    assert stop.status_code == 200
    assert stop.get_json()["stop_requested"] is True


def test_http_import_and_resume(workspace):
    root = workspace["root"]
    app = create_app(root, workspace["manifest"], workspace["config"], state_dir="state_http_resume")
    app.config.update(TESTING=True)
    client = app.test_client()
    stopped = client.post("/api/run", json={"stop_after_shards": 1}).get_json()
    assert stopped["status"] == "stopped"
    resumed = client.post("/api/resume", json={}).get_json()
    assert resumed["status"] == "complete"
    imported = client.post(
        "/api/import",
        json={
            "manifest_path": str(workspace["manifest"]),
            "config_path": str(workspace["legacy_config"]),
            "state_dir": "state_http_import",
        },
    )
    assert imported.status_code == 200
    assert imported.get_json()["shard_count"] == 4
    status = client.get("/api/status").get_json()
    assert status["config"]["schema_version"] == 1


def test_cli_demo_and_convert(workspace, capsys):
    root = workspace["root"]
    output = root / "test_cases"
    output.mkdir(exist_ok=True)
    exit_code = main(
        [
            "convert",
            "--root",
            str(root),
            "--input",
            str(workspace["legacy_config"]),
            "--output",
            "test_cases/cli_v1_export.json",
            "--target-version",
            "1",
        ]
    )
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["downgrade_restore"]["metrics.snr_threshold_db"] == 8.0
    exported = json.loads((root / "test_cases/cli_v1_export.json").read_text(encoding="utf-8"))
    assert exported["weights_percent"] == {"snr": 40, "spectral": 35, "clipping": 25}

    exit_code = main(
        [
            "status",
            "--root",
            str(root),
            "--manifest",
            str(workspace["manifest"]),
            "--config",
            str(workspace["config"]),
            "--state-dir",
            "state_cli_status",
        ]
    )
    assert exit_code == 0
    status = json.loads(capsys.readouterr().out)
    assert len(status["shards"]) == 4


def test_cli_returns_structured_error(workspace, capsys):
    root = workspace["root"]
    bad = root / "test_cases" / "bad_zero_weights.json"
    bad.parent.mkdir(exist_ok=True)
    bad.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "evaluation_id": "bad",
                "frame": {"length": 400, "hop": 160, "window": "hann"},
                "metrics": {},
                "weights": {"snr": 0, "spectral": 0, "clipping": 0},
                "fusion": {},
                "default_reference": {"min_mos": 0, "max_mos": 5, "target_mos": 4},
            }
        ),
        encoding="utf-8",
    )
    exit_code = main(
        [
            "run",
            "--root",
            str(root),
            "--manifest",
            str(workspace["manifest"]),
            "--config",
            "test_cases/bad_zero_weights.json",
            "--state-dir",
            "state_cli_bad",
        ]
    )
    assert exit_code == 2
    error = json.loads(capsys.readouterr().out)
    assert error["ok"] is False
    assert error["code"] == "E_INVALID_WEIGHT"
