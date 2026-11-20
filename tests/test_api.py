from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

from conftest import DATA_DIR
from linreg_workbench.api import create_server


def request(base: str, path: str, *, method: str = "GET", body: dict | None = None):
    payload = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        base + path,
        data=payload,
        method=method,
        headers={"Content-Type": "application/json"} if payload is not None else {},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            raw = response.read()
            if response.headers.get_content_type() == "application/json":
                return response.status, json.loads(raw), dict(response.headers)
            return response.status, raw, dict(response.headers)
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        return exc.code, json.loads(raw), dict(exc.headers)


def test_http_workbench_lifecycle(tmp_path: Path) -> None:
    server = create_server(tmp_path / "http", data_dir=DATA_DIR, port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    base = f"http://{host}:{port}"
    try:
        status, health, _ = request(base, "/api/health")
        assert status == 200 and health["ok"] is True
        status, html, headers = request(base, "/")
        assert status == 200 and b"LinearLab" in html
        assert "text/html" in headers["Content-Type"]
        status, js, _ = request(base, "/static/app.js")
        assert status == 200 and b"startTraining" in js

        import_csv = tmp_path / "http-import.csv"
        import_lines = ["a,b,label"]
        import_lines += [f"{index % 5},{index % 3},{index % 2}" for index in range(36)]
        import_csv.write_text("\n".join(import_lines) + "\n", encoding="utf-8")
        status, imported, _ = request(
            base,
            "/api/import",
            method="POST",
            body={"csv_path": str(import_csv), "dataset_id": "http-import", "target": "label"},
        )
        assert status == 201 and imported["dataset"]["rows"] == 36

        config_patch = {
            "dataset_id": "small",
            "cv": {"folds": 2, "seed": 5, "metric": "log_loss"},
            "regularization": {
                "mode": "elasticnet",
                "strengths": [0.02],
                "l1_ratios": [0.5],
            },
            "solver": {
                "name": "coordinate_descent",
                "tolerance": 1e-5,
                "max_iter": 60,
                "objective_limit": 1000000.0,
            },
        }
        status, payload, _ = request(
            base,
            "/api/train",
            method="POST",
            body={"config_patch": config_patch, "background": True, "stop_after_tasks": 1},
        )
        assert status == 202 and payload["accepted"] is True
        stopped = None
        for _ in range(120):
            status, payload, _ = request(base, "/api/status")
            if payload["status"]["status"] in {"stopped", "error"}:
                stopped = payload["status"]
                break
            time.sleep(0.05)
        assert stopped is not None and stopped["status"] == "stopped"
        assert stopped["checkpoints"]["last_sequence"] == 3
        status, resumed, _ = request(base, "/api/resume", method="POST", body={"background": True})
        assert status == 202 and resumed["accepted"] is True
        final = None
        for _ in range(120):
            status, payload, _ = request(base, "/api/status")
            if payload["status"]["status"] in {"complete", "error"}:
                final = payload["status"]
                break
            time.sleep(0.05)
        assert final is not None
        assert final["status"] == "complete", final.get("error")
        assert final["checkpoints"]["last_sequence"] == 6
        assert [record["checkpoint_sequence"] for record in final["folds"]] == [3, 4, 5]

        status, summary_payload, _ = request(base, "/api/summary")
        assert status == 200
        assert summary_payload["summary"]["result_digest"] == final["result_digest"]
        status, coefficients, _ = request(base, "/api/export/coefficients")
        assert status == 200 and len(coefficients["coefficients"]) == 20
        status, trajectory, _ = request(base, "/api/export/trajectory")
        assert status == 200 and trajectory["trajectory"][0]["iteration"] == 0

        status, invalid, _ = request(
            base,
            "/api/train",
            method="POST",
            body={"config_patch": {"regularization": {"l1_ratios": [4.0]}}},
        )
        assert status == 422
        assert invalid["error"]["code"] == "E_INVALID_HYPERPARAM"
        status, stop_payload, _ = request(
            base, "/api/stop", method="POST", body={"session_id": "default"}
        )
        assert status == 202 and stop_payload["accepted"] is False
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
