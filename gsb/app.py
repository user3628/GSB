"""Local Flask API and diagnostic workbench."""

from __future__ import annotations

import platform
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template, request, send_file

from .errors import WorkbenchError
from .service import WorkbenchService


def _error_status(code: str) -> int:
    if code == "E_NUMERIC":
        return 422
    if code in {
        "E_BATCH_STATE",
        "E_CALIBRATION_INFEASIBLE",
        "E_STALE_CHECKPOINT",
        "E_INVALID_STAGE",
        "E_INCREMENTAL_MISMATCH",
        "E_TRAIN_BUSY",
    }:
        return 409
    if code == "E_INTERNAL":
        return 500
    return 400


def _json_body() -> dict[str, Any]:
    if not request.is_json:
        return {}
    value = request.get_json(silent=True)
    return value if isinstance(value, dict) else {}


def create_app(
    project_root: Path,
    *,
    state_dir: Path | None = None,
    data_dir: Path | None = None,
) -> Flask:
    repository_root = Path(__file__).resolve().parent.parent
    app = Flask(
        __name__,
        template_folder=str(repository_root / "templates"),
        static_folder=str(repository_root / "static"),
    )
    app.config["JSON_SORT_KEYS"] = False
    service = WorkbenchService(project_root, state_dir=state_dir, data_dir=data_dir)
    app.extensions["gsb_service"] = service

    @app.errorhandler(WorkbenchError)
    def handle_workbench_error(error: WorkbenchError):
        return jsonify(error.to_dict()), _error_status(error.code)

    @app.errorhandler(Exception)
    def handle_unexpected(error: Exception):
        if isinstance(error, WorkbenchError):
            return handle_workbench_error(error)
        app.logger.exception("unhandled workbench error")
        return jsonify({"ok": False, "error": {"code": "E_INTERNAL", "message": str(error), "details": {}}}), 500

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.get("/api/health")
    def health():
        return jsonify(
            {
                "ok": True,
                "service": "gsb-calibration-workbench",
                "python": platform.python_version(),
                "project_root": str(service.project_root),
                "paths_within_project": True,
            }
        )

    @app.get("/api/datasets")
    def datasets():
        return jsonify({"ok": True, **service.datasets()})

    @app.post("/api/import")
    def import_dataset():
        body = _json_body()
        key = str(body.get("dataset") or body.get("key") or "small")
        result = service.import_dataset(key)
        return jsonify(result), 200

    @app.get("/api/config")
    def get_config():
        return jsonify(service.get_config())

    @app.put("/api/config")
    def put_config():
        body = _json_body()
        patch = body.get("config", body)
        if not isinstance(patch, dict):
            raise WorkbenchError("E_INVALID_PARAM", "config patch must be an object")
        result = service.set_config(patch)
        return jsonify(result)

    @app.post("/api/reconfigure")
    def reconfigure():
        body = _json_body()
        patch = body.get("config", body.get("patch", {}))
        if not isinstance(patch, dict):
            raise WorkbenchError("E_INVALID_PARAM", "config patch must be an object")
        result = service.reconfigure(
            patch,
            rebuild=bool(body.get("rebuild", True)),
            verify_incremental=bool(body.get("verify_incremental", True)),
        )
        return jsonify(result)

    @app.post("/api/train")
    def train():
        body = _json_body()
        resume = bool(body.get("resume", False))
        until_stage = str(body.get("until_stage", "diagnostics"))
        stop_after_rounds = body.get("stop_after_rounds")
        wait = bool(body.get("wait", False))
        if stop_after_rounds is not None:
            stop_after_rounds = int(stop_after_rounds)
        if wait:
            result = service.build(resume=resume, until_stage=until_stage, stop_after_rounds=stop_after_rounds)
            return jsonify({"ok": True, **result})
        result = service.train_background(resume=resume, until_stage=until_stage, stop_after_rounds=stop_after_rounds)
        return jsonify(result), 202

    @app.post("/api/stop")
    def stop():
        return jsonify(service.request_stop("http_stop"))

    @app.post("/api/resume")
    def resume():
        body = _json_body()
        until_stage = str(body.get("until_stage", "diagnostics"))
        wait = bool(body.get("wait", False))
        if wait:
            result = service.build(resume=True, until_stage=until_stage)
            return jsonify({"ok": True, **result})
        result = service.train_background(resume=True, until_stage=until_stage)
        return jsonify(result), 202

    @app.get("/api/status")
    def status():
        return jsonify({"ok": True, **service.status()})

    @app.get("/api/reliability")
    def reliability():
        return jsonify(service.reliability())

    @app.get("/api/diagnostics")
    def diagnostics():
        return jsonify(service.diagnostics())

    @app.post("/api/compare-batches")
    def compare_batches():
        body = _json_body()
        sizes = body.get("batch_sizes", [7, 64, 257])
        if not isinstance(sizes, list) or not sizes:
            raise WorkbenchError("E_BATCH_STATE", "batch_sizes must be a non-empty array")
        return jsonify(service.compare_batches([int(value) for value in sizes]))

    @app.post("/api/verify-incremental")
    def verify_incremental():
        return jsonify(service.verify_incremental())

    @app.get("/api/version")
    def version():
        status = service.status()
        return jsonify(
            {
                "ok": True,
                "config_version": status.get("config_version"),
                "seq": status.get("seq"),
                "fingerprints": status.get("fingerprints"),
                "invalidated": {
                    stage: details
                    for stage, details in status["stages"].items()
                    if details["status"] == "invalid"
                },
            }
        )

    @app.get("/api/export/probabilities")
    def export_probabilities():
        result = service.export(Path(service.project_root) / "out" / "http")
        return send_file(
            Path(result["probabilities_path"]),
            mimetype="text/csv",
            as_attachment=True,
            download_name="calibrated_probabilities.csv",
        )

    @app.get("/api/export/diagnostics")
    def export_diagnostics():
        result = service.export(Path(service.project_root) / "out" / "http")
        return send_file(
            Path(result["report_path"]),
            mimetype="application/json",
            as_attachment=True,
            download_name="calibration_diagnostics.json",
        )

    @app.get("/api/export/report")
    def export_report():
        return jsonify(service.export_report())

    return app
