from __future__ import annotations

import uuid
from typing import Any

from flask import Flask, jsonify, request
from werkzeug.exceptions import HTTPException

from .config import WorkbenchConfig
from .errors import BusinessError
from .service import SlowQueryService


def _json_body(*, allow_empty: bool = False) -> Any:
    if allow_empty and (request.content_length in {None, 0} or not request.data):
        return {}
    if not request.is_json:
        raise BusinessError(
            "E_REQUEST_INVALID",
            "The request body must use Content-Type: application/json.",
        )
    try:
        return request.get_json()
    except Exception as exc:
        raise BusinessError("E_REQUEST_INVALID", "Malformed JSON request body.") from exc


def create_app(
    config: WorkbenchConfig | None = None,
    *,
    service: SlowQueryService | None = None,
    recover_on_init: bool = True,
) -> Flask:
    config = config or WorkbenchConfig.from_sources()
    app = Flask(__name__)
    app.json.ensure_ascii = False
    workbench = service or SlowQueryService(config, recover_on_init=recover_on_init)
    app.extensions["slowquery"] = workbench
    app.config["SLOWQUERY_CONFIG"] = config

    @app.errorhandler(BusinessError)
    def handle_business_error(error: BusinessError):
        return jsonify(error.as_dict()), error.http_status or 400

    @app.errorhandler(HTTPException)
    def handle_http_error(error: HTTPException):
        code = "E_NOT_FOUND" if error.code == 404 else "E_REQUEST_INVALID"
        body = {
            "error": {
                "code": code,
                "message": error.description,
                "details": {"http_status": error.code},
            }
        }
        return jsonify(body), error.code or 500

    @app.errorhandler(Exception)
    def handle_unexpected(error: Exception):
        if isinstance(error, BusinessError):
            return handle_business_error(error)
        app.logger.exception("unhandled workbench error")
        body = {
            "error": {
                "code": "E_INTERNAL_ERROR",
                "message": "An unexpected local workbench error occurred.",
                "details": {"exception_type": type(error).__name__},
            }
        }
        return jsonify(body), 500

    @app.get("/api/health")
    def health():
        return jsonify(
            {
                "status": "ok",
                "database": str(config.database_path),
                "sample_database": str(config.sample_database_path),
                "active_snapshot_version": workbench.repo.active_snapshot_version(),
            }
        )

    @app.post("/api/samples")
    def submit_samples():
        result = workbench.submit_samples(_json_body())
        return jsonify({"ok": True, **result}), 201

    @app.post("/api/snapshots")
    def load_snapshot():
        result = workbench.load_snapshot(_json_body())
        return jsonify({"ok": True, **result}), 201

    @app.get("/api/snapshots/active")
    def active_snapshot():
        snapshot = workbench.repo.get_active_snapshot()
        if snapshot is None:
            raise BusinessError(
                "E_SNAPSHOT_INVALID", "No statistics snapshot is currently active."
            )
        return jsonify(
            {
                "version": snapshot["version"],
                "generated_at": snapshot["generated_at"],
                "digest": snapshot["digest"],
                "summary": snapshot["summary"],
            }
        )

    @app.post("/api/sessions")
    def create_session():
        body = _json_body()
        if not isinstance(body, dict):
            raise BusinessError("E_REQUEST_INVALID", "Session request must be a JSON object.")
        sample_id = body.get("sample_id")
        session_id = body.get("session_id") or ("diag_" + uuid.uuid4().hex)
        result = workbench.create_session(session_id=session_id, sample_id=sample_id)
        return jsonify({"ok": True, **result}), 201 if result["created"] else 200

    @app.get("/api/sessions/<session_id>")
    def get_session(session_id: str):
        return jsonify(workbench.get_session(session_id))

    @app.get("/api/sessions/<session_id>/trace")
    def get_trace(session_id: str):
        return jsonify(
            {
                "session_id": session_id,
                "trace": workbench.repo.get_session_trace(session_id),
            }
        )

    @app.post("/api/sessions/<session_id>/diagnose")
    def diagnose(session_id: str):
        body = _json_body(allow_empty=True)
        if not isinstance(body, dict):
            raise BusinessError("E_REQUEST_INVALID", "Diagnosis request must be a JSON object.")
        snapshot_version = body.get("snapshot_version")
        if snapshot_version is not None and (
            isinstance(snapshot_version, bool) or not isinstance(snapshot_version, int)
        ):
            raise BusinessError(
                "E_REQUEST_INVALID", "snapshot_version must be an integer when supplied."
            )
        return jsonify(
            workbench.diagnose(session_id, requested_snapshot_version=snapshot_version)
        )

    @app.get("/api/sessions/<session_id>/suggestions")
    def suggestions(session_id: str):
        return jsonify(workbench.get_suggestions(session_id))

    @app.post("/api/sessions/<session_id>/verify")
    def verify(session_id: str):
        body = _json_body(allow_empty=True)
        if not isinstance(body, dict):
            raise BusinessError("E_REQUEST_INVALID", "Verification request must be a JSON object.")
        suggestion_id = body.get("suggestion_id")
        if suggestion_id is not None and not isinstance(suggestion_id, str):
            raise BusinessError("E_REQUEST_INVALID", "suggestion_id must be a string.")
        return jsonify(workbench.verify(session_id, suggestion_id=suggestion_id))

    @app.get("/api/recovery/report")
    def recovery_report():
        return jsonify(workbench.get_recovery_report())

    @app.get("/api/recovery/latest")
    def recovery_latest():
        return jsonify(workbench.get_recovery_report())

    return app
