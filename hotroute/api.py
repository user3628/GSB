from __future__ import annotations

import logging
from typing import Any

from flask import Flask, jsonify, request
from werkzeug.exceptions import BadRequest, HTTPException

from .engine import Engine
from .errors import (
    BusinessError,
    E_BAD_REQUEST,
    E_GRAPH_INVALID,
    E_GRAY_RATIO_INVALID,
    E_INTERNAL,
    E_MODEL_NOT_FOUND,
    E_REQUEST_BINDING_MISMATCH,
    E_REQUEST_INTERRUPTED,
    E_SNAPSHOT_STALE,
    E_STALE_BASE,
    E_STATE_EMPTY,
    E_VERSION_CONFLICT,
    E_VERSION_NOT_FOUND,
    E_VOCAB_MISSING,
)


LOGGER = logging.getLogger("hotroute.api")


ERROR_STATUS = {
    E_BAD_REQUEST: 400,
    E_GRAPH_INVALID: 422,
    E_VOCAB_MISSING: 422,
    E_GRAY_RATIO_INVALID: 400,
    E_STALE_BASE: 409,
    E_SNAPSHOT_STALE: 409,
    E_VERSION_CONFLICT: 409,
    E_REQUEST_BINDING_MISMATCH: 409,
    E_REQUEST_INTERRUPTED: 409,
    E_MODEL_NOT_FOUND: 404,
    E_VERSION_NOT_FOUND: 404,
    E_STATE_EMPTY: 404,
    E_INTERNAL: 500,
}


def _json_body() -> dict[str, Any]:
    try:
        body = request.get_json(force=False, silent=False)
    except BadRequest as exc:
        raise BusinessError(
            E_BAD_REQUEST,
            "request body must be valid JSON",
            {"content_type": request.content_type},
        ) from exc
    if not isinstance(body, dict):
        raise BusinessError(E_BAD_REQUEST, "request body must be a JSON object", {"field": "body"})
    return body


def create_app(engine: Engine) -> Flask:
    app = Flask(__name__)
    app.config["JSON_AS_ASCII"] = False
    app.json.ensure_ascii = False
    app.config["TESTING"] = False

    @app.errorhandler(BusinessError)
    def handle_business_error(exc: BusinessError):
        return jsonify(exc.to_dict()), ERROR_STATUS.get(exc.code, 400)

    @app.errorhandler(HTTPException)
    def handle_http_error(exc: HTTPException):
        return (
            jsonify(
                {
                    "ok": False,
                    "code": E_BAD_REQUEST,
                    "message": exc.description,
                    "context": {"http_status": exc.code},
                }
            ),
            exc.code,
        )

    @app.errorhandler(Exception)
    def handle_unexpected_error(exc: Exception):
        LOGGER.exception("unhandled request failure")
        return (
            jsonify(
                {
                    "ok": False,
                    "code": E_INTERNAL,
                    "message": "unexpected internal error",
                    "context": {"error_type": type(exc).__name__},
                }
            ),
            500,
        )

    def success(result: Any, status: int = 200):
        return jsonify({"ok": True, "data": result}), status

    @app.get("/health")
    def health():
        state = engine._current_state()
        return success(
            {
                "service": "hotroute",
                "loopback_only_expected": True,
                "has_state": state is not None,
                "snapshot_seq": state["snapshot_seq"] if state else None,
            }
        )

    @app.post("/api/snapshots")
    def submit_snapshot():
        return success(engine.submit_snapshot(_json_body()), 201)

    @app.post("/api/increments")
    def submit_increment():
        return success(engine.submit_increment(_json_body()), 201)

    @app.get("/api/migrations")
    def migrations():
        snapshot_seq = request.args.get("snapshot_seq")
        model_id = request.args.get("model_id")
        limit = request.args.get("limit", "100")
        try:
            snapshot_int = int(snapshot_seq) if snapshot_seq is not None else None
            limit_int = int(limit)
        except ValueError as exc:
            raise BusinessError(
                E_BAD_REQUEST,
                "snapshot_seq and limit must be integers",
                {"snapshot_seq": snapshot_seq, "limit": limit},
            ) from exc
        return success(
            engine.list_migrations(
                snapshot_seq=snapshot_int,
                model_id=model_id,
                limit=limit_int,
            )
        )

    @app.get("/api/artifacts")
    def artifacts():
        return success(
            engine.get_artifacts(
                model_id=request.args.get("model_id"),
                version=request.args.get("version"),
            )
        )

    @app.get("/api/state")
    def state():
        return success(engine.get_state())

    @app.post("/api/grays/decide")
    def gray_decide():
        body = _json_body()
        return success(
            engine.decide_gray(
                request_id=body.get("request_id"),
                model_id=body.get("model_id"),
                ratio=body.get("ratio"),
            )
        )

    @app.post("/api/grays/normalize")
    def gray_normalize():
        body = _json_body()
        return success(engine.normalize_ratio(body.get("ratio")))

    @app.post("/api/infer")
    def infer():
        body = _json_body()
        return success(
            engine.infer(
                request_id=body.get("request_id"),
                model_id=body.get("model_id"),
                seed=body.get("seed", 42),
                version=body.get("version"),
            )
        )

    @app.get("/api/bindings/<request_id>")
    def binding(request_id: str):
        return success(engine.get_binding(request_id))

    @app.get("/api/recovery")
    def recovery():
        return success(engine.get_recovery_report())

    return app

