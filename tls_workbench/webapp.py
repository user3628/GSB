"""Local loopback Flask API for the TLS diagnostic workbench.

Endpoints (all bind to 127.0.0.1, no external network):
  POST /api/sessions                 create a handshake session
  POST /api/sessions/<id>/advance    single-writer advancement (idempotent)
  GET  /api/sessions/<id>            committed snapshot query
  GET  /api/sessions                 list sessions
  GET  /api/sessions/<id>/diagnostic full diagnostic payload
  POST /api/recover                  scan PENDING intents and recover
  GET  /api/chains/<chain_id>        parsed chain metadata
  GET  /api/numeric/audit            summation/rounding error explanation
  GET  /api/health                   liveness
"""

from __future__ import annotations

import json
import os

from flask import Flask, jsonify, request

from . import budget
from .engine import HandshakeConfig, HandshakeEngine, SampleStore
from .errors import WorkbenchError
from .persistence import Store


def create_app(base_dir: str | None = None, db_path: str | None = None) -> Flask:
    base_dir = base_dir or os.getcwd()
    db_path = db_path or os.path.join(base_dir, "data", "workbench.sqlite3")
    app = Flask(__name__)
    app.config["BASE_DIR"] = base_dir
    samples = SampleStore(base_dir)
    store = Store(db_path)
    engine = HandshakeEngine(store, samples)
    app.config["ENGINE"] = engine
    app.config["STORE"] = store

    def error_response(exc: WorkbenchError):
        status_mapping = {
            "E_CONFLICTING_SESSION": 409,
            "E_BUDGET_EXCEEDED": 409,
            "E_INVALID_TRANSITION": 409,
            "E_CHAIN_INVALID": 422,
            "E_SAMPLE_NOT_FOUND": 404,
            "E_RECOVERY_ABORTED": 409,
            "E_DOWNGRADE_FORBIDDEN": 422,
        }
        response = jsonify(
            {"ok": False, "error_code": exc.code, "message": exc.message,
             "details": exc.details}
        )
        response.status_code = status_mapping.get(exc.code, 400)
        return response

    @app.get("/api/health")
    def health():
        return jsonify({"ok": True, "service": "tls-workbench", "bind": "127.0.0.1"})

    @app.post("/api/sessions")
    def create_session():
        body = request.get_json(force=True)
        session_id = body["session_id"]
        chain_id = body["chain_id"]
        params = body["params"]
        try:
            session = engine.create_session(session_id, chain_id, params)
        except WorkbenchError as exc:
            return error_response(exc)
        return jsonify({"ok": True, "session": session}), 201

    @app.post("/api/sessions/<session_id>/advance")
    def advance(session_id: str):
        body = request.get_json(silent=True) or {}
        config = HandshakeConfig(
            continue_on_failure=bool(body.get("continue_on_failure", False)),
            downgrade_items=tuple(body.get("downgrade_items", [])),
        )
        try:
            result = engine.get_or_advance(
                session_id,
                request_params=body.get("params"),
                config=config,
            )
        except WorkbenchError as exc:
            return error_response(exc)
        return jsonify({"ok": True, "diagnostic": _jsonable(result)})

    @app.get("/api/sessions/<session_id>")
    def get_session(session_id: str):
        session = store.get_session(session_id)
        if session is None:
            return error_response(
                WorkbenchError("E_SAMPLE_NOT_FOUND", "unknown session",
                               {"session_id": session_id})
            )
        session["params"] = json.loads(session.pop("params_json"))
        return jsonify({"ok": True, "session": _jsonable(session)})

    @app.get("/api/sessions")
    def list_sessions():
        return jsonify({"ok": True, "sessions": store.list_sessions()})

    @app.get("/api/sessions/<session_id>/diagnostic")
    def get_diagnostic(session_id: str):
        try:
            result = engine._assemble_from_persistence(session_id)
        except WorkbenchError as exc:
            return error_response(exc)
        return jsonify({"ok": True, "diagnostic": _jsonable(result)})

    @app.post("/api/recover")
    def recover():
        report = store.recover_pending()
        return jsonify({"ok": True, "recovery_report": report})

    @app.get("/api/chains/<chain_id>")
    def get_chain(chain_id: str):
        try:
            chain = samples.load(chain_id)
        except WorkbenchError as exc:
            return error_response(exc)
        public = {key: value for key, value in chain.items() if not key.startswith("_")}
        return jsonify({"ok": True, "chain": _jsonable(public)})

    @app.get("/api/numeric/audit")
    def numeric_audit():
        sample_path = os.path.join(base_dir, "samples", "handshake_samples.json")
        with open(sample_path, encoding="utf-8") as handle:
            params = json.load(handle)["samples"][0]
        estimates = [
            budget.stage_estimate(
                stage,
                params["stage_costs_ms_fixed"][stage],
                params["chain_overhead_ms_fixed"][stage],
            )
            for stage in (
                "CLIENT_HELLO", "SERVER_HELLO", "CERTIFICATE",
                "CERT_VERIFY", "FINISHED",
            )
        ]
        return jsonify(
            {"ok": True, "audit": budget.naive_vs_compensated_audit(estimates)}
        )

    return app


def _jsonable(value):
    return json.loads(json.dumps(value, default=str, sort_keys=True))
