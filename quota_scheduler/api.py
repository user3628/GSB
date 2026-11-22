"""Flask application exposing the adjudication engine."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, request

from .errors import (
    E_BATCH_NOT_FOUND,
    E_MODEL_UNKNOWN,
    E_NODE_UNKNOWN,
    E_QUOTA_GROUP_UNKNOWN,
    E_REQUEST_NOT_FOUND,
    E_ROUND_NOT_FOUND,
    E_SPEC_INVALID,
    E_VALIDATION,
    SchedulerError,
)
from .service import QuotaScheduler


def create_app(
    database_path: str | Path | None = None,
    *,
    sample_dir: str | Path | None = None,
    bootstrap: bool = False,
    load_requests: bool = False,
    auto_admit: bool = False,
    recover_on_start: bool = True,
    abort_running_on_start: bool = True,
) -> Flask:
    db_path = str(
        database_path
        or os.environ.get("QUOTA_SCHEDULER_DB")
        or Path.cwd() / "quota_scheduler.db"
    )
    service = QuotaScheduler(db_path)
    if bootstrap:
        samples = sample_dir or os.environ.get("QUOTA_SCHEDULER_SAMPLES")
        if not samples:
            raise ValueError("sample_dir is required when bootstrap=True")
        service.bootstrap(samples, load_requests=load_requests, auto_admit=auto_admit)

    startup_recovery = None
    if recover_on_start:
        # A daemon restart cannot safely continue batches that were RUNNING in
        # a dead process.  The report is persisted and remains queryable.
        startup_recovery = service.recover(abort_running=abort_running_on_start)

    app = Flask(__name__)
    app.config["JSON_AS_ASCII"] = False
    app.json.ensure_ascii = False
    app.extensions["quota_scheduler"] = service
    app.extensions["startup_recovery"] = startup_recovery

    def body() -> dict[str, Any]:
        value = request.get_json(silent=True)
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise SchedulerError(E_VALIDATION, "request body must be a JSON object")
        return value

    @app.errorhandler(SchedulerError)
    def handle_scheduler_error(exc: SchedulerError):
        status = _http_status(exc.code)
        return jsonify({"ok": False, "error": exc.to_dict()}), status

    @app.errorhandler(404)
    def handle_not_found(_exc):
        return jsonify({"ok": False, "error": {"code": "E_ROUTE_NOT_FOUND",
                                                 "message": "route not found"}}), 404

    @app.errorhandler(Exception)
    def handle_unexpected(exc: Exception):
        if app.config.get("TESTING"):
            raise exc
        return jsonify(
            {
                "ok": False,
                "error": {
                    "code": "E_INTERNAL",
                    "message": "unexpected server error",
                    "details": {"type": type(exc).__name__},
                },
            }
        ), 500

    @app.get("/")
    def index():
        return jsonify(
            {
                "ok": True,
                "name": "local-gpu-quota-scheduler",
                "version": "1.0.0",
                "database": service.database_path,
                "endpoints": [
                    "GET /health",
                    "GET /api/summary",
                    "GET /api/quota",
                    "POST /api/requests",
                    "GET /api/requests",
                    "POST /api/requests/<id>/admit",
                    "POST /api/requests/<id>/preempt",
                    "POST /api/batches/organize",
                    "POST /api/scheduler/rounds",
                    "POST /api/batches/<id>/complete",
                    "POST /api/recovery",
                ],
            }
        )

    @app.get("/health")
    def health():
        summary = service.state_summary()
        return jsonify(
            {
                "ok": summary["invariants"]["ok"],
                "database": summary["database"],
                "invariants": summary["invariants"],
            }
        ), (200 if summary["invariants"]["ok"] else 503)

    @app.get("/api/summary")
    def summary():
        return jsonify(service.state_summary())

    @app.post("/api/bootstrap")
    def bootstrap_route():
        data = body()
        samples = data.get("sample_dir") or sample_dir
        if not samples:
            raise SchedulerError(E_VALIDATION, "sample_dir is required")
        result = service.bootstrap(
            samples,
            load_requests=bool(data.get("load_requests", False)),
            auto_admit=bool(data.get("auto_admit", False)),
        )
        return jsonify(result)

    @app.post("/api/requests")
    def submit_request():
        data = request.get_json(silent=True)
        if isinstance(data, list):
            results = []
            for item in data:
                results.append(
                    service.submit_request(
                        item,
                        auto_admit=request.args.get("auto_admit", "0")
                        in {"1", "true", "yes"},
                    )
                )
            return jsonify({"ok": True, "count": len(results), "requests": results}), 201
        if not isinstance(data, dict):
            raise SchedulerError(E_VALIDATION, "request body must be a JSON object")
        auto_admit = request.args.get("auto_admit", "0") in {"1", "true", "yes"}
        result = service.submit_request(data, auto_admit=auto_admit)
        return jsonify(result), (201 if result.get("created") else 200)

    @app.get("/api/requests")
    def list_requests():
        return jsonify(service.list_requests(request.args.get("status")))

    @app.get("/api/requests/<request_id>")
    def get_request(request_id: str):
        return jsonify(service.get_request(request_id))

    @app.get("/api/requests/<request_id>/transitions")
    def get_transitions(request_id: str):
        return jsonify(service.get_transitions(request_id))

    @app.post("/api/requests/<request_id>/admit")
    def admit_request(request_id: str):
        data = body()
        allow_preemption = data.get("allow_preemption")
        return jsonify(
            service.admit_request(request_id, allow_preemption=allow_preemption)
        )

    @app.post("/api/requests/<request_id>/transition")
    def transition_request(request_id: str):
        data = body()
        return jsonify(
            service.transition_request(
                request_id,
                str(data.get("to_state", "")),
                str(data.get("reason", "")),
            )
        )

    @app.post("/api/requests/<request_id>/preempt")
    def preempt_request(request_id: str):
        data = body()
        return jsonify(
            service.preempt_request(
                request_id,
                str(data.get("reason", "manual_api_preemption")),
                by_request_id=data.get("by_request_id"),
            )
        )

    @app.post("/api/batches/organize")
    def organize_batches():
        data = body()
        strict = data.get("strict")
        return jsonify(
            service.organize_batches(
                strategy=data.get("strategy"),
                strict=None if strict is None else bool(strict),
                request_ids=data.get("request_ids"),
            )
        )

    @app.get("/api/batches")
    def list_batches():
        return jsonify(service.list_batches(request.args.get("status")))

    @app.get("/api/batches/<batch_id>")
    def get_batch(batch_id: str):
        return jsonify(service.get_batch(batch_id))

    @app.post("/api/batches/<batch_id>/complete")
    def complete_batch(batch_id: str):
        return jsonify(service.complete_batch(batch_id))

    @app.post("/api/scheduler/rounds")
    def schedule_round():
        data = body()
        return jsonify(
            service.schedule_round(
                round_no=data.get("round_no"),
                max_parallel_batches=data.get("max_parallel_batches"),
                time_budget_millis=data.get("time_budget_millis", data.get("time_budget")),
                queue_capacity=data.get("queue_capacity"),
            )
        )

    @app.get("/api/scheduler/rounds/<int:round_no>")
    def get_round(round_no: int):
        return jsonify(service.get_round(round_no))

    @app.post("/api/scheduler/rounds/<int:round_no>/complete")
    def complete_round(round_no: int):
        return jsonify(service.complete_round(round_no))

    @app.get("/api/quota")
    def quota():
        return jsonify(service.quota_snapshot())

    @app.get("/api/intents")
    def intents():
        try:
            limit = int(request.args.get("limit", "200"))
        except ValueError:
            raise SchedulerError(E_VALIDATION, "limit must be an integer") from None
        return jsonify(service.list_intents(request.args.get("status"), limit=limit))

    @app.post("/api/recovery")
    def recover():
        data = body()
        return jsonify(service.recover(abort_running=bool(data.get("abort_running", False))))

    @app.get("/api/recovery")
    def recovery_status():
        return jsonify(service.recovery_status())

    return app


def _http_status(code: str) -> int:
    if code in {E_REQUEST_NOT_FOUND, E_BATCH_NOT_FOUND, E_ROUND_NOT_FOUND}:
        return 404
    if code in {E_SPEC_INVALID, E_VALIDATION, E_QUOTA_GROUP_UNKNOWN,
                E_MODEL_UNKNOWN, E_NODE_UNKNOWN}:
        return 400
    return 409


__all__ = ["create_app"]

