"""Flask HTTP API for local producers, consumers and verification."""

from __future__ import annotations

import threading
from typing import Any

from flask import Flask, jsonify, request
from werkzeug.exceptions import HTTPException

from .constants import DELIVERY_MODES
from .errors import E_INVALID_ARGUMENT, E_NOT_FOUND, MWError
from .scheduler import Scheduler
from .store import SQLiteStore
from .utils import new_id, now_ms, validate_loopback_url


def _body() -> dict[str, Any]:
    if not request.is_json:
        raise MWError(E_INVALID_ARGUMENT, "Content-Type must be application/json")
    value = request.get_json(silent=False)
    if not isinstance(value, dict):
        raise MWError(E_INVALID_ARGUMENT, "request body must be a JSON object")
    return value


def _request_id(body: dict[str, Any]) -> str:
    value = body.get("request_id")
    if value is not None and (not isinstance(value, str) or not value):
        raise MWError(E_INVALID_ARGUMENT, "request_id must be a non-empty string")
    return value or new_id("req_")


def create_app(
    db_path: str,
    *,
    ack_base_url: str = "http://127.0.0.1:8080",
    recover_on_start: bool = False,
) -> Flask:
    validate_loopback_url(ack_base_url, "ack_base_url")
    store = SQLiteStore(db_path)
    scheduler = Scheduler(store)
    app = Flask(__name__)
    app.json.ensure_ascii = False
    app.config["MQMW_STORE"] = store
    app.config["MQMW_SCHEDULER"] = scheduler
    app.config["MQMW_ACK_BASE_URL"] = ack_base_url
    app.extensions["mqmw"] = {
        "store": store,
        "scheduler": scheduler,
        "reaper_stop": None,
        "reaper_thread": None,
        "last_recovery": None,
    }
    if recover_on_start:
        app.extensions["mqmw"]["last_recovery"] = store.recover()

    @app.errorhandler(MWError)
    def handle_mw_error(error: MWError):
        return jsonify(error.to_dict()), error.http_status

    @app.errorhandler(404)
    def handle_404(_error):
        return jsonify({"ok": False, "error": {"code": E_NOT_FOUND, "message": "route not found"}}), 404

    @app.errorhandler(HTTPException)
    def handle_http_error(error: HTTPException):
        code = "E_NOT_FOUND" if error.code == 404 else f"E_HTTP_{error.code}"
        return (
            jsonify(
                {
                    "ok": False,
                    "error": {
                        "code": code,
                        "message": error.description,
                    },
                }
            ),
            error.code or 500,
        )

    @app.errorhandler(Exception)
    def handle_unexpected(error: Exception):
        return (
            jsonify(
                {
                    "ok": False,
                    "error": {
                        "code": "E_INTERNAL",
                        "message": f"{type(error).__name__}: {error}",
                    },
                }
            ),
            500,
        )

    @app.get("/v1/health")
    def health():
        return jsonify(
            {
                "ok": True,
                "service": "mqmw",
                "time_ms": now_ms(),
                "revision": store.revision(),
                "round_in_progress": scheduler._round_guard.locked(),
            }
        )

    @app.post("/v1/queues")
    def register_queue():
        body = _body()
        request_id = _request_id(body)
        result, replayed = store.execute_request(
            request_id,
            "register_queue",
            lambda: store.register_queue(
                body.get("queue_name"),
                body.get("priority", 100),
                request_id=request_id,
            ),
        )
        return jsonify({"request_id": request_id, "replayed": replayed, **result})

    @app.post("/v1/consumers")
    def register_consumer():
        body = _body()
        request_id = _request_id(body)
        endpoint = validate_loopback_url(body.get("endpoint"), "endpoint")
        queues = body.get("queues")
        if not isinstance(queues, list):
            raise MWError(E_INVALID_ARGUMENT, "queues must be a list")
        result, replayed = store.execute_request(
            request_id,
            "register_consumer",
            lambda: store.register_consumer(
                body.get("consumer_id"),
                body.get("group_name"),
                endpoint,
                queues,
                enabled=bool(body.get("enabled", True)),
                request_id=request_id,
            ),
        )
        return jsonify({"request_id": request_id, "replayed": replayed, **result})

    @app.get("/v1/consumers")
    def list_consumers():
        return jsonify({"ok": True, "consumers": store.list_consumers()})

    @app.post("/v1/enqueue")
    def enqueue():
        body = _body()
        request_id = _request_id(body)
        result, replayed = store.execute_request(
            request_id,
            "enqueue",
            lambda: store.enqueue(
                body.get("queue_name"), body.get("messages"), request_id=request_id
            ),
        )
        return jsonify({"request_id": request_id, "replayed": replayed, **result})

    @app.post("/v1/rounds")
    def run_round():
        body = _body()
        request_id = _request_id(body)
        mode = body.get("mode")
        if mode not in DELIVERY_MODES:
            raise MWError(E_INVALID_ARGUMENT, f"mode must be one of {sorted(DELIVERY_MODES)}")
        config = body.get("config", {})
        if not isinstance(config, dict):
            raise MWError(E_INVALID_ARGUMENT, "config must be a JSON object")
        ack_base = body.get("ack_base_url") or app.config["MQMW_ACK_BASE_URL"]
        validate_loopback_url(ack_base, "ack_base_url")
        result, replayed = store.execute_request(
            request_id,
            "round",
            lambda: scheduler.run_round(
                mode,
                config,
                request_id=request_id,
                ack_base_url=ack_base,
                consumer_id=body.get("consumer_id"),
            ),
        )
        loaded = store.get_round(result["round_id"], include_traces=True)
        result["traces"] = loaded["traces"]
        result["request_id"] = request_id
        result["replayed"] = replayed
        http_status = int(result.pop("http_status", 200))
        return jsonify(result), http_status

    @app.post("/v1/claims")
    @app.post("/v1/subscribe")
    def claim():
        body = _body()
        request_id = _request_id(body)
        mode = body.get("mode")
        if mode not in DELIVERY_MODES:
            raise MWError(E_INVALID_ARGUMENT, f"mode must be one of {sorted(DELIVERY_MODES)}")

        def execute():
            delivery = store.claim(
                body.get("queue_name"),
                body.get("consumer_id"),
                mode,
                request_id=request_id,
                defer_commit=False,
            )
            if delivery is None:
                return {
                    "ok": True,
                    "delivery": None,
                    "reason": "NO_ELIGIBLE_MESSAGE_OR_LEASE_HELD",
                }
            return {"ok": True, "delivery": delivery}

        result, replayed = store.execute_request(request_id, "claim", execute)
        result["request_id"] = request_id
        result["replayed"] = replayed
        return jsonify(result)

    @app.post("/v1/acks")
    def ack():
        body = _body()
        request_id = _request_id(body)
        delivery_id = body.get("delivery_id")
        if isinstance(delivery_id, bool) or not isinstance(delivery_id, int):
            raise MWError(E_INVALID_ARGUMENT, "delivery_id must be an integer")
        if not isinstance(body.get("lease_token"), str) or not body.get("lease_token"):
            raise MWError(E_INVALID_ARGUMENT, "lease_token must be a non-empty string")
        if not isinstance(body.get("consumer_id"), str) or not body.get("consumer_id"):
            raise MWError(E_INVALID_ARGUMENT, "consumer_id must be a non-empty string")
        result, replayed = store.execute_request(
            request_id,
            "ack",
            lambda: store.ack(
                delivery_id,
                body.get("lease_token"),
                body.get("consumer_id"),
                body.get("mode"),
                request_id=request_id,
            ),
        )
        result["request_id"] = request_id
        result["replayed"] = replayed
        return jsonify(result)

    @app.post("/v1/reap")
    def reap():
        body = _body()
        request_id = _request_id(body)
        result, replayed = store.execute_request(
            request_id, "reap", lambda: store.reap_expired(body.get("limit", 1000))
        )
        result["request_id"] = request_id
        result["replayed"] = replayed
        return jsonify(result)

    @app.get("/v1/queues/<queue_name>")
    def queue_summary(queue_name: str):
        return jsonify(store.get_queue_summary(queue_name))

    @app.get("/v1/messages/<queue_name>/<message_id>")
    def message(queue_name: str, message_id: str):
        return jsonify({"ok": True, "message": store.get_message(queue_name, message_id)})

    @app.post("/v1/messages/<queue_name>/<message_id>/transitions")
    def transition(queue_name: str, message_id: str):
        body = _body()
        request_id = _request_id(body)
        result, replayed = store.execute_request(
            request_id,
            "transition",
            lambda: store.transition_state(
                queue_name,
                message_id,
                body.get("to_state"),
                consumer_id=body.get("consumer_id"),
                request_id=request_id,
            ),
        )
        result["request_id"] = request_id
        result["replayed"] = replayed
        return jsonify(result)

    @app.get("/v1/snapshot")
    def snapshot():
        queue_name = request.args.get("queue")
        return jsonify(store.snapshot(queue_name))

    @app.get("/v1/intents")
    def intents():
        return jsonify(
            {
                "ok": True,
                "intents": store.list_intents(
                    state=request.args.get("state"), limit=int(request.args.get("limit", "200"))
                ),
            }
        )

    @app.get("/v1/rounds/<round_id>")
    def get_round(round_id: str):
        return jsonify({"ok": True, "round": store.get_round(round_id, include_traces=True)})

    @app.get("/v1/recovery")
    def recovery():
        report = store.get_latest_recovery_report()
        startup = app.extensions["mqmw"].get("last_recovery")
        return jsonify({"ok": True, "latest": report or startup})

    return app


def start_reaper(app: Flask, interval_ms: int = 500) -> threading.Event:
    """Start a daemon reaper for visibility timeouts. Intended for CLI serve."""

    state = app.extensions["mqmw"]
    if state.get("reaper_thread") is not None:
        return state["reaper_stop"]
    stop = threading.Event()
    store: SQLiteStore = state["store"]

    def loop() -> None:
        while not stop.wait(max(0.01, interval_ms / 1000.0)):
            try:
                store.reap_expired()
            except Exception:
                # A background reaper must not kill the API process. Explicit
                # /v1/reap remains available for diagnostics.
                continue

    thread = threading.Thread(target=loop, name="mqmw-reaper", daemon=True)
    state["reaper_stop"] = stop
    state["reaper_thread"] = thread
    thread.start()
    return stop


def stop_reaper(app: Flask) -> None:
    state = app.extensions["mqmw"]
    stop = state.get("reaper_stop")
    if stop is not None:
        stop.set()
