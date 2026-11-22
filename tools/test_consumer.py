#!/usr/bin/env python3
"""Controllable local consumer endpoint for middleware verification.

Examples:
  python tools/test_consumer.py --port 8091
  POST /consume?behavior=success&auto_ack=true
  POST /consume?behavior=refuse
  POST /consume?behavior=hang&sleep_ms=3000
"""

from __future__ import annotations

import argparse
import json
import threading
import time
from collections import Counter
from typing import Any

import requests
from flask import Flask, jsonify, request
from werkzeug.serving import make_server


def _truthy(value: str | None) -> bool:
    return str(value or "").lower() in {"1", "true", "yes", "on"}


def create_consumer_app() -> Flask:
    app = Flask(__name__)
    app.json.ensure_ascii = False
    state: dict[str, Any] = {
        "lock": threading.Lock(),
        "received": [],
        "behaviors": Counter(),
        "active_hangs": 0,
        "active_requests": 0,
        "max_active_requests": 0,
        "ack_results": [],
    }
    app.config["CONSUMER_STATE"] = state

    @app.get("/health")
    def health():
        with state["lock"]:
            return jsonify(
                {
                    "ok": True,
                    "received": len(state["received"]),
                    "active_hangs": state["active_hangs"],
                    "active_requests": state["active_requests"],
                    "max_active_requests": state["max_active_requests"],
                    "behaviors": dict(state["behaviors"]),
                }
            )

    @app.post("/state/reset")
    def reset():
        with state["lock"]:
            state["received"].clear()
            state["behaviors"].clear()
            state["ack_results"].clear()
            state["active_hangs"] = 0
            state["active_requests"] = 0
            state["max_active_requests"] = 0
        return jsonify({"ok": True})

    @app.get("/state")
    def get_state():
        with state["lock"]:
            return jsonify(
                {
                    "ok": True,
                    "received": list(state["received"]),
                    "behaviors": dict(state["behaviors"]),
                    "active_hangs": state["active_hangs"],
                    "active_requests": state["active_requests"],
                    "max_active_requests": state["max_active_requests"],
                    "ack_results": list(state["ack_results"]),
                }
            )

    def _consume(path_behavior: str | None = None):
        payload = request.get_json(silent=True) or {}
        behavior = (
            request.args.get("behavior")
            or path_behavior
            or payload.get("behavior")
            or "success"
        )
        delivered_at_ms = int(time.time() * 1000)
        with state["lock"]:
            state["behaviors"][behavior] += 1
            record = {
                "received_at_ms": delivered_at_ms,
                "behavior": behavior,
                "delivery_id": payload.get("delivery_id"),
                "round_id": payload.get("round_id"),
                "consumer_id": payload.get("consumer_id"),
                "queue_name": (payload.get("message") or {}).get("queue_name"),
                "message_id": (payload.get("message") or {}).get("message_id"),
                "attempt": (payload.get("message") or {}).get("attempt"),
                "mode": payload.get("mode"),
            }
            state["received"].append(record)

        delay_ms = int(request.args.get("delay_ms", "0"))
        if behavior != "hang" and delay_ms > 0:
            time.sleep(delay_ms / 1000.0)

        if behavior == "hang":
            sleep_ms = int(request.args.get("sleep_ms", "3000"))
            with state["lock"]:
                state["active_hangs"] += 1
            try:
                time.sleep(max(0, sleep_ms) / 1000.0)
            finally:
                with state["lock"]:
                    state["active_hangs"] -= 1
            return jsonify({"ok": True, "behavior": "hang", "slept_ms": sleep_ms})

        if behavior == "refuse":
            status = int(request.args.get("refuse_status", "409"))
            return jsonify({"ok": False, "behavior": "refuse"}), status

        if behavior == "error":
            return jsonify({"ok": False, "behavior": "error"}), 500

        ack_result = None
        ack_error = None
        if _truthy(request.args.get("auto_ack")):
            ack_url = payload.get("ack_url")
            ack_body = {
                "request_id": f"consumer-ack-{payload.get('delivery_id')}",
                "delivery_id": payload.get("delivery_id"),
                "lease_token": payload.get("lease_token"),
                "consumer_id": payload.get("consumer_id"),
                "mode": payload.get("mode"),
            }
            try:
                response = requests.post(ack_url, json=ack_body, timeout=3.0)
                try:
                    ack_result = response.json()
                except ValueError:
                    ack_result = {"http_status": response.status_code, "text": response.text}
                if response.status_code >= 300:
                    ack_error = ack_result
            except requests.RequestException as exc:
                ack_error = {"code": "ACK_CALLBACK_FAILED", "message": str(exc)}
            with state["lock"]:
                state["ack_results"].append(
                    {"delivery_id": payload.get("delivery_id"), "ack": ack_result, "error": ack_error}
                )
        return jsonify(
            {
                "ok": True,
                "behavior": "success",
                "delivery_id": payload.get("delivery_id"),
                "auto_ack": _truthy(request.args.get("auto_ack")),
                "ack_result": ack_result,
                "ack_error": ack_error,
            }
        )

    @app.post("/consume")
    @app.post("/consume/<path:path_behavior>")
    def consume(path_behavior: str | None = None):
        with state["lock"]:
            state["active_requests"] += 1
            state["max_active_requests"] = max(
                state["max_active_requests"], state["active_requests"]
            )
        try:
            return _consume(path_behavior)
        finally:
            with state["lock"]:
                state["active_requests"] -= 1

    return app


def serve(port: int, host: str = "127.0.0.1") -> None:
    app = create_consumer_app()
    server = make_server(host, port, app, threaded=True)
    print(json.dumps({"ok": True, "consumer_server": f"http://{host}:{port}", "threaded": True}))
    server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8091)
    args = parser.parse_args()
    if args.host not in {"127.0.0.1", "::1", "localhost"}:
        raise SystemExit("this test endpoint only binds to a loopback address")
    serve(args.port, args.host)


if __name__ == "__main__":
    main()
