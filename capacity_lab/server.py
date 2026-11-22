"""HTTP API for plan editing, round execution and read-only evidence queries."""

from __future__ import annotations

import argparse
import json
import re
import threading
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from .engine import CapacityEngine
from .errors import AppError, BadRequest, NotFound
from .models import normalize_plan
from .repository import Database


class RunManager:
    def __init__(self, engine: CapacityEngine) -> None:
        self.engine = engine
        self._threads: dict[str, threading.Thread] = {}
        self._errors: dict[str, str] = {}
        self._lock = threading.Lock()

    def start(self, plan_id: str, version: int | None, *, asynchronous: bool) -> dict[str, Any]:
        run = self.engine.db.create_run(plan_id, version, mode="FULL")
        run_id = run["run_id"]
        if not asynchronous:
            return self.engine.execute_run(run_id)
        thread = threading.Thread(
            target=self._background_run,
            args=(run_id,),
            name=f"capacity-run-{run_id}",
            daemon=True,
        )
        with self._lock:
            self._threads[run_id] = thread
        thread.start()
        return self.engine.db.get_run(run_id)

    def _background_run(self, run_id: str) -> None:
        try:
            self.engine.execute_run(run_id)
        except Exception:
            with self._lock:
                self._errors[run_id] = traceback.format_exc(limit=8)
        finally:
            with self._lock:
                self._threads.pop(run_id, None)

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "active_threads": sorted(self._threads),
                "background_error_count": len(self._errors),
            }


class CapacityApplication:
    def __init__(self, db_path: str | Path) -> None:
        self.db = Database(db_path)
        self.db.initialise()
        self.recovery_report = self.db.recover()
        self.engine = CapacityEngine(self.db)
        self.runs = RunManager(self.engine)

    def dispatch(self, method: str, raw_path: str, body: dict[str, Any] | None) -> tuple[int, dict[str, Any]]:
        parsed = urlparse(raw_path)
        path = parsed.path
        query = parse_qs(parsed.query)
        if method == "GET" and path == "/api/health":
            return 200, {
                "ok": True,
                "data": {
                    "service": "capacity-lab",
                    "db": str(self.db.path),
                    "recovery": {
                        "report_id": self.recovery_report.get("report_id"),
                        "unresolved_intents": self.recovery_report.get("unresolved_intents"),
                        "rounds_aborted": len(self.recovery_report.get("rounds_aborted", [])),
                    },
                    "runs": self.runs.status(),
                },
            }

        match = re.fullmatch(r"/api/plans/([^/]+)", path)
        if match:
            plan_id = unquote(match.group(1))
            if method == "PUT":
                if body is None:
                    raise BadRequest("plan body is required")
                return 200, {"ok": True, "data": self.db.submit_plan(plan_id, body)}
            if method == "GET":
                version = _optional_int(query, "version")
                return 200, {"ok": True, "data": self.db.get_plan(plan_id, version)}

        match = re.fullmatch(r"/api/plans/([^/]+)/versions", path)
        if match and method == "GET":
            return 200, {"ok": True, "data": self.db.list_plan_versions(unquote(match.group(1)))}

        match = re.fullmatch(r"/api/plans/([^/]+)/validate", path)
        if match and method == "POST":
            plan_id = unquote(match.group(1))
            if body is None:
                raise BadRequest("plan body is required")
            normalized = normalize_plan(body, plan_id)
            return 200, {
                "ok": True,
                "data": {
                    "valid": True,
                    "stored_thresholds": normalized["stored_thresholds"],
                    "numeric_contract": normalized["numeric_contract"],
                },
            }

        match = re.fullmatch(r"/api/plans/([^/]+)/runs", path)
        if match and method == "POST":
            plan_id = unquote(match.group(1))
            body = body or {}
            version = _body_optional_int(body, "version")
            asynchronous = bool(body.get("async", False))
            run = self.runs.start(plan_id, version, asynchronous=asynchronous)
            return 200, {"ok": True, "data": run}

        match = re.fullmatch(r"/api/plans/([^/]+)/stages/(\d+)/rounds", path)
        if match and method == "POST":
            plan_id = unquote(match.group(1))
            stage_index = int(match.group(2))
            body = body or {}
            version = _body_optional_int(body, "version")
            run_id = body.get("run_id")
            if run_id is not None and not isinstance(run_id, str):
                raise BadRequest("run_id must be a string")
            round_result = self.engine.trigger_manual_stage(
                plan_id,
                stage_index,
                version=version,
                run_id=run_id,
            )
            return 200, {"ok": True, "data": round_result}

        match = re.fullmatch(r"/api/plans/([^/]+)/report", path)
        if match and method == "GET":
            plan_id = unquote(match.group(1))
            version = _optional_int(query, "version")
            latest = self.db.latest_run_for_plan(plan_id, version)
            if latest is None:
                raise NotFound("no run exists for this plan", details={"plan_id": plan_id})
            return 200, {"ok": True, "data": latest}

        match = re.fullmatch(r"/api/runs/([^/]+)", path)
        if match and method == "GET":
            return 200, {"ok": True, "data": self.db.get_run(unquote(match.group(1)))}

        match = re.fullmatch(r"/api/rounds/([^/]+)", path)
        if match and method == "GET":
            include_samples = query.get("include_samples", ["true"])[-1].lower() not in {
                "0",
                "false",
                "no",
            }
            return 200, {
                "ok": True,
                "data": self.db.get_round(unquote(match.group(1)), include_samples=include_samples),
            }

        if method == "GET" and path == "/api/recovery/latest":
            return 200, {"ok": True, "data": self.db.get_latest_recovery()}
        if method == "GET" and path == "/api/recovery/reports":
            limit = int(query.get("limit", ["20"])[-1])
            return 200, {"ok": True, "data": self.db.list_recovery_reports(limit)}
        if method == "POST" and path == "/api/admin/recover":
            return 200, {"ok": True, "data": self.db.recover()}

        raise NotFound("route not found", details={"method": method, "path": path})


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "CapacityLab/1.0"
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        self._dispatch("GET")

    def do_POST(self) -> None:
        self._dispatch("POST")

    def do_PUT(self) -> None:
        self._dispatch("PUT")

    def _dispatch(self, method: str) -> None:
        try:
            body = self._read_json_body() if method in {"POST", "PUT"} else None
            application = getattr(self.server, "application")
            status, payload = application.dispatch(method, self.path, body)
            self._write_json(status, payload)
        except AppError as exc:
            self._write_json(exc.http_status, exc.to_dict())
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True
        except Exception:
            error = AppError(
                "E_INTERNAL",
                "internal server error",
                http_status=500,
                details={"traceback": traceback.format_exc(limit=5)},
            )
            self._write_json(500, error.to_dict())

    def _read_json_body(self) -> dict[str, Any] | None:
        length_text = self.headers.get("Content-Length")
        if not length_text:
            return None
        try:
            length = int(length_text)
        except ValueError as exc:
            raise BadRequest("invalid Content-Length") from exc
        if length == 0:
            return None
        if length > 4_000_000:
            raise BadRequest("request body is too large")
        raw = self.rfile.read(length)
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BadRequest("request body must be valid JSON") from exc
        if not isinstance(value, dict):
            raise BadRequest("request body must be a JSON object")
        return value

    def _write_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(
            payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.close_connection = True

    def log_message(self, fmt: str, *args: Any) -> None:
        return


class CapacityHttpServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], application: CapacityApplication) -> None:
        self.application = application
        super().__init__(address, ApiHandler)


def _optional_int(query: dict[str, list[str]], name: str) -> int | None:
    values = query.get(name)
    if not values:
        return None
    try:
        return int(values[-1])
    except ValueError as exc:
        raise BadRequest(f"query parameter {name} must be an integer") from exc


def _body_optional_int(body: dict[str, Any], name: str) -> int | None:
    value = body.get(name)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise BadRequest(f"body field {name} must be an integer")
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description="Capacity Lab HTTP API")
    parser.add_argument("--db", default="capacity_lab.db")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18080)
    args = parser.parse_args()
    application = CapacityApplication(args.db)
    server = CapacityHttpServer((args.host, args.port), application)
    recovery = application.recovery_report
    print(
        f"capacity lab listening on http://{args.host}:{server.server_port}; "
        f"startup recovery aborted {len(recovery.get('rounds_aborted', []))} rounds",
        flush=True,
    )
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()

