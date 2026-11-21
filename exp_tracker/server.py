"""Loopback-only HTTP interface around :class:`ExperimentStore`.

A :class:`http.server.ThreadingHTTPServer` serves many concurrent readers;
writes are serialized inside the store. All error responses use the unified
business error body ``{"error": {"code", "message", "context"}}``. The server
binds to 127.0.0.1 only and never reaches the network beyond loopback.
"""

from __future__ import annotations

import json
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse

from . import negotiation
from .timeutil import ms_to_us, normalize_batch, TRUNCATE
from .errors import ErrorCode, TrackerError
from .models import Run
from .store import ExperimentStore


def _json_response(handler: BaseHTTPRequestHandler, status: int, body: Dict[str, Any]) -> None:
    encoded = json.dumps(body, sort_keys=True).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(encoded)))
    handler.end_headers()
    handler.wfile.write(encoded)


class TrackerHandler(BaseHTTPRequestHandler):
    server_version = "ExpTracker/1.0"

    # Quieter logging; the demo prints structured results instead.
    def log_message(self, fmt: str, *args) -> None:  # noqa: A003
        return

    @property
    def store(self) -> ExperimentStore:
        return self.server.store  # type: ignore[attr-defined]

    def _read_json(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise TrackerError(ErrorCode.INVALID_REQUEST, "request body is not valid JSON",
                               {"detail": exc.msg})
        if not isinstance(data, dict):
            raise TrackerError(ErrorCode.INVALID_REQUEST, "request body must be a JSON object", {})
        return data

    def _request_id(self, data: Dict[str, Any]) -> Optional[str]:
        return data.get("request_id") or self.headers.get("X-Request-Id")

    def _handle_error(self, exc: TrackerError) -> None:
        _json_response(self, exc.http_status, exc.to_body())

    # ------------------------------------------------------------- routing
    def do_GET(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            query = parse_qs(parsed.query)
            if path == "/health":
                _json_response(self, 200, {"status": "ok", "revision": self.store.revision()})
            elif path == "/runs":
                _json_response(self, 200, {"runs": [r.to_dict() for r in self.store.list_runs()]})
            elif path.startswith("/runs/"):
                run_id = path.split("/", 2)[2]
                run = self.store.get_run(run_id)
                if run is None:
                    raise TrackerError(ErrorCode.NOT_FOUND, "run not found",
                                       {"run_id": run_id}, http_status=404)
                _json_response(self, 200, run.to_dict())
            elif path == "/summaries":
                _json_response(self, 200, {"summaries": self.store.list_summaries()})
            elif path == "/params":
                key = query.get("key", [None])[0]
                if key:
                    _json_response(self, 200, {"param_key": key,
                                               "run_ids": self.store.find_runs_by_param(key)})
                else:
                    _json_response(self, 200, {"param_index": self.store.list_param_index()})
            elif path == "/reports":
                _json_response(self, 200, {"reports": self.store.list_import_reports()})
            elif path.startswith("/reports/"):
                report_id = path.split("/", 2)[2]
                _json_response(self, 200, self.store.get_import_report(report_id))
            elif path == "/recovery":
                report = self.store.recovery_report
                _json_response(self, 200, report.to_dict() if report else {})
            else:
                raise TrackerError(ErrorCode.NOT_FOUND, "unknown endpoint",
                                   {"path": path}, http_status=404)
        except TrackerError as exc:
            self._handle_error(exc)

    def do_POST(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            data = self._read_json()
            request_id = self._request_id(data)
            if path == "/runs":
                run = Run.from_dict(data["run"])
                expected = data.get("expected_revision")
                result = self.store.submit_run(
                    run, request_id=request_id,
                    expected_revision=int(expected) if expected is not None else None,
                )
                _json_response(self, 200, result)
            elif path == "/precheck":
                result = negotiation.precheck(data["path"]).to_dict()
                _json_response(self, 200, result)
            elif path == "/imports":
                runs, neg = negotiation.decode_with_negotiation(data["path"])
                report_id = data.get("report_id") or f"rep-{uuid.uuid4().hex[:12]}"
                report = self.store.import_runs(
                    runs, neg.to_dict(), report_id, request_id=request_id
                )
                _json_response(self, 200, report)
            elif path == "/snapshots/export":
                result = self.store.export_snapshot(
                    data["path"], generated_at_us=data.get("generated_at_us")
                )
                _json_response(self, 200, result)
            elif path == "/snapshots/load":
                result = self.store.load_snapshot(data["path"], request_id=request_id)
                _json_response(self, 200, result)
            elif path == "/cascade":
                result = self.store.cascade_rebuild(data["experiment"], request_id=request_id)
                _json_response(self, 200, result)
            elif path == "/normalize":
                rule = data.get("rule", TRUNCATE)
                records = []
                for item in data.get("records", []):
                    time_us = ms_to_us(item["time_ms"]) if item.get("unit") == "ms" \
                        else int(item["time_us"])
                    records.append((item["run_id"], time_us))
                rounding = normalize_batch(records, rule)
                _json_response(self, 200, {
                    "rule": rule,
                    "internal_unit": "us",
                    "records": [record.to_dict() for record in rounding],
                })
            else:
                raise TrackerError(ErrorCode.NOT_FOUND, "unknown endpoint",
                                   {"path": path}, http_status=404)
        except TrackerError as exc:
            self._handle_error(exc)
        except KeyError as exc:
            self._handle_error(TrackerError(
                ErrorCode.INVALID_REQUEST, "missing required request field",
                {"field": str(ex).strip("'")}))


class TrackerHttpServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, host: str, port: int, store: ExperimentStore):
        if host not in ("127.0.0.1", "localhost"):
            raise ValueError("the tracker HTTP server is loopback-only")
        super().__init__((host, port), TrackerHandler)
        self.store = store
        self._start_lock = threading.Lock()

    def serve_in_thread(self) -> threading.Thread:
        thread = threading.Thread(target=self.serve_forever, name="tracker-http", daemon=True)
        thread.start()
        return thread


def build_server(db_path: str, host: str = "127.0.0.1", port: int = 0) -> TrackerHttpServer:
    store = ExperimentStore(db_path)
    return TrackerHttpServer(host, port, store)
