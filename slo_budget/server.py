"""Local JSON HTTP API using only Python's standard library."""

from __future__ import annotations

import argparse
import json
import os
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .errors import SloError
from .models import dumps_json
from .service import SloService


MAX_BODY_BYTES = 2 * 1024 * 1024


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes", "on"}
    return bool(value)


class SloHttpServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, service: SloService, *, verbose: bool = False):
        super().__init__(address, SloRequestHandler)
        self.service = service
        self.verbose = verbose


class SloRequestHandler(BaseHTTPRequestHandler):
    server: SloHttpServer
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args) -> None:
        if self.server.verbose:
            super().log_message(fmt, *args)

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = (dumps_json(payload) + "\n").encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, error: SloError) -> None:
        self._send_json(error.http_status, error.to_dict())

    def _read_json_body(self) -> dict[str, Any]:
        raw_length = self.headers.get("Content-Length")
        if raw_length is None:
            raise SloError("E_REQUEST_INVALID", "Content-Length is required")
        try:
            length = int(raw_length)
        except ValueError as exc:
            raise SloError("E_REQUEST_INVALID", "Content-Length must be an integer") from exc
        if length < 0 or length > MAX_BODY_BYTES:
            raise SloError(
                "E_REQUEST_INVALID",
                "request body size is outside allowed bounds",
                details={"length": length, "maximum": MAX_BODY_BYTES},
            )
        raw = self.rfile.read(length)
        if not raw:
            return {}
        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SloError(
                "E_REQUEST_INVALID",
                "request body must be valid JSON",
                details={"error": str(exc)},
            ) from exc
        if not isinstance(body, dict):
            raise SloError("E_REQUEST_INVALID", "request body must be a JSON object")
        return body

    def _query(self) -> dict[str, str]:
        parsed = urlparse(self.path)
        return {
            key: values[-1]
            for key, values in parse_qs(parsed.query, keep_blank_values=True).items()
            if values
        }

    def _path(self) -> str:
        return urlparse(self.path).path.rstrip("/") or "/"

    def do_GET(self) -> None:  # noqa: N802 - http.server API
        try:
            path = self._path()
            query = self._query()
            service = self.server.service
            if path == "/health":
                self._send_json(
                    HTTPStatus.OK,
                    {"ok": True, "status": "healthy", "active_snapshot": service.storage.active_snapshot()},
                )
                return
            if path == "/v1/compatibility":
                self._send_json(HTTPStatus.OK, service.compatibility())
                return
            if path == "/v1/state":
                self._send_json(HTTPStatus.OK, service.state())
                return
            if path == "/v1/windows":
                self._send_json(
                    HTTPStatus.OK,
                    service.windows(
                        service=query.get("service"),
                        sli_type=query.get("sli_type"),
                    ),
                )
                return
            if path == "/v1/imports":
                self._send_json(HTTPStatus.OK, service.imports(limit=int(query.get("limit", "100"))))
                return
            if path.startswith("/v1/imports/"):
                batch_id = path.rsplit("/", 1)[-1]
                self._send_json(
                    HTTPStatus.OK,
                    {"ok": True, "report": service.storage.get_import_report(batch_id)},
                )
                return
            if path == "/v1/alerts":
                self._send_json(HTTPStatus.OK, service.alerts(limit=int(query.get("limit", "100"))))
                return
            if path == "/v1/recovery":
                self._send_json(HTTPStatus.OK, service.recovery())
                return
            if path in {"/v1/query", "/v1/sli"}:
                for required in ("service", "sli_type", "as_of_ms"):
                    if not query.get(required):
                        raise SloError(
                            "E_REQUEST_INVALID",
                            f"query parameter {required} is required",
                            details={"parameter": required},
                        )
                try:
                    as_of_ms = int(query["as_of_ms"])
                    int(query.get("limit", "100"))
                except ValueError as exc:
                    raise SloError(
                        "E_REQUEST_INVALID",
                        "as_of_ms and limit must be integers",
                    ) from exc
                result = service.query(
                    service=query["service"],
                    sli_type=query["sli_type"],
                    as_of_ms=as_of_ms,
                    events_path=query.get("events_path"),
                    slo_target=query.get("slo_target"),
                    min_samples=query.get("min_samples"),
                    persist_alert=_as_bool(query.get("persist_alert"), False),
                )
                self._send_json(HTTPStatus.OK, result)
                return
            raise SloError(
                "E_NOT_FOUND",
                "HTTP endpoint not found",
                details={"path": path, "method": "GET"},
            )
        except SloError as error:
            self._send_error(error)
        except Exception as exc:  # noqa: BLE001 - stable API error envelope
            self._send_error(
                SloError(
                    "E_INTERNAL",
                    "unexpected server error",
                    details={"error": str(exc)},
                )
            )

    def do_POST(self) -> None:  # noqa: N802 - http.server API
        try:
            path = self._path()
            body = self._read_json_body()
            service = self.server.service
            if path == "/v1/preflight":
                require_path(body)
                result = service.preflight(
                    body["path"],
                    expected_format=body.get("format", "auto"),
                )
                self._send_json(HTTPStatus.OK, result)
                return
            if path == "/v1/import":
                require_path(body)
                result = service.import_file(
                    body["path"],
                    expected_format=body.get("format", "auto"),
                    truncation_rule=body.get("truncation_rule", "floor"),
                    batch_id=body.get("batch_id"),
                )
                self._send_json(HTTPStatus.OK, result)
                return
            if path == "/v1/snapshots/replace":
                require_path(body)
                result = service.replace_snapshot(
                    body["path"],
                    expected_format=body.get("format", "auto"),
                    truncation_rule=body.get("truncation_rule", "floor"),
                    batch_id=body.get("batch_id"),
                )
                self._send_json(HTTPStatus.OK, result)
                return
            if path == "/v1/export":
                require_path(body)
                result = service.export_snapshot(
                    body["path"],
                    file_format=body.get("format", "auto"),
                    endian=body.get("endian", "little"),
                )
                self._send_json(HTTPStatus.OK, result)
                return
            if path == "/v1/analyze":
                if not body.get("events_path"):
                    raise SloError("E_REQUEST_INVALID", "events_path is required")
                for required in ("service", "sli_type", "as_of_ms"):
                    if body.get(required) in {None, ""}:
                        raise SloError(
                            "E_REQUEST_INVALID",
                            f"request field {required} is required",
                        )
                try:
                    as_of_ms = int(body["as_of_ms"])
                except (TypeError, ValueError) as exc:
                    raise SloError("E_REQUEST_INVALID", "as_of_ms must be an integer") from exc
                result = service.analyze_events_file(
                    events_path=body["events_path"],
                    service=body["service"],
                    sli_type=body["sli_type"],
                    as_of_ms=as_of_ms,
                    slo_target=body.get("slo_target"),
                    min_samples=body.get("min_samples"),
                    persist_alert=_as_bool(body.get("persist_alert"), False),
                )
                self._send_json(HTTPStatus.OK, result)
                return
            raise SloError(
                "E_NOT_FOUND",
                "HTTP endpoint not found",
                details={"path": path, "method": "POST"},
            )
        except SloError as error:
            self._send_error(error)
        except Exception as exc:  # noqa: BLE001 - stable API error envelope
            self._send_error(
                SloError(
                    "E_INTERNAL",
                    "unexpected server error",
                    details={"error": str(exc)},
                )
            )


def require_path(body: dict[str, Any]) -> None:
    if not body.get("path"):
        raise SloError("E_REQUEST_INVALID", "path is required")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the local SLO error-budget HTTP API")
    parser.add_argument("--db", required=True, help="SQLite database path")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--fixed-clock-ms", type=int, default=None)
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.fixed_clock_ms is not None:
        os.environ["SLO_FIXED_NOW_MS"] = str(args.fixed_clock_ms)
    service = SloService(args.db)
    server = SloHttpServer((args.host, args.port), service, verbose=args.verbose)
    host, port = server.server_address[:2]
    print(
        dumps_json(
            {
                "ok": True,
                "event": "server_started",
                "url": f"http://{host}:{port}",
                "db": str(Path(args.db).resolve()),
                "active_snapshot": service.storage.active_snapshot(),
            }
        ),
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        server.server_close()
        service.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
