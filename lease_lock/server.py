from __future__ import annotations

import argparse
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

from .engine import LeaseEngine
from .errors import ApiError


MAX_BODY_BYTES = 8 * 1024 * 1024


class LeaseHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, handler, engine: LeaseEngine) -> None:
        super().__init__(address, handler)
        self.engine = engine


class LeaseRequestHandler(BaseHTTPRequestHandler):
    server: LeaseHTTPServer
    protocol_version = "HTTP/1.1"
    server_version = "LocalLeaseLock/1.0"

    def do_GET(self) -> None:  # noqa: N802
        self._dispatch("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch("POST")

    def do_PUT(self) -> None:  # noqa: N802
        self._dispatch("PUT")

    def do_DELETE(self) -> None:  # noqa: N802
        self._send_error(ApiError("E_INVALID_REQUEST", "method not supported"), 405)

    def _dispatch(self, method: str) -> None:
        self._cached_body: dict[str, Any] | None = None
        try:
            parsed = urlsplit(self.path)
            path = parsed.path.rstrip("/") or "/"
            query = parse_qs(parsed.query)
            engine = self.server.engine

            if method in {"POST", "PUT"}:
                self._read_json()

            if method == "GET" and path == "/healthz":
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "service": "local-lease-lock",
                        "snapshot_version": engine.snapshot_version,
                    },
                )
                return

            if method == "GET" and path == "/v1/snapshot":
                if "version" in query:
                    version = int(query["version"][0])
                    self._send_json(200, engine.get_snapshot_version(version))
                else:
                    self._send_json(200, engine.get_current_snapshot())
                return

            if method == "GET" and path == "/v1/recovery/report":
                self._send_json(200, engine.get_recovery_report())
                return

            if method == "GET" and path.startswith("/v1/locks/"):
                key = unquote(path[len("/v1/locks/") :])
                self._send_json(200, engine.get_lease(key))
                return

            if method == "GET" and path.startswith("/v1/resources/"):
                key = unquote(path[len("/v1/resources/") :])
                self._send_json(200, engine.get_resource(key))
                return

            if method == "POST" and path == "/v1/locks/acquire":
                body = self._read_json()
                result = engine.acquire(
                    body.get("lock_key", ""),
                    body.get("holder", ""),
                    body.get("ttl_ms"),
                    body.get("dependencies", []),
                    debug_fault=_debug_fault(body),
                )
                self._send_json(200, result)
                return

            if method == "POST" and path == "/v1/locks/renew":
                body = self._read_json()
                result = engine.renew(
                    body.get("lock_key", ""),
                    body.get("holder", ""),
                    body.get("lease_version"),
                    body.get("ttl_ms"),
                    debug_fault=_debug_fault(body),
                )
                self._send_json(200, result)
                return

            if method == "POST" and path == "/v1/locks/release":
                body = self._read_json()
                result = engine.release(
                    body.get("lock_key", ""),
                    body.get("holder", ""),
                    body.get("lease_version"),
                    debug_fault=_debug_fault(body),
                )
                self._send_json(200, result)
                return

            if method == "POST" and path == "/v1/snapshot/commit":
                body = self._read_json()
                result = engine.commit_increment(
                    body.get("base_version"),
                    body.get("changes", []),
                    debug_fault=_debug_fault(body),
                )
                self._send_json(200, result)
                return

            if method in {"POST", "PUT"} and path == "/v1/snapshot/replace":
                body = self._read_json()
                result = engine.replace_snapshot(
                    body.get("base_version"),
                    body.get("leases", []),
                    debug_fault=_debug_fault(body),
                )
                self._send_json(200, result)
                return

            if method == "POST" and path == "/v1/admin/clock":
                body = self._read_json()
                if "advance_ms" in body:
                    amount = body["advance_ms"]
                elif "offset_ms" in body:
                    amount = body["offset_ms"]
                else:
                    raise ApiError("E_INVALID_REQUEST", "advance_ms is required")
                self._send_json(200, engine.advance_clock(amount))
                return

            if method == "POST" and path == "/v1/admin/sweep":
                self._send_json(200, engine.sweep())
                return

            raise ApiError("E_INVALID_REQUEST", f"unknown route: {method} {path}")
        except ApiError as exc:
            current_version = None
            try:
                current_version = self.server.engine.snapshot_version
            except Exception:
                pass
            self._send_error(exc, exc.http_status, current_version)
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
            self._send_error(ApiError("E_INVALID_REQUEST", f"invalid JSON/input: {exc}"), 400)
        except Exception as exc:
            current_version = None
            try:
                current_version = self.server.engine.snapshot_version
            except Exception:
                pass
            self._send_error(
                ApiError("E_INTERNAL", f"unexpected server error: {exc}", current_version),
                500,
                current_version,
            )

    def _read_json(self) -> dict[str, Any]:
        cached = getattr(self, "_cached_body", None)
        if cached is not None:
            return cached
        content_length = int(self.headers.get("Content-Length", "0"))
        if content_length <= 0:
            self._cached_body = {}
            return {}
        if content_length > MAX_BODY_BYTES:
            raise ApiError("E_INVALID_REQUEST", "request body is too large")
        raw = self.rfile.read(content_length)
        parsed = json.loads(raw.decode("utf-8"))
        if not isinstance(parsed, dict):
            raise ApiError("E_INVALID_REQUEST", "request body must be a JSON object")
        self._cached_body = parsed
        return parsed

    def _send_error(
        self,
        exc: ApiError,
        status: int,
        current_version: int | None = None,
    ) -> None:
        self.close_connection = True
        self._send_json(status, exc.to_dict(current_version))

    def _send_json(self, status: int, body: dict[str, Any]) -> None:
        raw = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write(
            "%s - - [%s] %s\n" % (self.address_string(), self.log_date_time_string(), fmt % args)
        )


def _debug_fault(body: dict[str, Any]) -> str | None:
    value = body.get("_debug_fault") or body.get("debug_fault")
    if value is None:
        return None
    if not isinstance(value, str):
        raise ApiError("E_INVALID_REQUEST", "_debug_fault must be a string")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Local SQLite lease-lock middleware")
    parser.add_argument("--db", default="runtime/leases.db", help="SQLite database path")
    parser.add_argument("--host", default="127.0.0.1", help="loopback bind address")
    parser.add_argument("--port", type=int, default=8765, help="HTTP port (0 chooses a free port)")
    parser.add_argument("--clock-offset-ms", type=int, default=0, help="initial logical clock offset")
    parser.add_argument(
        "--enable-faults",
        action="store_true",
        default=os.environ.get("LEASE_ENABLE_FAULTS") == "1",
        help="enable _debug_fault crash injection for the recovery demo",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    engine = LeaseEngine(
        args.db,
        clock_offset_ms=args.clock_offset_ms,
        enable_faults=args.enable_faults,
    )
    server = LeaseHTTPServer((args.host, args.port), LeaseRequestHandler, engine)
    port = server.server_address[1]
    sys.stdout.write(f"local-lease-lock listening on http://{args.host}:{port} db={args.db}\n")
    sys.stdout.flush()
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

