from __future__ import annotations

import argparse
import json
import re
import socket
import sys
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from .errors import EngineError
from .service import EngineService


_REQUEST_COLLECTION = re.compile(r"^/v1/requests/([^/]+)$")
_REQUEST_ACTION = re.compile(r"^/v1/requests/([^/]+)/([a-z-]+)$")
_MAX_BODY_BYTES = 2 * 1024 * 1024


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "LocalJoinShuffle/1.0"
    protocol_version = "HTTP/1.1"

    @property
    def service(self) -> EngineService:
        return self.server.service  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write(
            "%s - - [%s] %s\n" % (self.client_address[0], self.log_date_time_string(), fmt % args)
        )

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(
            payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _ok(self, data: Any, status: int = 200) -> None:
        self._send_json(status, {"ok": True, "data": data})

    def _error(self, error: EngineError) -> None:
        payload = error.to_dict()
        payload["ok"] = False
        self._send_json(error.http_status, payload)

    def _read_json(self) -> dict[str, Any]:
        raw_length = self.headers.get("Content-Length")
        if raw_length is None:
            raise EngineError("E_INVALID_REQUEST", "Content-Length is required")
        try:
            length = int(raw_length)
        except ValueError as exc:
            raise EngineError("E_INVALID_REQUEST", "invalid Content-Length") from exc
        if length < 0 or length > _MAX_BODY_BYTES:
            raise EngineError(
                "E_INVALID_REQUEST",
                "request body is too large",
                {"max_bytes": _MAX_BODY_BYTES, "actual_bytes": length},
            )
        raw = self.rfile.read(length)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise EngineError("E_INVALID_REQUEST", "request body must be UTF-8 JSON") from exc
        if not isinstance(payload, dict):
            raise EngineError("E_INVALID_REQUEST", "request body must be a JSON object")
        return payload

    @staticmethod
    def _required(payload: dict[str, Any], name: str) -> Any:
        if name not in payload:
            raise EngineError(
                "E_INVALID_REQUEST", f"missing required field: {name}", {"field": name}
            )
        return payload[name]

    @staticmethod
    def _bool_value(payload: dict[str, Any], name: str, default: bool) -> bool:
        value = payload.get(name, default)
        if not isinstance(value, bool):
            raise EngineError("E_INVALID_REQUEST", f"{name} must be boolean", {"field": name})
        return value

    def _route(self) -> tuple[str, dict[str, list[str]], dict[str, Any] | None]:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        body = self._read_json() if self.command == "POST" else None
        return unquote(parsed.path), query, body

    def do_GET(self) -> None:  # noqa: N802
        try:
            path, query, _ = self._route()
            if path == "/v1/health":
                self._ok({"status": "ok", "loopback_only": True})
                return
            if path == "/v1/datasets":
                self._ok({"datasets": self.service.repo.list_active_datasets()})
                return
            if path == "/v1/recovery/report":
                limit = int(query.get("limit", ["100"])[0])
                self._ok(self.service.recovery_report(limit=limit))
                return
            match = _REQUEST_COLLECTION.fullmatch(path)
            if match:
                self._ok(self.service.get_request_status(match.group(1)))
                return
            match = _REQUEST_ACTION.fullmatch(path)
            if match:
                request_id, action = match.group(1), match.group(2)
                if query.get("wait", ["0"])[0] in {"1", "true", "yes"}:
                    timeout = float(query.get("timeout", ["60"])[0])
                    self.service.wait_for_request(
                        request_id, timeout=timeout, raise_terminal_error=False
                    )
                if action == "partitions":
                    self._ok(self.service.get_partitions(request_id))
                elif action == "result":
                    self._ok(self.service.get_aggregates(request_id))
                elif action == "precision":
                    self._ok(self.service.get_precision_contract(request_id))
                elif action == "partials":
                    self._ok(self.service.get_partials(request_id))
                else:
                    raise EngineError(
                        "E_REQUEST_NOT_FOUND", "API route not found", {"path": path}
                    )
                return
            raise EngineError("E_REQUEST_NOT_FOUND", "API route not found", {"path": path})
        except EngineError as error:
            self._error(error)
        except (ValueError, TypeError) as error:
            self._error(
                EngineError("E_INVALID_REQUEST", "invalid query parameter", {"error": str(error)})
            )
        except Exception as error:  # pragma: no cover - final safety net
            self._error(
                EngineError(
                    "E_INTERNAL",
                    "unexpected server error",
                    {"error_type": type(error).__name__, "error": str(error)},
                )
            )

    def do_POST(self) -> None:  # noqa: N802
        try:
            path, _, payload = self._route()
            assert payload is not None
            if path == "/v1/snapshots/load":
                result = self.service.load_snapshots(
                    dataset_id=self._required(payload, "dataset_id"),
                    left_path=self._required(payload, "left_path"),
                    right_path=self._required(payload, "right_path"),
                )
                self._ok(result)
                return
            if path == "/v1/shuffle":
                wait = self._bool_value(payload, "wait", True)
                timeout = payload.get("timeout", 60.0)
                result = self.service.submit_shuffle(
                    self._required(payload, "request_id"),
                    self._required(payload, "dataset_id"),
                    partitions=self._required(payload, "partitions"),
                    skew_limit=payload.get("skew_limit", 0.35),
                    skew_action=payload.get("skew_action", "report"),
                    wait=wait,
                    timeout=timeout,
                )
                self._ok(result, HTTPStatus.OK if wait else HTTPStatus.ACCEPTED)
                return
            if path == "/v1/join":
                wait = self._bool_value(payload, "wait", True)
                timeout = payload.get("timeout", 60.0)
                precision = payload.get("precision", {})
                if precision is None:
                    precision = {}
                if not isinstance(precision, dict):
                    raise EngineError(
                        "E_INVALID_REQUEST", "precision must be an object", {"field": "precision"}
                    )
                result = self.service.submit_join(
                    self._required(payload, "request_id"),
                    self._required(payload, "dataset_id"),
                    partitions=self._required(payload, "partitions"),
                    join_type=payload.get("join_type", "inner"),
                    measure=payload.get("measure", "left"),
                    skew_limit=payload.get("skew_limit", 0.35),
                    skew_action=payload.get("skew_action", "report"),
                    precision_mode=precision.get(
                        "mode", payload.get("precision_mode", "auto")
                    ),
                    target_abs_error=precision.get(
                        "target_abs_error", payload.get("target_abs_error")
                    ),
                    max_operations=precision.get(
                        "max_operations", payload.get("max_operations")
                    ),
                    wait=wait,
                    timeout=timeout,
                )
                self._ok(result, HTTPStatus.OK if wait else HTTPStatus.ACCEPTED)
                return
            raise EngineError("E_REQUEST_NOT_FOUND", "API route not found", {"path": path})
        except EngineError as error:
            self._error(error)
        except (ValueError, TypeError) as error:
            self._error(
                EngineError("E_INVALID_REQUEST", "invalid request value", {"error": str(error)})
            )
        except Exception as error:  # pragma: no cover - final safety net
            self._error(
                EngineError(
                    "E_INTERNAL",
                    "unexpected server error",
                    {"error_type": type(error).__name__, "error": str(error)},
                )
            )


class LocalApiServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], service: EngineService):
        self.service = service
        if ":" in address[0]:
            self.address_family = socket.AF_INET6
        super().__init__(address, ApiHandler)


def serve(
    *, db_path: str | Path, host: str = "127.0.0.1", port: int = 8765
) -> None:
    if host not in {"127.0.0.1", "localhost", "::1"}:
        raise EngineError(
            "E_INVALID_REQUEST",
            "the API may bind only to a loopback address",
            {"host": host},
        )
    if not 1 <= int(port) <= 65535:
        raise EngineError("E_INVALID_REQUEST", "port must be in 1..65535", {"port": port})
    service = EngineService(db_path)
    server = LocalApiServer((host, int(port)), service)
    display_host = f"[{host}]" if ":" in host else host
    print(f"Local Join/Shuffle API listening on http://{display_host}:{port}", flush=True)
    print(f"SQLite database: {Path(db_path).resolve()}", flush=True)
    print("Loopback only; no external network or accounts are used.", flush=True)
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        print("\nStopping server...", flush=True)
    finally:
        server.server_close()
        service.close(wait=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Local deterministic Join/Shuffle HTTP API")
    parser.add_argument("--db", default="artifacts/engine.db", help="SQLite database path")
    parser.add_argument("--host", default="127.0.0.1", help="loopback host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8765, help="TCP port (default: 8765)")
    args = parser.parse_args(argv)
    try:
        serve(db_path=args.db, host=args.host, port=args.port)
    except EngineError as error:
        print(json.dumps(error.to_dict(), ensure_ascii=False), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
