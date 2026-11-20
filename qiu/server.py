"""Dependency-free local HTTP interface built on http.server.HTTPServer."""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .engine import QueryEngine
from .errors import QueryError, RequestError
from .experiment import experiment_bytes, run_experiment
from .io import canonical_json

MAX_REQUEST_BYTES = 1_048_576


def create_server(
    engine: QueryEngine,
    *,
    eval_path: str | Path,
    host: str = "127.0.0.1",
    port: int = 8765,
) -> HTTPServer:
    class Handler(_QIRequestHandler):
        pass

    Handler.engine = engine
    Handler.eval_path = Path(eval_path)
    return HTTPServer((host, port), Handler)


class _QIRequestHandler(BaseHTTPRequestHandler):
    engine: QueryEngine
    eval_path: Path

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path == "/health":
            self._send(200, {"ok": True, "service": "qiu"})
        elif path == "/v1/version":
            self._send(200, self.engine.version_status())
        else:
            self._send_error(404, "E_NOT_FOUND", "route not found")

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        try:
            body = self._read_json_body()
            if path == "/v1/parse":
                sample, faults = _extract_query_payload(body)
                self._send(200, self.engine.parse(sample, faults))
            elif path == "/v1/experiment":
                seed = body.get("seed", 17)
                bootstrap_samples = body.get("bootstrap_samples", 200)
                if not isinstance(seed, int) or isinstance(seed, bool):
                    raise RequestError("seed must be an integer")
                if not isinstance(bootstrap_samples, int) or isinstance(bootstrap_samples, bool) or bootstrap_samples < 0:
                    raise RequestError("bootstrap_samples must be a non-negative integer")
                summary = run_experiment(
                    self.engine,
                    eval_path=self.eval_path,
                    seed=seed,
                    bootstrap_samples=bootstrap_samples,
                )
                self._send_bytes(200, experiment_bytes(summary))
            elif path == "/v1/reload":
                self._send(200, self.engine.refresh(force=False))
            else:
                self._send_error(404, "E_NOT_FOUND", "route not found")
        except QueryError as exc:
            self._send(400, exc.to_dict())
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            self._send(
                400,
                QueryError("E_BAD_REQUEST", "request", "request body is not valid JSON", details={"error": str(exc)}).to_dict(),
            )
        except ValueError as exc:
            self._send(400, QueryError("E_BAD_REQUEST", "request", str(exc)).to_dict())
        except Exception as exc:  # pragma: no cover - defensive HTTP boundary
            self._send(
                500,
                QueryError("E_INTERNAL", "server", "unexpected internal error", details={"error": type(exc).__name__}).to_dict(),
            )

    def _read_json_body(self) -> dict[str, Any]:
        length_header = self.headers.get("Content-Length")
        if length_header is None:
            raise RequestError("Content-Length is required")
        try:
            length = int(length_header)
        except ValueError as exc:
            raise RequestError("Content-Length must be an integer") from exc
        if length < 0 or length > MAX_REQUEST_BYTES:
            raise RequestError("request body is too large", details={"max_bytes": MAX_REQUEST_BYTES})
        data = self.rfile.read(length)
        value = json.loads(data.decode("utf-8")) if data else {}
        if not isinstance(value, dict):
            raise RequestError("JSON request body must be an object")
        return value

    def _send(self, status: int, value: Any) -> None:
        self._send_bytes(status, (canonical_json(value) + "\n").encode("utf-8"))

    def _send_bytes(self, status: int, payload: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    def _send_error(self, status: int, code: str, message: str) -> None:
        self._send(status, QueryError(code, "http", message).to_dict())

    def log_message(self, format: str, *args: Any) -> None:
        return


def _extract_query_payload(body: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    if "query" in body:
        sample = body["query"]
        faults = body.get("faults", {})
        if not isinstance(sample, dict) or not isinstance(faults, dict):
            raise RequestError("query must be an object and faults must be an object")
        return sample, faults
    sample = dict(body)
    faults = sample.pop("faults", {})
    if not isinstance(faults, dict):
        raise RequestError("faults must be an object")
    return sample, faults
