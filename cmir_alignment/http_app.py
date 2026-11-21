"""Local HTTP API using only the Python standard library."""

from __future__ import annotations

import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .canonical import canonical_bytes
from .engine import RetrievalEngine
from .errors import (
    CMIRError,
    E_INCREMENTAL_MISMATCH,
    E_INVALID_BITWIDTH,
    E_INVALID_QUERY,
    E_INVALID_STAGE,
    E_MISSING_INPUT,
    E_NUMERIC,
    E_PROJECTION_INVALID,
    E_QUANTIZATION_ZERO_SCALE,
    E_STALE_CHECKPOINT,
)


ERROR_STATUS = {
    E_INVALID_QUERY: HTTPStatus.BAD_REQUEST,
    E_MISSING_INPUT: HTTPStatus.UNPROCESSABLE_ENTITY,
    E_PROJECTION_INVALID: HTTPStatus.UNPROCESSABLE_ENTITY,
    E_INVALID_BITWIDTH: HTTPStatus.UNPROCESSABLE_ENTITY,
    E_QUANTIZATION_ZERO_SCALE: HTTPStatus.UNPROCESSABLE_ENTITY,
    E_NUMERIC: HTTPStatus.UNPROCESSABLE_ENTITY,
    E_STALE_CHECKPOINT: HTTPStatus.CONFLICT,
    E_INVALID_STAGE: HTTPStatus.CONFLICT,
    E_INCREMENTAL_MISMATCH: HTTPStatus.CONFLICT,
}


def make_handler(engine: RetrievalEngine) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "CMIR/0.1"

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            # Keep the demo terminal clean and responses deterministic.
            return

        def _send(self, status: int, payload: dict[str, Any]) -> None:
            body = canonical_bytes(payload) + b"\n"
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self) -> dict[str, Any]:
            length_header = self.headers.get("Content-Length", "0")
            try:
                length = int(length_header)
            except ValueError as exc:
                raise CMIRError(E_INVALID_QUERY, "invalid Content-Length") from exc
            if length > 16 * 1024 * 1024:
                raise CMIRError(E_INVALID_QUERY, "request body too large")
            raw = self.rfile.read(length) if length else b"{}"
            try:
                value = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise CMIRError(E_INVALID_QUERY, "request body is not valid JSON", reason=str(exc)) from exc
            if not isinstance(value, dict):
                raise CMIRError(E_INVALID_QUERY, "request body must be a JSON object")
            return value

        def _dispatch(self, method: str) -> None:
            path = urlparse(self.path).path.rstrip("/") or "/"
            try:
                if method == "GET" and path == "/health":
                    self._send(HTTPStatus.OK, {"ok": True, "service": "cmir-alignment"})
                elif method == "GET" and path == "/status":
                    self._send(HTTPStatus.OK, engine.status())
                elif method == "GET" and path == "/checkpoints/audit":
                    self._send(HTTPStatus.OK, engine.checkpoints.audit())
                elif method == "POST" and path == "/import":
                    body = self._read_json()
                    if "dataset_dir" not in body:
                        raise CMIRError(E_INVALID_QUERY, "dataset_dir is required")
                    self._send(HTTPStatus.OK, engine.import_dataset(body["dataset_dir"]))
                elif method == "POST" and path == "/config":
                    body = self._read_json()
                    self._send(HTTPStatus.OK, engine.set_config(patch=body))
                elif method == "POST" and path == "/build":
                    body = self._read_json()
                    self._send(
                        HTTPStatus.OK,
                        engine.build(stop_after_units=body.get("stop_after_units")),
                    )
                elif method == "POST" and path == "/search":
                    body = self._read_json()
                    self._send(
                        HTTPStatus.OK,
                        engine.search(
                            mode=body.get("mode"),
                            query=body.get("query"),
                            query_item_id=body.get("query_item_id"),
                            query_image_file=body.get("query_image_file"),
                            query_vector=body.get("query_vector"),
                            k=body.get("k"),
                        ),
                    )
                elif method == "POST" and path == "/update-shard":
                    body = self._read_json()
                    if "shard_id" not in body:
                        raise CMIRError(E_INVALID_QUERY, "shard_id is required")
                    self._send(
                        HTTPStatus.OK,
                        engine.update_shard(
                            body["shard_id"],
                            stop_after_units=body.get("stop_after_units"),
                        ),
                    )
                elif method == "POST" and path == "/stop":
                    self._send(HTTPStatus.OK, engine.request_stop())
                elif method == "POST" and path == "/resume":
                    body = self._read_json()
                    self._send(
                        HTTPStatus.OK,
                        engine.resume(stop_after_units=body.get("stop_after_units")),
                    )
                elif method == "POST" and path == "/verify":
                    self._send(HTTPStatus.OK, engine.verify_full_consistency())
                elif method == "POST" and path == "/export":
                    body = self._read_json()
                    output_dir = body.get("output_dir")
                    if not output_dir:
                        raise CMIRError(E_INVALID_QUERY, "output_dir is required")
                    self._send(
                        HTTPStatus.OK,
                        engine.export_report(
                            output_dir,
                            queries=body.get("queries"),
                        ),
                    )
                elif method == "POST" and path == "/checkpoints/recover":
                    self._send(HTTPStatus.OK, engine.checkpoints.repair_head())
                elif path == "/":
                    self._send(
                        HTTPStatus.OK,
                        {
                            "ok": True,
                            "service": "cmir-alignment",
                            "routes": [
                                "POST /import",
                                "POST /config",
                                "POST /build",
                                "POST /search",
                                "POST /update-shard",
                                "GET /status",
                                "POST /stop",
                                "POST /resume",
                                "POST /verify",
                                "POST /export",
                                "GET /checkpoints/audit",
                                "POST /checkpoints/recover",
                            ],
                        },
                    )
                else:
                    self._send(
                        HTTPStatus.NOT_FOUND,
                        {"ok": False, "error": {"code": "E_NOT_FOUND", "message": "unknown route"}},
                    )
            except CMIRError as exc:
                payload = exc.to_dict()
                self._send(ERROR_STATUS.get(exc.code, HTTPStatus.INTERNAL_SERVER_ERROR), payload)
            except Exception as exc:  # pragma: no cover - defensive HTTP boundary
                self._send(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {
                        "ok": False,
                        "error": {
                            "code": "E_INTERNAL",
                            "message": str(exc),
                            "details": {},
                        },
                    },
                )

        def do_GET(self) -> None:  # noqa: N802
            self._dispatch("GET")

        def do_POST(self) -> None:  # noqa: N802
            self._dispatch("POST")

    return Handler


def create_server(
    workspace: str | Path,
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
) -> ThreadingHTTPServer:
    engine = RetrievalEngine(workspace)
    return ThreadingHTTPServer((host, port), make_handler(engine))


def serve(workspace: str | Path, *, host: str = "127.0.0.1", port: int = 8765) -> None:
    server = create_server(workspace, host=host, port=port)
    try:
        print(canonical_bytes({"ok": True, "listening": f"http://{host}:{port}"}).decode("utf-8"), flush=True)
        server.serve_forever()
    finally:
        server.server_close()
