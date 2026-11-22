from __future__ import annotations

import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .engine import Engine, import_inputs
from .errors import (
    E_CHECKPOINT_CORRUPT,
    E_CONFIG_INVALID,
    E_EMPTY_BATCH,
    E_INPUT_CONTRACT,
    E_INVALID_STAGE,
    E_MANIFEST_INVALID,
    E_NUMERIC,
    E_REFERENCE_INVALID,
    E_STALE_CHECKPOINT,
    E_STALE_VERSION,
    BusinessError,
)

ERROR_STATUS = {
    E_STALE_CHECKPOINT: HTTPStatus.CONFLICT,
    E_STALE_VERSION: HTTPStatus.CONFLICT,
    E_INVALID_STAGE: HTTPStatus.CONFLICT,
    E_EMPTY_BATCH: HTTPStatus.UNPROCESSABLE_ENTITY,
    E_REFERENCE_INVALID: HTTPStatus.UNPROCESSABLE_ENTITY,
    E_NUMERIC: HTTPStatus.UNPROCESSABLE_ENTITY,
    E_MANIFEST_INVALID: HTTPStatus.BAD_REQUEST,
    E_CONFIG_INVALID: HTTPStatus.BAD_REQUEST,
    E_INPUT_CONTRACT: HTTPStatus.BAD_REQUEST,
    E_CHECKPOINT_CORRUPT: HTTPStatus.INTERNAL_SERVER_ERROR,
}


class DensityTraceServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self, server_address: tuple[str, int], workspace_path: str | Path
    ) -> None:
        self.workspace_path = Path(workspace_path).resolve()
        super().__init__(server_address, DensityTraceHandler)


class DensityTraceHandler(BaseHTTPRequestHandler):
    server_version = "density-trace/0.1"

    @property
    def workspace_path(self) -> Path:
        return self.server.workspace_path

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _send_json(self, status: int, payload: Any) -> None:
        body = json.dumps(
            payload, ensure_ascii=False, allow_nan=False, sort_keys=True
        ).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length > 2 * 1024 * 1024:
            raise BusinessError(E_INPUT_CONTRACT, "request body is too large")
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            value = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise BusinessError(
                E_INPUT_CONTRACT, f"request body is not valid JSON: {exc}"
            ) from exc
        if not isinstance(value, dict):
            raise BusinessError(E_INPUT_CONTRACT, "request body must be a JSON object")
        return value

    def _engine(self) -> Engine:
        return Engine.from_workspace(self.workspace_path)

    def _query(self) -> dict[str, list[str]]:
        return parse_qs(urlparse(self.path).query, keep_blank_values=True)

    def _batch_param(self, query: dict[str, list[str]]) -> int:
        if "batch" not in query:
            raise BusinessError(E_INVALID_STAGE, "query parameter batch is required")
        try:
            return int(query["batch"][0])
        except ValueError as exc:
            raise BusinessError(
                E_INVALID_STAGE, "batch query parameter must be an integer"
            ) from exc

    def _expected_version(self, query: dict[str, list[str]]) -> int | None:
        if "expected_version" not in query:
            return None
        try:
            return int(query["expected_version"][0])
        except ValueError as exc:
            raise BusinessError(
                E_STALE_VERSION, "expected_version must be an integer"
            ) from exc

    def do_GET(self) -> None:
        try:
            parsed = urlparse(self.path)
            query = self._query()
            if parsed.path == "/health":
                self._send_json(HTTPStatus.OK, {"ok": True, "service": "density-trace"})
                return
            engine = self._engine()
            if parsed.path == "/api/version":
                result = engine.version_info()
            elif parsed.path == "/api/batches":
                result = {
                    "batch_count": len(engine.manifest.batches),
                    "batches": [
                        {
                            **spec.to_dict(),
                            "input_digest": engine.manifest.input_digest(spec),
                        }
                        for spec in engine.manifest.batches
                    ],
                }
            elif parsed.path == "/api/checkpoints":
                history = engine.workspace.history()
                result = {
                    "generation": history.generation,
                    "next_sequence": history.next_sequence,
                    "next_batch_id": history.next_batch_id,
                    "records": [
                        {
                            "sequence": record["sequence"],
                            "batch_id": record["batch_id"],
                            "state_summary": record["state_summary"],
                            "decision": record["adjudication"]["decision"],
                        }
                        for record in history.records
                    ],
                    "ignored_records": list(history.ignored_records),
                }
            elif parsed.path == "/api/state":
                result = engine.get_state(
                    self._batch_param(query),
                    expected_version=self._expected_version(query),
                )
            elif parsed.path == "/api/curve":
                result = engine.get_curve(
                    self._batch_param(query),
                    expected_version=self._expected_version(query),
                )
            elif parsed.path == "/api/verdict":
                result = engine.get_verdict(
                    self._batch_param(query),
                    expected_version=self._expected_version(query),
                )
            elif parsed.path == "/api/errors":
                result = engine.get_error_report(
                    self._batch_param(query),
                    expected_version=self._expected_version(query),
                )
            elif parsed.path == "/api/summary":
                result = engine.final_summary()
            else:
                raise BusinessError("E_NOT_FOUND", f"unknown endpoint: {parsed.path}")
            self._send_json(HTTPStatus.OK, {"ok": True, "result": result})
        except BusinessError as exc:
            self._send_json(
                ERROR_STATUS.get(exc.code, HTTPStatus.INTERNAL_SERVER_ERROR),
                {"ok": False, "error": exc.to_dict()},
            )
        except Exception as exc:
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"ok": False, "error": {"code": "E_INTERNAL", "message": str(exc)}},
            )

    def do_POST(self) -> None:
        try:
            parsed = urlparse(self.path)
            body = self._read_body()
            if parsed.path == "/api/import":
                required = ["manifest_path", "config_path", "reference_path"]
                missing = [key for key in required if key not in body]
                if missing:
                    raise BusinessError(
                        E_INPUT_CONTRACT,
                        "import payload is missing paths",
                        missing=missing,
                    )
                result = import_inputs(
                    self.workspace_path,
                    body["manifest_path"],
                    body["config_path"],
                    body["reference_path"],
                )
            elif parsed.path == "/api/run":
                engine = self._engine()
                expected_version = body.get("expected_version")
                if expected_version is not None:
                    expected_version = int(expected_version)
                if "batch_id" in body:
                    result = engine.run(
                        batch_id=int(body["batch_id"]),
                        expected_version=expected_version,
                    )
                else:
                    result = engine.resume(
                        expected_version=expected_version,
                        max_batches=(
                            None
                            if body.get("max_batches") is None
                            else int(body["max_batches"])
                        ),
                    )
            elif parsed.path == "/api/resume":
                engine = self._engine()
                expected_version = body.get("expected_version")
                result = engine.resume(
                    expected_version=None
                    if expected_version is None
                    else int(expected_version),
                    max_batches=(
                        None
                        if body.get("max_batches") is None
                        else int(body["max_batches"])
                    ),
                )
            elif parsed.path == "/api/reconcile":
                result = self._engine().reconcile()
            elif parsed.path == "/api/stop":
                result = self._engine().request_stop(body)
            else:
                raise BusinessError("E_NOT_FOUND", f"unknown endpoint: {parsed.path}")
            self._send_json(HTTPStatus.OK, {"ok": True, "result": result})
        except BusinessError as exc:
            self._send_json(
                ERROR_STATUS.get(exc.code, HTTPStatus.INTERNAL_SERVER_ERROR),
                {"ok": False, "error": exc.to_dict()},
            )
        except Exception as exc:
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"ok": False, "error": {"code": "E_INTERNAL", "message": str(exc)}},
            )


def create_server(
    workspace_path: str | Path,
    host: str = "127.0.0.1",
    port: int = 8765,
) -> DensityTraceServer:
    return DensityTraceServer((host, int(port)), workspace_path)


def serve(
    workspace_path: str | Path, host: str = "127.0.0.1", port: int = 8765
) -> None:
    server = create_server(workspace_path, host, port)
    try:
        server.serve_forever()
    finally:
        server.server_close()
