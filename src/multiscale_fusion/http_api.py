from __future__ import annotations

import json
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import parse_qs, urlparse

from .engine import FusionEngine
from .errors import FusionError
from .workspace import import_bundle


_ERROR_STATUS = {
    "E_INVALID_PARAM": 400,
    "E_SHAPE_MISMATCH": 422,
    "E_NUMERIC": 422,
    "E_INVALID_STAGE": 409,
    "E_STALE_CHECKPOINT": 409,
    "E_INCREMENTAL_MISMATCH": 409,
    "E_LOCK_TIMEOUT": 503,
}


class FusionHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address: tuple[str, int], workspace: Path) -> None:
        self.workspace = workspace
        super().__init__(server_address, FusionRequestHandler)


class FusionRequestHandler(BaseHTTPRequestHandler):
    server: FusionHTTPServer

    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
        return

    def _send_json(self, status: int, payload: Mapping[str, Any]) -> None:
        data = (json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _read_json(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            raise FusionError("E_INVALID_PARAM", "invalid Content-Length")
        if length > 2 * 1024 * 1024:
            raise FusionError("E_INVALID_PARAM", "request body is too large")
        raw = self.rfile.read(length) if length else b"{}"
        try:
            value = json.loads(raw.decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            raise FusionError("E_INVALID_PARAM", f"request body is not valid JSON: {exc}")
        if not isinstance(value, dict):
            raise FusionError("E_INVALID_PARAM", "request body must be a JSON object")
        return value

    def _engine(self) -> FusionEngine:
        return FusionEngine(self.server.workspace)

    def _task_id(self, query: Mapping[str, list[str]]) -> str | None:
        values = query.get("task_id")
        return values[0] if values else None

    def _handle(self, function: Any) -> None:
        try:
            status, payload = function()
            self._send_json(status, payload)
        except FusionError as exc:
            payload = exc.to_dict()
            status = _ERROR_STATUS.get(exc.code, 400)
            self._send_json(status, payload)
        except Exception as exc:  # noqa: BLE001
            payload = {
                "ok": False,
                "error": {
                    "code": "E_INTERNAL",
                    "message": str(exc),
                    "details": {"type": type(exc).__name__},
                },
            }
            self._send_json(500, payload)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        if parsed.path == "/health":
            self._send_json(200, {"ok": True, "service": "multiscale-fusion", "workspace": str(self.server.workspace)})
            return
        if parsed.path == "/":
            self._send_json(
                200,
                {
                    "ok": True,
                    "endpoints": [
                        "GET /health",
                        "POST /v1/import",
                        "POST /v1/run",
                        "POST /v1/stage",
                        "POST /v1/stop",
                        "POST /v1/resume",
                        "POST /v1/verify-incremental",
                        "GET /v1/status?task_id=...",
                        "GET /v1/sensitivity?task_id=...",
                        "GET /v1/checkpoints?task_id=...",
                        "GET /v1/invalidation?task_id=...",
                        "GET /v1/quantization?task_id=...",
                    ],
                },
            )
            return

        def run() -> tuple[int, Mapping[str, Any]]:
            engine = self._engine()
            task_id = self._task_id(query)
            if parsed.path == "/v1/status":
                return 200, engine.status(task_id)
            if parsed.path == "/v1/sensitivity":
                return 200, engine.sensitivity(task_id)
            if parsed.path == "/v1/checkpoints":
                return 200, engine.verify_checkpoints(task_id)
            if parsed.path == "/v1/invalidation":
                task = engine._resolve_task(task_id)
                report = engine.store(task).read_report("incremental.json")
                if report is None:
                    raise FusionError.invalid_stage("incremental report is unavailable", details={"task_id": task.id})
                return 200, {"ok": True, "task_id": task.id, "report": report}
            if parsed.path == "/v1/quantization":
                task = engine._resolve_task(task_id)
                report = engine.store(task).read_report("quantization.json")
                if report is None:
                    raise FusionError.invalid_stage("quantization report is unavailable", details={"task_id": task.id})
                return 200, {"ok": True, "task_id": task.id, "report": report}
            raise FusionError.invalid_stage(f"unknown endpoint {parsed.path!r}")

        self._handle(run)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        self._handle(lambda: self._dispatch_post(parsed.path))

    def _dispatch_post(self, path: str) -> tuple[int, Mapping[str, Any]]:
        body = self._read_json()
        if path == "/v1/import":
            bundle_dir = body.get("bundle_dir")
            if not bundle_dir:
                manifest_path = body.get("manifest_path")
                if not manifest_path:
                    raise FusionError("E_INVALID_PARAM", "bundle_dir or manifest_path is required")
                if Path(manifest_path).name != "manifest.json":
                    raise FusionError("E_INVALID_PARAM", "manifest_path must point to a file named manifest.json")
                bundle_dir = str(Path(manifest_path).parent)
            overrides = body.get("shard_overrides") or {}
            if not isinstance(overrides, dict):
                raise FusionError("E_INVALID_PARAM", "shard_overrides must be an object")
            report = import_bundle(
                bundle_dir,
                self.server.workspace,
                shard_overrides=overrides,
                force=bool(body.get("force", False)),
            )
            return 200, report
        engine = self._engine()
        task_id = body.get("task_id")
        if path == "/v1/run":
            return 200, engine.run(
                task_id,
                stage_delay=float(body.get("stage_delay", 0.0)),
                verify_full=bool(body.get("verify_full", False)),
            )
        if path == "/v1/stage":
            layer = body.get("layer")
            if not layer:
                raise FusionError("E_INVALID_PARAM", "layer is required")
            return 200, engine.run_until(str(layer), task_id, stage_delay=float(body.get("stage_delay", 0.0)))
        if path == "/v1/stop":
            return 200, engine.stop(task_id, reason=str(body.get("reason", "http_request")))
        if path == "/v1/resume":
            return 200, engine.resume(
                task_id,
                stage_delay=float(body.get("stage_delay", 0.0)),
                verify_full=bool(body.get("verify_full", False)),
            )
        if path == "/v1/verify-incremental":
            return 200, engine.verify_incremental(task_id)
        raise FusionError.invalid_stage(f"unknown endpoint {path!r}")


def make_http_server(workspace: str | Path, *, host: str = "127.0.0.1", port: int = 8765) -> FusionHTTPServer:
    return FusionHTTPServer((host, int(port)), Path(workspace).resolve())


def serve_http(workspace: str | Path, *, host: str = "127.0.0.1", port: int = 8765) -> FusionHTTPServer:
    return make_http_server(workspace, host=host, port=port)
