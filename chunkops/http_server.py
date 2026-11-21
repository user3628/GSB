"""Dependency-free local HTTP API for the chunked operator pipeline."""

from __future__ import annotations

import ipaddress
import json
import math
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .data import generate_datasets, load_manifest
from .errors import (
    ChunkOpsError,
    E_BAD_REQUEST,
    E_INVALID_STATE,
    E_MEMORY_BUDGET,
    E_MISSING_FRAGMENT,
    E_NOT_FOUND,
    E_NONFINITE,
    E_NUMERIC_MISMATCH,
    E_STALE_PUBLISH,
    E_STALE_VERSION,
    E_ZERO_ROW,
)
from .pipeline import Pipeline, import_fragment, request_stop


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        return value
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except Exception:
            pass
    return value


_ERROR_STATUS = {
    E_BAD_REQUEST: 400,
    "E_INVALID_SHAPE": 400,
    "E_INVALID_AXIS": 400,
    "E_INVALID_CONFIG": 400,
    E_NOT_FOUND: 404,
    E_MISSING_FRAGMENT: 404,
    E_STALE_VERSION: 409,
    E_STALE_PUBLISH: 409,
    E_INVALID_STATE: 409,
    E_NUMERIC_MISMATCH: 422,
    E_ZERO_ROW: 422,
    E_NONFINITE: 422,
    E_MEMORY_BUDGET: 507,
}


class LocalHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class ChunkOpsHandler(BaseHTTPRequestHandler):
    server_version = "ChunkOpsHTTP/1.0"

    def log_message(self, format: str, *args: Any) -> None:
        if os.environ.get("CHUNKOPS_HTTP_QUIET") == "1":
            return
        super().log_message(format, *args)

    def _send_json(self, status: int, value: Any) -> None:
        data = json.dumps(
            _json_safe(value), ensure_ascii=False, sort_keys=True, allow_nan=False
        ).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(data)

    def _read_json_body(self) -> dict[str, Any]:
        length_header = self.headers.get("Content-Length")
        if not length_header:
            return {}
        try:
            length = int(length_header)
        except ValueError as exc:
            raise ChunkOpsError(E_BAD_REQUEST, "invalid Content-Length") from exc
        if length < 0 or length > 4 * 1024 * 1024:
            raise ChunkOpsError(
                E_BAD_REQUEST, "request body too large", {"content_length": length}
            )
        raw = self.rfile.read(length)
        if not raw:
            return {}
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ChunkOpsError(
                E_BAD_REQUEST, "request body must be valid JSON", {"cause": type(exc).__name__}
            ) from exc
        if not isinstance(value, dict):
            raise ChunkOpsError(E_BAD_REQUEST, "request JSON body must be an object")
        return value

    @staticmethod
    def _workspace(source: dict[str, Any]) -> str:
        workspace = source.get("workspace")
        if not workspace:
            raise ChunkOpsError(
                E_BAD_REQUEST,
                "workspace is required",
                {"position": "workspace"},
            )
        return str(workspace)

    def _dispatch_get(self, parsed: Any) -> Any:
        query = {key: values[-1] for key, values in parse_qs(parsed.query).items()}
        path = parsed.path
        if path in ("/", "/health"):
            return {
                "service": "chunkops",
                "status": "ok",
                "endpoints": [
                    "POST /api/fragments/import",
                    "POST /api/datasets/generate",
                    "GET /api/fragments/manifest?root=data",
                    "POST /api/runs/run",
                    "POST /api/runs/stage",
                    "POST /api/runs/resume",
                    "POST /api/runs/stop",
                    "GET /api/runs/status?workspace=...",
                    "GET /api/runs/plan?workspace=...",
                    "GET /api/runs/checkpoints?workspace=...",
                    "GET /api/runs/versions?workspace=...",
                    "GET /api/runs/errors?workspace=...",
                    "GET /api/runs/summary?workspace=...",
                ],
            }
        if path == "/api/fragments/manifest":
            return load_manifest(query.get("root", "data"))
        known_get = {
            "/api/runs/status",
            "/api/runs/plan",
            "/api/runs/checkpoints",
            "/api/runs/versions",
            "/api/runs/errors",
            "/api/runs/summary",
        }
        if path not in known_get:
            raise ChunkOpsError(E_NOT_FOUND, "unknown endpoint", {"path": path})
        workspace = self._workspace(query)
        pipeline = Pipeline(workspace)
        if path == "/api/runs/status":
            return pipeline.status()
        if path == "/api/runs/plan":
            return {
                "status": pipeline.status(),
                "plan": pipeline.plan_info(),
            }
        if path == "/api/runs/checkpoints":
            return pipeline.checkpoints.discover()
        if path == "/api/runs/versions":
            return {
                "plans": pipeline.plan_versions.inspect(),
                "results": pipeline.result_versions.inspect(),
                "checkpoints": pipeline.checkpoints.discover(),
            }
        if path == "/api/runs/errors":
            return pipeline.error_report()
        if path == "/api/runs/summary":
            return pipeline.summary()
        raise ChunkOpsError(E_NOT_FOUND, "unknown endpoint", {"path": path})

    def _dispatch_post(self, parsed: Any) -> Any:
        body = self._read_json_body()
        path = parsed.path
        if path == "/api/datasets/generate":
            return generate_datasets(
                body.get("root", "data"),
                seed=int(body.get("seed", 20261122)),
                include_large=bool(body.get("include_large", True)),
                overwrite=bool(body.get("overwrite", False)),
            )
        if path == "/api/fragments/import":
            workspace = self._workspace(body)
            fragment = body.get("fragment") or body.get("fragment_path")
            if not fragment:
                raise ChunkOpsError(
                    E_BAD_REQUEST, "fragment path is required", {"position": "fragment"}
                )
            config = import_fragment(
                workspace,
                fragment,
                op=body.get("op", "softmax"),
                axis=int(body.get("axis", 1)),
                fragment_id=body.get("fragment_id"),
                cache_budget_bytes=int(body.get("cache_budget_bytes", 4 * 1024 * 1024)),
                memory_budget_bytes=int(body.get("memory_budget_bytes", 512 * 1024 * 1024)),
                eps=float(body.get("eps", 1e-5)),
                tolerance=body.get("tolerance"),
                output_dtype=body.get("output_dtype"),
                affine=bool(body.get("affine", False)),
                plan_override=body.get("plan_override"),
                validation_max_sample_blocks=int(body.get("validation_max_sample_blocks", 10)),
                validation_seed=body.get("validation_seed"),
                reject_zero_rows=bool(body.get("reject_zero_rows", False)),
                reject_nonfinite=bool(body.get("reject_nonfinite", False)),
            )
            return {"config": config, "status": Pipeline(workspace).status()}
        if path == "/api/runs/stop":
            workspace = self._workspace(body)
            marker = request_stop(workspace, reason=body.get("reason", "http_request"))
            return {"stop": marker, "status": Pipeline(workspace).status()}
        workspace = self._workspace(body)
        pipeline = Pipeline(workspace)
        if path == "/api/runs/run":
            if body.get("clear_stop"):
                from .pipeline import clear_stop

                clear_stop(workspace)
            if body.get("stage"):
                return pipeline.run_stage(str(body["stage"]))
            return pipeline.run(
                until_stage=body.get("until_stage"),
                max_stages=int(body["max_stages"]) if body.get("max_stages") is not None else None,
                stop_after_stage=body.get("stop_after_stage"),
                clear_stop_request=bool(body.get("clear_stop", False)),
            )
        if path == "/api/runs/stage":
            stage = body.get("stage")
            if not stage:
                raise ChunkOpsError(E_BAD_REQUEST, "stage is required", {"position": "stage"})
            return pipeline.run_stage(str(stage))
        if path == "/api/runs/resume":
            return pipeline.resume(
                max_stages=int(body["max_stages"]) if body.get("max_stages") is not None else None,
                stop_after_stage=body.get("stop_after_stage"),
            )
        raise ChunkOpsError(E_NOT_FOUND, "unknown endpoint", {"path": path})

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        try:
            result = self._dispatch_get(parsed)
            self._send_json(200, {"ok": True, "code": "OK", "result": result})
        except Exception as exc:
            self._send_error(exc)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        try:
            result = self._dispatch_post(parsed)
            self._send_json(200, {"ok": True, "code": "OK", "result": result})
        except Exception as exc:
            self._send_error(exc)

    def _send_error(self, exc: Exception) -> None:
        if isinstance(exc, ChunkOpsError):
            error = exc
        else:
            error = ChunkOpsError(
                "E_INTERNAL",
                "unhandled server error",
                {"exception_type": type(exc).__name__},
            )
        status = _ERROR_STATUS.get(error.code, 500)
        self._send_json(status, error.to_dict())


def _assert_loopback(host: str) -> None:
    if host in ("localhost",):
        return
    try:
        address = ipaddress.ip_address(host)
    except ValueError as exc:
        raise ChunkOpsError(
            E_BAD_REQUEST, "host must be a loopback IP address", {"host": host}
        ) from exc
    if not address.is_loopback:
        raise ChunkOpsError(
            E_BAD_REQUEST,
            "local service refuses non-loopback bind addresses",
            {"host": host},
        )


def create_server(host: str = "127.0.0.1", port: int = 8765) -> LocalHTTPServer:
    _assert_loopback(host)
    return LocalHTTPServer((host, int(port)), ChunkOpsHandler)


def serve(host: str = "127.0.0.1", port: int = 8765) -> None:
    server = create_server(host, port)
    actual_host, actual_port = server.server_address[:2]
    print(
        json.dumps(
            {
                "service": "chunkops",
                "status": "listening",
                "url": f"http://{actual_host}:{actual_port}",
                "loopback": True,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    serve()
