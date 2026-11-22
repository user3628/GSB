"""Dependency-free local HTTP API built on http.server."""

from __future__ import annotations

import json
import re
import sys
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import unquote, urlparse

from . import __version__
from .canonical import canonical_json_bytes
from .config import PipelineConfig
from .errors import E_INVALID_INPUT, E_TASK_NOT_FOUND, PipelineError, internal_error
from .pipeline import Pipeline
from .workspace import Workspace


_TASK_ROUTE_RE = re.compile(r"^/v1/(?:query|tasks)/([^/]+)$")
_RESULT_ROUTE_RE = re.compile(r"^/v1/results/([^/]+)\.png$")
_PARSE_ROUTE_RE = re.compile(r"^/v1/parse/([^/]+)$")


class GspipeHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address: tuple[str, int], workspace: str | Path) -> None:
        super().__init__(server_address, APIHandler)
        self.workspace_root = str(Path(workspace).resolve())


class APIHandler(BaseHTTPRequestHandler):
    server: GspipeHTTPServer
    server_version = f"gspipe/{__version__}"

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write("[gspipe-http] " + (fmt % args) + "\n")

    def _send_bytes(
        self,
        status: int,
        data: bytes,
        *,
        content_type: str = "application/json; charset=utf-8",
        extra_headers: Mapping[str, str] | None = None,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        for key, value in (extra_headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(data)

    def _send_json(self, status: int, value: Any, *, extra_headers: Mapping[str, str] | None = None) -> None:
        data = canonical_json_bytes(value) + b"\n"
        self._send_bytes(status, data, extra_headers=extra_headers)

    def _send_error(self, exc: PipelineError) -> None:
        self._send_json(exc.http_status, exc.as_response())

    def _read_json(self) -> dict[str, Any]:
        raw_length = self.headers.get("Content-Length", "0")
        try:
            length = int(raw_length)
        except ValueError as exc:
            raise PipelineError(E_INVALID_INPUT, "Invalid Content-Length") from exc
        if length < 0 or length > 32 * 1024 * 1024:
            raise PipelineError(E_INVALID_INPUT, "Request body is too large")
        body = self.rfile.read(length) if length else b"{}"
        try:
            value = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PipelineError(E_INVALID_INPUT, f"Invalid JSON request body: {exc}") from exc
        if not isinstance(value, dict):
            raise PipelineError(E_INVALID_INPUT, "JSON request body must be an object")
        return value

    def _pipeline(self) -> Pipeline:
        return Pipeline(self.server.workspace_root)

    def _workspace(self) -> Workspace:
        workspace = Workspace(self.server.workspace_root)
        workspace.ensure_layout()
        return workspace

    def do_OPTIONS(self) -> None:
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,PUT,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        try:
            if path in {"/", "/v1/health", "/health"}:
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "service": "gspipe",
                        "version": __version__,
                        "workspace": self.server.workspace_root,
                        "endpoints": [
                            "POST /v1/import",
                            "POST /v1/demo/init",
                            "POST /v1/run",
                            "GET /v1/status",
                            "GET /v1/fingerprints",
                            "GET /v1/tasks",
                            "GET /v1/query/<task_id>",
                            "GET /v1/parse/<task_id>",
                            "POST /v1/stop",
                            "POST /v1/resume",
                            "POST /v1/export",
                            "GET /v1/export/latest",
                            "GET /v1/export/summary",
                            "GET /v1/results/<task_id>.png",
                        ],
                    },
                )
                return
            if path in {"/v1/status", "/status"}:
                self._send_json(HTTPStatus.OK, self._pipeline().status())
                return
            if path == "/v1/fingerprints":
                self._send_json(HTTPStatus.OK, self._pipeline().fingerprints())
                return
            if path == "/v1/tasks":
                workspace = self._workspace()
                self._send_json(HTTPStatus.OK, {"ok": True, "tasks": [task.to_dict() for task in workspace.load_tasks()]})
                return
            if path == "/v1/config":
                workspace = self._workspace()
                config = workspace.load_config()
                self._send_json(HTTPStatus.OK, {"ok": True, "config": config.to_dict(), "config_version": config.config_version})
                return
            if path == "/v1/export/latest":
                latest = self._pipeline().latest_export()
                if latest is None:
                    raise PipelineError("E_RESULT_UNAVAILABLE", "No export bundle is available")
                self._send_json(HTTPStatus.OK, {"ok": True, "export": latest})
                return
            if path == "/v1/export/summary":
                latest = self._pipeline().latest_export()
                if latest is None:
                    raise PipelineError("E_RESULT_UNAVAILABLE", "No export bundle is available")
                self._send_json(HTTPStatus.OK, latest["summary"])
                return
            if path == "/v1/last-run":
                last_run = self._pipeline().metrics.read_last_run()
                self._send_json(HTTPStatus.OK, {"ok": True, "last_run": last_run})
                return
            match = _PARSE_ROUTE_RE.match(path)
            if match:
                task_id = unquote(match.group(1))
                self._send_json(HTTPStatus.OK, self._pipeline().validate_task(task_id))
                return
            match = _TASK_ROUTE_RE.match(path)
            if match:
                task_id = unquote(match.group(1))
                data, cache_hit = self._pipeline().query_task(task_id)
                self._send_bytes(
                    HTTPStatus.OK,
                    data,
                    extra_headers={"X-GSPipe-Cache": "HIT" if cache_hit else "MISS"},
                )
                return
            match = _RESULT_ROUTE_RE.match(path)
            if match:
                task_id = unquote(match.group(1))
                png_bytes = self._pipeline().get_result_png(task_id)
                self._send_bytes(HTTPStatus.OK, png_bytes, content_type="image/png")
                return
            raise PipelineError("E_INVALID_INPUT", f"Unknown endpoint GET {path}", details={"path": path})
        except PipelineError as exc:
            self._send_error(exc)
        except Exception as exc:
            self._send_error(internal_error(exc, method="GET", path=path))

    def do_POST(self) -> None:
        path = urlparse(self.path).path.rstrip("/") or "/"
        try:
            body = self._read_json()
            if path == "/v1/import":
                result = self._workspace().import_http_payload(body)
                self._send_json(HTTPStatus.OK, result)
                return
            if path == "/v1/demo/init":
                result = self._workspace().init_demo(force=bool(body.get("force", False)))
                self._send_json(HTTPStatus.OK, result)
                return
            if path == "/v1/run":
                stages = body.get("stages")
                parallel = bool(body.get("parallel", True))
                resume = bool(body.get("resume", False))
                result = self._pipeline().run(stages, resume=resume, parallel=parallel)
                self._send_json(HTTPStatus.OK, result)
                return
            if path == "/v1/resume":
                stages = body.get("stages")
                parallel = bool(body.get("parallel", True))
                result = self._pipeline().run(stages, resume=True, parallel=parallel)
                self._send_json(HTTPStatus.OK, result)
                return
            if path == "/v1/stop":
                result = self._workspace().request_stop(str(body.get("reason", "http_stop")))
                self._send_json(HTTPStatus.OK, result)
                return
            if path == "/v1/export":
                result = self._pipeline().export(parallel=bool(body.get("parallel", True)))
                self._send_json(HTTPStatus.OK, result)
                return
            if path == "/v1/query":
                task_id = body.get("task_id")
                if not isinstance(task_id, str):
                    raise PipelineError(E_INVALID_INPUT, "task_id is required")
                data, cache_hit = self._pipeline().query_task(task_id, force_recompute=bool(body.get("recompute", False)))
                self._send_bytes(
                    HTTPStatus.OK,
                    data,
                    extra_headers={"X-GSPipe-Cache": "HIT" if cache_hit else "MISS"},
                )
                return
            raise PipelineError("E_INVALID_INPUT", f"Unknown endpoint POST {path}", details={"path": path})
        except PipelineError as exc:
            self._send_error(exc)
        except Exception as exc:
            self._send_error(internal_error(exc, method="POST", path=path))

    def do_PUT(self) -> None:
        path = urlparse(self.path).path.rstrip("/") or "/"
        try:
            body = self._read_json()
            if path != "/v1/config":
                raise PipelineError("E_INVALID_INPUT", f"Unknown endpoint PUT {path}", details={"path": path})
            workspace = self._workspace()
            config = PipelineConfig(body.get("config", body))
            with workspace.run_lock():
                workspace.save_config(config)
            self._send_json(
                HTTPStatus.OK,
                {"ok": True, "config": config.to_dict(), "config_version": config.config_version},
            )
        except PipelineError as exc:
            self._send_error(exc)
        except Exception as exc:
            self._send_error(internal_error(exc, method="PUT", path=path))


def create_server(workspace: str | Path, host: str = "127.0.0.1", port: int = 8765) -> GspipeHTTPServer:
    workspace_obj = Workspace(workspace)
    workspace_obj.ensure_layout()
    return GspipeHTTPServer((host, int(port)), workspace_obj.root)


def serve(workspace: str | Path, host: str = "127.0.0.1", port: int = 8765) -> None:
    server = create_server(workspace, host, port)
    actual_host, actual_port = server.server_address[:2]
    print(f"gspipe HTTP listening on http://{actual_host}:{actual_port}", flush=True)
    print(f"workspace: {server.workspace_root}", flush=True)
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
