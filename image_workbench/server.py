"""Local standard-library HTTP API."""

from __future__ import annotations

import json
import os
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .errors import WorkbenchError
from .workbench import Workbench, import_assets


class WorkbenchHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        server_address: tuple[str, int],
        workspace: str | Path,
        *,
        tasks_file: str | Path | None = None,
        config_file: str | Path | None = None,
        shards_dir: str | Path | None = None,
    ):
        self.workspace = str(Path(workspace).resolve())
        self.tasks_file = str(tasks_file) if tasks_file else None
        self.config_file = str(config_file) if config_file else None
        self.shards_dir = str(shards_dir) if shards_dir else None
        super().__init__(server_address, WorkbenchRequestHandler)

    def workbench(self) -> Workbench:
        return Workbench(
            self.workspace,
            tasks_file=self.tasks_file,
            config_file=self.config_file,
            shards_dir=self.shards_dir,
        )


class WorkbenchRequestHandler(BaseHTTPRequestHandler):
    server: WorkbenchHTTPServer
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        if os.environ.get("IMAGE_WORKBENCH_HTTP_LOG"):
            super().log_message(fmt, *args)

    def _send(self, status: int, value: Any) -> None:
        data = json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8") + b"\n"
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(data)

    def _body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            value = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise WorkbenchError(
                "E_REQUEST_INVALID",
                f"request body must be JSON: {exc}",
                reason="REQUEST_JSON",
            ) from exc
        if not isinstance(value, dict):
            raise WorkbenchError(
                "E_REQUEST_INVALID",
                "request body must be a JSON object",
                reason="REQUEST_ROOT",
            )
        return value

    def _error(self, exc: BaseException) -> None:
        if isinstance(exc, WorkbenchError):
            self._send(HTTPStatus.BAD_REQUEST, {"ok": False, "error": exc.to_dict()})
        else:
            self._send(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {
                    "ok": False,
                    "error": {
                        "code": "E_INTERNAL",
                        "message": f"{type(exc).__name__}: {exc}",
                    },
                },
            )

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/":
                self._send(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "service": "image-workbench",
                        "endpoints": [
                            "GET /api/health",
                            "GET /api/status",
                            "GET /api/budget",
                            "GET /api/versions",
                            "GET /api/reports",
                            "POST /api/import",
                            "POST /api/run",
                            "POST /api/resume",
                            "POST /api/stop",
                        ],
                    },
                )
                return
            if parsed.path == "/api/health":
                self._send(HTTPStatus.OK, {"ok": True, "service": "image-workbench"})
                return
            workbench = self.server.workbench()
            if parsed.path == "/api/status":
                self._send(HTTPStatus.OK, {"ok": True, "result": workbench.status()})
                return
            if parsed.path == "/api/budget":
                self._send(HTTPStatus.OK, {"ok": True, "result": workbench.budget()})
                return
            if parsed.path == "/api/versions":
                self._send(HTTPStatus.OK, {"ok": True, "result": workbench.versions()})
                return
            if parsed.path == "/api/reports":
                query = parse_qs(parsed.query)
                reports = workbench.reports()["reports"]
                name = query.get("name", [None])[0]
                if name:
                    if name not in reports:
                        raise WorkbenchError(
                            "E_NOT_FOUND", f"report not found: {name}", reason="REPORT_MISSING"
                        )
                    self._send(HTTPStatus.OK, {"ok": True, "result": reports[name]})
                else:
                    self._send(HTTPStatus.OK, {"ok": True, "result": reports})
                return
            raise WorkbenchError(
                "E_NOT_FOUND", f"unknown path {parsed.path}", reason="PATH_NOT_FOUND"
            )
        except BaseException as exc:
            self._error(exc)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        try:
            body = self._body()
            if parsed.path == "/api/import":
                required = ("shards_dir", "tasks_file", "config_file")
                missing = [key for key in required if not body.get(key)]
                if missing:
                    raise WorkbenchError(
                        "E_REQUEST_INVALID",
                        f"missing import fields: {', '.join(missing)}",
                        reason="IMPORT_FIELDS",
                    )
                result = import_assets(
                    self.server.workspace,
                    body["shards_dir"],
                    body["tasks_file"],
                    body["config_file"],
                )
                self._send(HTTPStatus.OK, {"ok": True, "result": result})
                return
            workbench = self.server.workbench()
            if parsed.path == "/api/stop":
                self._send(HTTPStatus.OK, {"ok": True, "result": workbench.request_stop()})
                return
            if parsed.path in {"/api/run", "/api/resume"}:
                until_stage = str(body.get("until_stage", "exported"))
                result = workbench.run(
                    until_stage=until_stage,
                    sequential=bool(body.get("sequential", False)),
                    task_delay_ms=int(body.get("task_delay_ms", 0)),
                    clear_stop=parsed.path == "/api/resume",
                )
                self._send(HTTPStatus.OK, {"ok": True, "result": result})
                return
            raise WorkbenchError(
                "E_NOT_FOUND", f"unknown path {parsed.path}", reason="PATH_NOT_FOUND"
            )
        except BaseException as exc:
            self._error(exc)


def make_server(
    workspace: str | Path,
    host: str = "127.0.0.1",
    port: int = 8765,
    *,
    tasks_file: str | Path | None = None,
    config_file: str | Path | None = None,
    shards_dir: str | Path | None = None,
) -> WorkbenchHTTPServer:
    return WorkbenchHTTPServer(
        (host, port),
        workspace,
        tasks_file=tasks_file,
        config_file=config_file,
        shards_dir=shards_dir,
    )


def serve(
    workspace: str | Path,
    host: str = "127.0.0.1",
    port: int = 8765,
    *,
    tasks_file: str | Path | None = None,
    config_file: str | Path | None = None,
    shards_dir: str | Path | None = None,
) -> None:
    server = make_server(
        workspace,
        host,
        port,
        tasks_file=tasks_file,
        config_file=config_file,
        shards_dir=shards_dir,
    )
    print(f"image-workbench HTTP listening on http://{host}:{server.server_port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
