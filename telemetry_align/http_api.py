"""Local-only HTTP API and static workbench server."""

from __future__ import annotations

import json
import mimetypes
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .errors import TelemetryError, error_payload
from .workspace import AlignmentWorkspace


STATIC_DIR = Path(__file__).resolve().parent / "static"
MAX_BODY_BYTES = 32 * 1024 * 1024


class WorkbenchServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple[str, int], root: Path) -> None:
        self.workspace_root = Path(root).resolve()
        self.request_lock = threading.Lock()
        super().__init__(address, WorkbenchHandler)


class WorkbenchHandler(BaseHTTPRequestHandler):
    server: WorkbenchServer
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        if getattr(self.server, "quiet", False):
            return
        super().log_message(format, *args)

    @property
    def workspace(self) -> AlignmentWorkspace:
        return AlignmentWorkspace(self.server.workspace_root)

    # -------------------------------------------------------------- responses
    def _send_json(self, status: int, payload: Any) -> None:
        data = (
            json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2)
            + "\n"
        ).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _send_static(self, path: Path, *, download: bool = False) -> None:
        if not path.exists() or not path.is_file():
            raise TelemetryError(
                "E_NOT_FOUND", "静态文件不存在 / static file not found", {"path": str(path)}
            )
        data = path.read_bytes()
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        if path.suffix == ".js":
            content_type = "application/javascript"
        if path.suffix == ".css":
            content_type = "text/css"
        if path.suffix == ".html":
            content_type = "text/html"
        self.send_response(200)
        self.send_header("Content-Type", f"{content_type}; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        if download:
            self.send_header(
                "Content-Disposition", f'attachment; filename="{path.name}"'
            )
        self.end_headers()
        self.wfile.write(data)

    def _read_json_body(self) -> dict[str, Any]:
        length_header = self.headers.get("Content-Length", "0")
        try:
            length = int(length_header)
        except ValueError as exc:
            raise TelemetryError(
                "E_CONFIG_INVALID", "Content-Length 无效 / invalid Content-Length", {}
            ) from exc
        if length < 0 or length > MAX_BODY_BYTES:
            raise TelemetryError(
                "E_CONFIG_INVALID",
                "请求体过大 / request body is too large",
                {"length": length, "max": MAX_BODY_BYTES},
            )
        if length == 0:
            return {}
        data = self.rfile.read(length)
        try:
            payload = json.loads(data.decode("utf-8"))
        except Exception as exc:
            raise TelemetryError(
                "E_CONFIG_INVALID", "JSON 请求体无效 / invalid JSON body", {}
            ) from exc
        if not isinstance(payload, dict):
            raise TelemetryError(
                "E_CONFIG_INVALID", "JSON 请求体必须是对象 / body must be an object", {}
            )
        return payload

    def _handle(self, method: str) -> None:
        parsed = urlparse(self.path)
        route = parsed.path.rstrip("/") or "/"
        try:
            if method == "GET" and route == "/":
                self._send_static(STATIC_DIR / "index.html")
                return
            if method == "GET" and route.startswith("/static/"):
                name = route[len("/static/") :]
                if "/" in name or name.startswith("."):
                    raise TelemetryError("E_NOT_FOUND", "静态路径无效", {"path": route})
                self._send_static(STATIC_DIR / name)
                return
            if route == "/api/health" and method == "GET":
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "service": "telemetry-align-workbench",
                        "workspace": str(self.server.workspace_root),
                    },
                )
                return
            if route == "/api/status" and method == "GET":
                query = parse_qs(parsed.query)
                config = query.get("config", [None])[0]
                try:
                    self._send_json(200, self.workspace.status(config))
                except TelemetryError as exc:
                    if exc.code == "E_NOT_FOUND":
                        self._send_json(
                            200,
                            {
                                "ok": True,
                                "initialized": False,
                                "workspace": str(self.server.workspace_root),
                            },
                        )
                        return
                    raise
                return
            if route == "/api/fingerprints" and method == "GET":
                query = parse_qs(parsed.query)
                config = query.get("config", [None])[0]
                self._send_json(200, self.workspace.fingerprints(config))
                return
            if route == "/api/conflicts" and method == "GET":
                config = parse_qs(parsed.query).get("config", [None])[0]
                self._send_json(200, self.workspace.conflicts(config))
                return
            if route == "/api/gaps" and method == "GET":
                config = parse_qs(parsed.query).get("config", [None])[0]
                self._send_json(200, self.workspace.gaps(config))
                return
            if route == "/api/cleaning" and method == "GET":
                config = parse_qs(parsed.query).get("config", [None])[0]
                self._send_json(200, self.workspace.cleaning(config))
                return
            if route == "/api/summary" and method == "GET":
                config = parse_qs(parsed.query).get("config", [None])[0]
                self._send_json(200, self.workspace.summary(config))
                return
            if route == "/api/checkpoints" and method == "GET":
                self._send_json(200, {"ok": True, "checkpoints": self.workspace.store.summary()})
                return
            if route == "/api/checkpoints/verify" and method == "GET":
                self._send_json(200, {"ok": True, "verification": self.workspace.store.verify()})
                return
            if route == "/api/export" and method == "GET":
                config = parse_qs(parsed.query).get("config", [None])[0]
                self._send_json(200, self.workspace.summary(config))
                return
            if method == "POST":
                body = self._read_json_body()
                if route == "/api/demo/init":
                    result = self.workspace.init_demo(reset=bool(body.get("reset", True)))
                    if body.get("run"):
                        result["alignment"] = self.workspace.align()
                        result["status"] = self.workspace.status()
                    self._send_json(200, {"ok": True, **result})
                    return
                if route == "/api/run":
                    result = self.workspace.align(
                        config_name=body.get("config"),
                        resume=bool(body.get("resume", False)),
                        until_stage=body.get("until_stage"),
                    )
                    self._send_json(200, result)
                    return
                if route == "/api/resume":
                    result = self.workspace.align(
                        config_name=body.get("config"),
                        resume=True,
                        until_stage=body.get("until_stage"),
                    )
                    self._send_json(200, result)
                    return
                if route == "/api/stop":
                    result = self.workspace.request_stop(body.get("after_stage"))
                    self._send_json(200, {"ok": True, "stop": result})
                    return
                if route == "/api/increment":
                    result = self.workspace.mutate_source_and_rebuild(
                        sensor_id=body.get("sensor_id", "sensor_a"),
                        config_name=body.get("config"),
                    )
                    self._send_json(200, result)
                    return
                if route == "/api/import":
                    result = self.workspace.import_inputs(
                        sources=body.get("sources"),
                        configs=body.get("configs"),
                        active_config=body.get("active_config"),
                        v1_policy=body.get("v1_policy"),
                        workspace_id=body.get("workspace_id"),
                    )
                    self._send_json(200, result)
                    return
                if route == "/api/export":
                    result = self.workspace.export_summary(
                        body.get("config"),
                        output_path=body.get(
                            "output_path", "out/alignment_summary.json"
                        ),
                    )
                    self._send_json(200, result)
                    return
                if route == "/api/upgrade-v1":
                    result = self.workspace.upgrade_v1_config(
                        config_name=body.get("config", "v1"),
                        output_path=body.get(
                            "output_path", "out/upgraded_config_v2.yaml"
                        ),
                    )
                    self._send_json(200, result)
                    return
            raise TelemetryError(
                "E_NOT_FOUND",
                "接口不存在 / endpoint not found",
                {"method": method, "path": route},
            )
        except TelemetryError as exc:
            self._send_json(exc.http_status, exc.to_dict())
        except BrokenPipeError:
            return
        except Exception as exc:  # pragma: no cover - defensive boundary
            self._send_json(500, error_payload(exc))

    def do_GET(self) -> None:  # noqa: N802
        self._handle("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._handle("POST")


def create_server(
    root: Path | str,
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
    quiet: bool = False,
) -> WorkbenchServer:
    root_path = Path(root).resolve()
    root_path.mkdir(parents=True, exist_ok=True)
    try:
        server = WorkbenchServer((host, int(port)), root_path)
    except OSError as exc:
        if getattr(exc, "errno", None) in {48, 98, 10048}:
            raise TelemetryError(
                "E_PORT_IN_USE",
                "HTTP 端口已被占用 / HTTP port is already in use",
                {"host": host, "port": port},
            ) from exc
        raise TelemetryError(
            "E_IO",
            "HTTP 服务启动失败 / could not start HTTP server",
            {"host": host, "port": port, "reason": str(exc)},
        ) from exc
    server.quiet = quiet
    return server


def serve(
    root: Path | str,
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
    quiet: bool = False,
) -> None:
    server = create_server(root, host=host, port=port, quiet=quiet)
    actual_host, actual_port = server.server_address[:2]
    print(
        json.dumps(
            {
                "ok": True,
                "url": f"http://{actual_host}:{actual_port}/",
                "workspace": str(Path(root).resolve()),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()

