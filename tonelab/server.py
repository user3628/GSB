"""Local dependency-free HTTP workbench."""

from __future__ import annotations

import json
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .config import Config
from .engine import BuildEngine
from .errors import ToneLabError
from .workspace import Workspace


class WorkbenchApp:
    def __init__(self, workspace: Workspace) -> None:
        self.workspace = workspace
        self.engine = BuildEngine(workspace)
        self.lock = threading.RLock()

    def dispatch(self, method: str, path: str, payload: dict[str, Any], query: dict[str, list[str]]) -> dict[str, Any]:
        with self.lock:
            if method == "GET" and path == "/api/health":
                return {
                    "ok": True,
                    "service": "tonelab",
                    "workspace": str(self.workspace.root),
                    "config_version": self.workspace.config.version,
                    "endpoints": [
                        "/api/health",
                        "/api/status",
                        "/api/fingerprints",
                        "/api/checkpoints",
                        "/api/import",
                        "/api/import/wav",
                        "/api/import/units",
                        "/api/build",
                        "/api/stage",
                        "/api/edit/pitch",
                        "/api/edit/unit",
                        "/api/replace-sample",
                        "/api/upgrade",
                        "/api/stop",
                        "/api/resume",
                        "/api/export",
                        "/api/roundtrip",
                        "/api/consistency",
                    ],
                }
            if method == "GET" and path == "/api/status":
                return self.engine.status()
            if method == "GET" and path == "/api/fingerprints":
                return self.engine.fingerprints()
            if method == "GET" and path == "/api/checkpoints":
                return self.engine.checkpoints()
            if method == "POST" and path == "/api/import":
                manifests = payload.get("manifests") or payload.get("manifest")
                if not manifests:
                    raise ToneLabError("E_BAD_REQUEST", "import requires manifests")
                if isinstance(manifests, str):
                    manifests = [manifests]
                project = self.workspace.import_manifests(manifests, project_id=payload.get("project_id"))
                return {
                    "ok": True,
                    "project_id": project.project_id,
                    "shards": [shard.shard_id for shard in project.shards],
                }
            if method == "POST" and path == "/api/import/wav":
                if not payload.get("wav"):
                    raise ToneLabError("E_BAD_REQUEST", "import/wav requires wav path")
                project = self.workspace.add_wav_shard(payload["wav"], payload.get("sidecar"))
                return {
                    "ok": True,
                    "project_id": project.project_id,
                    "shards": [shard.shard_id for shard in project.shards],
                }
            if method == "POST" and path == "/api/import/units":
                if not payload.get("path"):
                    raise ToneLabError("E_BAD_REQUEST", "import/units requires annotation path")
                project = self.workspace.apply_units_file(payload["path"])
                return {
                    "ok": True,
                    "project_id": project.project_id,
                    "units": {shard.shard_id: len(shard.units) for shard in project.shards},
                }
            if method == "POST" and path == "/api/build":
                return self.engine.build(
                    str(payload.get("mode", "incremental")),
                    clear_stop=bool(payload.get("clear_stop", False)),
                    until_stage=payload.get("until_stage") or payload.get("stage"),
                )
            if method == "POST" and path == "/api/stage":
                return self.engine.run_stage(
                    str(payload["stage"]),
                    str(payload.get("mode", "incremental")),
                )
            if method == "POST" and path == "/api/edit/pitch":
                return self.engine.edit_pitch_point(
                    str(payload["shard_id"]),
                    int(payload["point_index"]),
                    time=float(payload["time"]) if "time" in payload else None,
                    value=float(payload["value"]) if "value" in payload else None,
                )
            if method == "POST" and path == "/api/edit/unit":
                return self.engine.edit_unit(
                    str(payload["shard_id"]),
                    int(payload["unit_index"]),
                    start_ms=float(payload["start_ms"]) if "start_ms" in payload else None,
                    end_ms=float(payload["end_ms"]) if "end_ms" in payload else None,
                    symbol=str(payload["symbol"]) if "symbol" in payload else None,
                    formants_hz=[float(value) for value in payload["formants_hz"]]
                    if "formants_hz" in payload
                    else None,
                    formant_ranges_hz=[
                        [float(pair[0]), float(pair[1])] for pair in payload["formant_ranges_hz"]
                    ]
                    if "formant_ranges_hz" in payload
                    else None,
                )
            if method == "POST" and path == "/api/replace-sample":
                if payload.get("wav"):
                    return self.engine.replace_sample(str(payload["shard_id"]), payload["wav"])
                raise ToneLabError("E_BAD_REQUEST", "replace-sample requires wav path")
            if method == "POST" and path == "/api/upgrade":
                return self.engine.upgrade_shard(str(payload["shard_id"]), int(payload.get("to_version", 2)))
            if method == "POST" and path == "/api/stop":
                return {"ok": True, **self.engine.request_stop(str(payload.get("reason", "user requested stop")))}
            if method == "POST" and path == "/api/resume":
                return self.engine.resume()
            if method == "POST" and path == "/api/export":
                fmt = str(payload["format"])
                out_dir = Path(str(payload.get("out", self.workspace.exports_dir / fmt)))
                return self.engine.export(fmt, out_dir)
            if method == "POST" and path == "/api/roundtrip":
                fmt = str(payload.get("format", "json_v2"))
                return self.engine.roundtrip(fmt)
            if method == "POST" and path == "/api/consistency":
                mutation = payload.get("mutation", payload)
                return self.engine.consistency_check(mutation)
            raise ToneLabError(
                "E_NOT_FOUND",
                "unknown HTTP endpoint",
                context={"method": method, "path": path},
            )


def make_handler(app: WorkbenchApp, web_root: Path) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "ToneLab/0.1"

        def log_message(self, format: str, *args: Any) -> None:
            print(f"[tonelab-http] {self.address_string()} {format % args}")

        def _send_json(self, status: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _send_html(self, path: Path) -> None:
            body = path.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path in {"/", "/index.html"}:
                self._send_html(web_root / "index.html")
                return
            try:
                result = app.dispatch("GET", parsed.path, {}, parse_qs(parsed.query))
                self._send_json(HTTPStatus.OK, result)
            except ToneLabError as exc:
                status = HTTPStatus.NOT_FOUND if exc.code == "E_NOT_FOUND" else HTTPStatus.BAD_REQUEST
                self._send_json(status, exc.to_dict())
            except Exception as exc:  # pragma: no cover - defensive boundary
                self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": {"code": "E_INTERNAL", "message": str(exc)}})

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            length = int(self.headers.get("Content-Length", "0"))
            if length > 16 * 1024 * 1024:
                self._send_json(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"ok": False, "error": {"code": "E_BAD_REQUEST", "message": "request too large"}})
                return
            raw = self.rfile.read(length) if length else b"{}"
            try:
                payload = json.loads(raw.decode("utf-8")) if raw else {}
            except Exception as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": {"code": "E_BAD_REQUEST", "message": f"invalid JSON body: {exc}"}})
                return
            try:
                result = app.dispatch("POST", parsed.path, payload, parse_qs(parsed.query))
                self._send_json(HTTPStatus.OK, result)
            except ToneLabError as exc:
                status = HTTPStatus.NOT_FOUND if exc.code == "E_NOT_FOUND" else HTTPStatus.BAD_REQUEST
                self._send_json(status, exc.to_dict())
            except (KeyError, ValueError, TypeError) as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": {"code": "E_BAD_REQUEST", "message": str(exc)}})
            except Exception as exc:  # pragma: no cover - defensive boundary
                self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": {"code": "E_INTERNAL", "message": str(exc)}})

    return Handler


def create_server(
    workspace: str | Path,
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
    config_path: str | Path | None = None,
) -> ThreadingHTTPServer:
    config = Config.load(config_path) if config_path else None
    ws = Workspace(workspace, config=config) if config else Workspace(workspace)
    app = WorkbenchApp(ws)
    web_root = Path(__file__).resolve().parent / "web"
    handler = make_handler(app, web_root)
    server = ThreadingHTTPServer((host, int(port)), handler)
    server.daemon_threads = True
    return server


def serve(
    workspace: str | Path,
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
    config_path: str | Path | None = None,
    open_browser: bool = False,
) -> None:
    server = create_server(workspace, host=host, port=port, config_path=config_path)
    url = f"http://{host}:{port}/"
    print(f"ToneLab HTTP workbench: {url}")
    print(f"Workspace: {Path(workspace).resolve()}")
    if open_browser:
        import webbrowser

        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping HTTP workbench")
    finally:
        server.server_close()

