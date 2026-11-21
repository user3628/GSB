"""Local stdlib HTTP application and JSON API."""

from __future__ import annotations

import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from .errors import BusinessError, error
from .pipeline import Pipeline
from .search import corpus_diagnostics, run_search
from .util import canonical_bytes
from .verification import verify_incremental
from .workspace import Workspace


WEB_DIR = Path(__file__).resolve().parent / "web"
MAX_BODY_BYTES = 1 << 20


class ShardSeekHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, server_address: tuple[str, int], workspace: Workspace, project_root: Path) -> None:
        super().__init__(server_address, ShardSeekRequestHandler)
        self.workspace = workspace
        self.project_root = project_root


class ShardSeekRequestHandler(BaseHTTPRequestHandler):
    server: ShardSeekHTTPServer
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:
        # Keep the terminal useful during interactive use without noisy request logs.
        return

    def _send(self, status: int, body: bytes, content_type: str, *, cache: bool = False) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Cache-Control", "public, max-age=300" if cache else "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status: int, payload: Any) -> None:
        self._send(status, canonical_bytes(payload), "application/json; charset=utf-8")

    def _error(self, exc: BusinessError) -> None:
        self._json(exc.http_status, exc.to_dict())

    def _body(self) -> dict[str, Any]:
        length_header = self.headers.get("Content-Length", "0")
        try:
            length = int(length_header)
        except ValueError as exc:
            raise error("E_INVALID_REQUEST", "Content-Length is not an integer", stage="http") from exc
        if length < 0 or length > MAX_BODY_BYTES:
            raise error("E_INVALID_REQUEST", "Request body is too large", stage="http", details={"length": length})
        if length == 0:
            return {}
        try:
            value = json.loads(self.rfile.read(length).decode("utf-8"))
        except Exception as exc:
            raise error("E_INVALID_REQUEST", "Request body is not valid JSON", stage="http") from exc
        if not isinstance(value, dict):
            raise error("E_INVALID_REQUEST", "Request body must be a JSON object", stage="http")
        return value

    def _serve_asset(self, filename: str, content_type: str) -> None:
        path = WEB_DIR / filename
        if not path.is_file():
            self._json(HTTPStatus.NOT_FOUND, {"ok": False, "error": {"code": "E_NOT_FOUND", "stage": "http"}})
            return
        self._send(HTTPStatus.OK, path.read_bytes(), content_type, cache=True)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        try:
            if parsed.path in ("/", "/index.html"):
                self._serve_asset("index.html", "text/html; charset=utf-8")
            elif parsed.path == "/assets/styles.css":
                self._serve_asset("styles.css", "text/css; charset=utf-8")
            elif parsed.path == "/assets/app.js":
                self._serve_asset("app.js", "text/javascript; charset=utf-8")
            elif parsed.path == "/api/health":
                self._json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "service": "shardseek",
                        "version": __import__("shardseek").__version__,
                    },
                )
            elif parsed.path == "/api/status":
                self._json(HTTPStatus.OK, self.server.workspace.status())
            elif parsed.path == "/api/fingerprints":
                self._json(HTTPStatus.OK, self.server.workspace.fingerprints())
            elif parsed.path == "/api/diagnostics":
                self._json(HTTPStatus.OK, corpus_diagnostics(self.server.workspace))
            elif parsed.path.startswith("/api/image/"):
                item_id = unquote(parsed.path[len("/api/image/") :])
                try:
                    path = self.server.workspace.image_path(item_id)
                except BusinessError as exc:
                    self._error(exc)
                    return
                self._send(HTTPStatus.OK, path.read_bytes(), "image/png", cache=True)
            else:
                self._json(
                    HTTPStatus.NOT_FOUND,
                    {"ok": False, "error": {"code": "E_NOT_FOUND", "message": "Route not found", "stage": "http"}},
                )
        except BusinessError as exc:
            self._error(exc)
        except Exception as exc:
            self._json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {
                    "ok": False,
                    "error": {
                        "code": "E_INTERNAL",
                        "message": str(exc),
                        "stage": "http",
                        "type": type(exc).__name__,
                    },
                },
            )

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        try:
            body = self._body()
            workspace = self.server.workspace
            def manifest_from_body() -> str:
                requested = Path(body.get("manifest_path", "samples/manifest.jsonl"))
                return str(requested if requested.is_absolute() else self.server.project_root / requested)
            if parsed.path == "/api/init":
                if body.get("clean"):
                    workspace.clean()
                else:
                    workspace.init()
                self._json(HTTPStatus.OK, workspace.status())
            elif parsed.path == "/api/reset":
                workspace.clean()
                imported = workspace.import_manifest(manifest_from_body())
                config = workspace.set_config(body.get("config", "baseline_8bit"))
                self._json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "import": {
                            "item_count": imported["item_count"],
                            "enabled_count": imported["enabled_count"],
                            "rejected": imported["rejected"],
                        },
                        "config": config.get("name"),
                    },
                )
            elif parsed.path == "/api/import":
                imported = workspace.import_manifest(manifest_from_body())
                self._json(HTTPStatus.OK, {"ok": True, "import": imported})
            elif parsed.path == "/api/config":
                config = workspace.set_config(body["config"])
                self._json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "config": {
                            "name": config.get("name"),
                            "bits": config["quantization"]["bits"],
                        },
                    },
                )
            elif parsed.path == "/api/build":
                result = Pipeline(workspace).build(
                    mode=body.get("mode", "incremental"),
                    stop_after_shards=body.get("stop_after_shards"),
                    config_name=body.get("config"),
                )
                self._json(HTTPStatus.OK, result)
            elif parsed.path == "/api/resume":
                result = Pipeline(workspace).build(
                    mode="resume",
                    stop_after_shards=body.get("stop_after_shards"),
                )
                self._json(HTTPStatus.OK, result)
            elif parsed.path == "/api/stop":
                self._json(HTTPStatus.OK, {"ok": True, "control": workspace.request_stop()})
            elif parsed.path == "/api/search":
                report = run_search(
                    workspace,
                    direction=body.get("direction"),
                    query_text=body.get("query_text"),
                    image_item_id=body.get("image_item_id"),
                    top_k=body.get("top_k", 5),
                    weights=body.get("weights"),
                )
                self._json(HTTPStatus.OK, report)
            elif parsed.path == "/api/verify-incremental":
                result = verify_incremental(
                    parent_root=workspace.root / "verification" / "http",
                    manifest_path=manifest_from_body(),
                    project_root=self.server.project_root,
                    baseline_config=body.get("baseline_config", "baseline_8bit"),
                    changed_config=body.get("changed_config", "compact_4bit"),
                )
                self._json(HTTPStatus.OK, result)
            else:
                self._json(
                    HTTPStatus.NOT_FOUND,
                    {"ok": False, "error": {"code": "E_NOT_FOUND", "message": "Route not found", "stage": "http"}},
                )
        except BusinessError as exc:
            self._error(exc)
        except KeyError as exc:
            self._json(
                HTTPStatus.BAD_REQUEST,
                {
                    "ok": False,
                    "error": {
                        "code": "E_INVALID_REQUEST",
                        "message": "Missing request field",
                        "stage": "http",
                        "field": str(exc),
                    },
                },
            )
        except Exception as exc:
            self._json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {
                    "ok": False,
                    "error": {
                        "code": "E_INTERNAL",
                        "message": str(exc),
                        "stage": "http",
                        "type": type(exc).__name__,
                    },
                },
            )


def serve(root: str | Path, *, project_root: str | Path = ".", host: str = "127.0.0.1", port: int = 8767) -> None:
    project = Path(project_root).resolve()
    workspace = Workspace(root, project_root=project).init()
    server = ShardSeekHTTPServer((host, port), workspace, project)
    print(f"ShardSeek UI: http://{host}:{server.server_address[1]}/", flush=True)
    try:
        server.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()

