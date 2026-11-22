"""Local-only HTTP API and dashboard server."""

from __future__ import annotations

import json
import mimetypes
import re
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .canonical import read_json
from .config import Settings
from .errors import E_INVALID_STAGE, E_NOT_FOUND, WorkbenchError, raise_error
from .generator import generate_demo
from .workbench import Workbench


_SAFE_SEQUENCE = re.compile(r"^[A-Za-z0-9_-]+$")


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "LocalTrackWorkbench/1.0"
    protocol_version = "HTTP/1.1"
    workbench: Workbench
    settings: Settings

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        # Keep terminal output deterministic and quiet for a local dashboard.
        return

    def _send(self, status: int, payload: bytes, content_type: str, *, cache: str = "no-store") -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", cache)
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(payload)

    def _json(self, status: int, value: Any) -> None:
        payload = json.dumps(value, sort_keys=True, ensure_ascii=False, indent=2).encode("utf-8") + b"\n"
        self._send(status, payload, "application/json; charset=utf-8")

    def _error(self, error: WorkbenchError) -> None:
        self._json(error.http_status, error.to_dict())

    def _body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length <= 0:
            return {}
        if length > 2 * 1024 * 1024:
            raise_error("E_IO", "request body is too large", context={"max_bytes": 2 * 1024 * 1024})
        try:
            value = json.loads(self.rfile.read(length).decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            raise_error("E_IO", "request body is not valid JSON", context={"detail": str(exc)})
        if not isinstance(value, dict):
            raise_error("E_IO", "request JSON root must be an object")
        return value

    def _static(self, relative: str) -> None:
        static_root = (Path(__file__).parent / "static").resolve()
        target = (static_root / relative).resolve()
        try:
            target.relative_to(static_root)
        except ValueError:
            raise_error(E_NOT_FOUND, "static path is outside the static directory")
        if not target.is_file():
            raise_error(E_NOT_FOUND, "static file was not found", context={"path": relative})
        content_type = mimetypes.guess_type(target.name)[0] or "application/octet-stream"
        if target.suffix in {".html", ".css", ".js"}:
            content_type += "; charset=utf-8"
        self._send(200, target.read_bytes(), content_type)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        try:
            if path in {"/", "/index.html"}:
                self._static("index.html")
                return
            if path.startswith("/static/"):
                self._static(path[len("/static/") :])
                return
            if path == "/api/health":
                self._json(200, {"ok": True, "service": "local-track-workbench", "version": "1.0"})
                return
            if path == "/api/sequences":
                self._json(200, self.workbench.list_sequences())
                return
            match = re.fullmatch(r"/api/sequences/([A-Za-z0-9_-]+)", path)
            if match:
                self._json(200, self.workbench.sequence_status(match.group(1)))
                return
            match = re.fullmatch(r"/api/sequences/([A-Za-z0-9_-]+)/frame/(\d+)", path)
            if match:
                self._json(200, self.workbench.frame_payload(match.group(1), int(match.group(2))))
                return
            match = re.fullmatch(r"/api/sequences/([A-Za-z0-9_-]+)/diagnostics/(\d+)", path)
            if match:
                self._json(200, self.workbench.diagnose_frame(match.group(1), int(match.group(2))))
                return
            match = re.fullmatch(r"/api/sequences/([A-Za-z0-9_-]+)/result", path)
            if match:
                self._json(200, {"ok": True, "bundle": self.workbench.build_sequence_bundle(match.group(1))})
                return
            if path == "/api/invalidations":
                query = parse_qs(parsed.query)
                sequence_id = query.get("sequence_id", [None])[0]
                self._json(200, self.workbench.invalidations(sequence_id))
                return
            if path == "/api/report":
                self._json(200, self.workbench.build_report())
                return
            raise_error(E_NOT_FOUND, "HTTP route was not found", context={"path": path})
        except WorkbenchError as exc:
            self._error(exc)
        except Exception as exc:  # pragma: no cover - defensive boundary
            self._json(500, {"ok": False, "error": {"code": "E_IO", "message": "unexpected server error", "context": {"detail": str(exc)}}})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        try:
            body = self._body()
            if path == "/api/import":
                manifest_path = body.get("manifest_path")
                if not isinstance(manifest_path, str):
                    raise_error("E_MANIFEST", "manifest_path is required")
                result = self.workbench.import_manifest(manifest_path, source_data_dir=body.get("source_data_dir"))
                self._json(200, result)
                return
            if path == "/api/generate":
                result = generate_demo(self.settings, force=bool(body.get("force", False)))
                self.workbench.reload_manifest()
                self._json(200, result)
                return
            if path == "/api/run":
                sequence_id = body.get("sequence_id")
                if not isinstance(sequence_id, str):
                    raise_error(E_INVALID_STAGE, "sequence_id is required")
                stage = str(body.get("stage", "all"))
                stop_after = body.get("stop_after")
                if stop_after is not None:
                    stop_after = int(stop_after)
                self._json(200, self.workbench.run_stage(sequence_id, stage, stop_after=stop_after))
                return
            if path == "/api/stop":
                sequence_id = body.get("sequence_id")
                if not isinstance(sequence_id, str):
                    raise_error(E_INVALID_STAGE, "sequence_id is required")
                self._json(200, self.workbench.request_stop(sequence_id, reason=str(body.get("reason", "HTTP stop request"))))
                return
            if path == "/api/resume":
                sequence_id = body.get("sequence_id")
                if not isinstance(sequence_id, str):
                    raise_error(E_INVALID_STAGE, "sequence_id is required")
                self._json(200, self.workbench.resume_sequence(sequence_id))
                return
            if path == "/api/report":
                self._json(200, self.workbench.build_report())
                return
            raise_error(E_NOT_FOUND, "HTTP route was not found", context={"path": path})
        except WorkbenchError as exc:
            self._error(exc)
        except (TypeError, ValueError) as exc:
            error = WorkbenchError("E_CONFIG", "request parameter has an invalid type", context={"detail": str(exc)})
            self._error(error)
        except Exception as exc:  # pragma: no cover - defensive boundary
            self._json(500, {"ok": False, "error": {"code": "E_IO", "message": "unexpected server error", "context": {"detail": str(exc)}}})


def build_server(settings: Settings, host: str | None = None, port: int | None = None) -> ThreadingHTTPServer:
    workbench = Workbench(settings)
    handler = type(
        "BoundApiHandler",
        (ApiHandler,),
        {"workbench": workbench, "settings": settings},
    )
    bind_host = host if host is not None else str(settings.server.get("host", "127.0.0.1"))
    bind_port = int(port if port is not None else settings.server.get("port", 8765))
    server = ThreadingHTTPServer((bind_host, bind_port), handler)
    server.daemon_threads = True
    return server


def serve(settings: Settings, host: str | None = None, port: int | None = None) -> None:
    server = build_server(settings, host, port)
    actual_host, actual_port = server.server_address[:2]
    print(json.dumps({"ok": True, "url": f"http://{actual_host}:{actual_port}/", "host": actual_host, "port": actual_port}, ensure_ascii=False))
    try:
        server.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        server.server_close()
