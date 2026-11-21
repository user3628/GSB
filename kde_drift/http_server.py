"""Local-only JSON API and static workstation server."""

from __future__ import annotations

import json
import mimetypes
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import numpy as np

from .batch import Batch
from .canonical import canonical_json_bytes, json_safe
from .errors import WorkbenchError
from .service import ERROR_CATALOG, WorkbenchService


STATIC_DIR = Path(__file__).with_name("static")


class WorkbenchHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], service: WorkbenchService) -> None:
        self.service = service
        super().__init__(address, WorkbenchRequestHandler)


class WorkbenchRequestHandler(BaseHTTPRequestHandler):
    server: WorkbenchHTTPServer
    server_version = "KDEDriftWorkbench/1.0"

    def log_message(self, format_string: str, *args: Any) -> None:
        print(f"[http] {self.address_string()} {format_string % args}")

    def _send_bytes(
        self,
        data: bytes,
        *,
        status: int = 200,
        content_type: str = "application/json; charset=utf-8",
        headers: dict[str, str] | None = None,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        if headers:
            for key, value in headers.items():
                self.send_header(key, value)
        self.end_headers()
        self.wfile.write(data)

    def _send_json(self, value: Any, *, status: int = 200) -> None:
        safe = json_safe(value)
        self._send_bytes(canonical_json_bytes(safe, strict=True), status=status)

    def _error(self, exc: WorkbenchError) -> None:
        self._send_json(exc.to_dict(), status=exc.http_status)

    def _read_json(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise WorkbenchError("E_CONFIG", "invalid Content-Length", location="http.body") from exc
        if length > 64 * 1024 * 1024:
            raise WorkbenchError(
                "E_CONFIG", "request body is too large", location="http.body", http_status=413
            )
        raw = self.rfile.read(length) if length else b"{}"
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise WorkbenchError(
                "E_CONFIG", "request body is not valid JSON", location="http.body", details={"reason": str(exc)}
            ) from exc
        if not isinstance(value, dict):
            raise WorkbenchError("E_CONFIG", "JSON request body must be an object", location="http.body")
        return value

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/api/health":
                self._send_json(
                    {
                        "ok": True,
                        "service": "kde-drift-workbench",
                        "bind": "127.0.0.1",
                        "numpy": np.__version__,
                    }
                )
                return
            if parsed.path == "/api/datasets":
                self._send_json({"ok": True, "datasets": self.server.service.datasets()})
                return
            if parsed.path == "/api/config/default":
                self._send_json({"ok": True, "config": self.server.service.default_config()})
                return
            if parsed.path == "/api/session":
                self._send_json({"ok": True, "session": self.server.service.session()})
                return
            if parsed.path == "/api/status":
                self._send_json({"ok": True, **self.server.service.status()})
                return
            if parsed.path == "/api/invalidation":
                self._send_json(self.server.service.invalidation())
                return
            if parsed.path == "/api/results":
                query = parse_qs(parsed.query)
                stage = query.get("stage", [None])[0]
                include_full = query.get("full", ["1"])[0] not in {"0", "false", "no"}
                self._send_json(self.server.service.results(stage=stage, include_full=include_full))
                return
            if parsed.path == "/api/export/density":
                self._send_bytes(
                    self.server.service.export("density"),
                    headers={"Content-Disposition": 'attachment; filename="density_curves.json"'},
                )
                return
            if parsed.path == "/api/export/report":
                self._send_bytes(
                    self.server.service.export("report"),
                    headers={"Content-Disposition": 'attachment; filename="drift_report.json"'},
                )
                return
            if parsed.path == "/api/error-codes":
                self._send_json({"ok": True, "errors": ERROR_CATALOG})
                return
            if parsed.path.startswith("/api/"):
                raise WorkbenchError(
                    "E_NOT_FOUND", f"unknown API route: {parsed.path}", location="http.route"
                )
            self._serve_static(parsed.path)
        except WorkbenchError as exc:
            self._error(exc)
        except Exception as exc:  # pragma: no cover - defensive HTTP boundary
            self._send_json(
                WorkbenchError(
                    "E_IO",
                    f"unhandled server error: {exc}",
                    location="http.handler",
                    details={"type": type(exc).__name__},
                ).to_dict(),
                status=500,
            )

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/api/session":
                body = self._read_json()
                session = self.server.service.set_session(
                    dataset=str(body.get("dataset", "small")),
                    config=body.get("config"),
                )
                self._send_json({"ok": True, "session": session})
                return
            if parsed.path == "/api/run":
                body = self._read_json()
                batches = self._parse_batches(body.get("batches"))
                result = self.server.service.start_run(
                    dataset=body.get("dataset"),
                    config=body.get("config"),
                    resume=bool(body.get("resume", True)),
                    force=bool(body.get("force", False)),
                    batch_size=self._optional_int(body.get("batch_size")),
                    batch_order=str(body.get("batch_order", "forward")),
                    current_batches=batches,
                )
                self._send_json({"ok": True, "run": result}, status=202)
                return
            if parsed.path == "/api/stop":
                self._read_json()
                self._send_json({"ok": True, **self.server.service.stop()}, status=202)
                return
            if parsed.path == "/api/resume":
                body = self._read_json()
                result = self.server.service.start_run(
                    dataset=body.get("dataset"),
                    config=body.get("config"),
                    resume=True,
                    force=False,
                    batch_size=self._optional_int(body.get("batch_size")),
                    batch_order=str(body.get("batch_order", "forward")),
                )
                self._send_json({"ok": True, "run": result}, status=202)
                return
            raise WorkbenchError(
                "E_NOT_FOUND", f"unknown API route: {parsed.path}", location="http.route"
            )
        except WorkbenchError as exc:
            self._error(exc)
        except Exception as exc:  # pragma: no cover - defensive HTTP boundary
            self._send_json(
                WorkbenchError(
                    "E_IO",
                    f"unhandled server error: {exc}",
                    location="http.handler",
                    details={"type": type(exc).__name__},
                ).to_dict(),
                status=500,
            )

    def _optional_int(self, value: Any) -> int | None:
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise WorkbenchError("E_CONFIG", "batch_size must be an integer", location="batch_size") from exc

    def _parse_batches(self, value: Any) -> list[np.ndarray | Batch] | None:
        if value is None:
            return None
        if not isinstance(value, list):
            raise WorkbenchError(
                "E_INVALID_BATCH", "batches must be a JSON array", stage="batch_ingest", location="http.batches"
            )
        batches: list[np.ndarray | Batch] = []
        for index, item in enumerate(value):
            if isinstance(item, dict):
                features = item.get("features")
                row_indices = item.get("row_indices")
                source_labels = item.get("source_labels")
            else:
                features = item
                row_indices = None
                source_labels = None
            try:
                matrix = np.asarray(features, dtype=np.float64)
                indices = None if row_indices is None else np.asarray(row_indices, dtype=np.int64)
                labels = None if source_labels is None else np.asarray(source_labels)
            except (TypeError, ValueError) as exc:
                raise WorkbenchError(
                    "E_INVALID_BATCH",
                    "batch features are not numeric",
                    stage="batch_ingest",
                    location=f"http.batches[{index}].features",
                    details={"batch_index": index},
                ) from exc
            batches.append(Batch(matrix, indices, labels))
        return batches

    def _serve_static(self, request_path: str) -> None:
        relative = "index.html" if request_path in {"", "/"} else request_path.lstrip("/")
        candidate = (STATIC_DIR / relative).resolve()
        try:
            candidate.relative_to(STATIC_DIR.resolve())
        except ValueError:
            raise WorkbenchError("E_NOT_FOUND", "static path is invalid", location="http.static")
        if not candidate.is_file():
            raise WorkbenchError(
                "E_NOT_FOUND", f"static file not found: {relative}", location="http.static"
            )
        content_type = mimetypes.guess_type(candidate.name)[0] or "application/octet-stream"
        if content_type.startswith("text/") or content_type in {"application/javascript", "application/json"}:
            content_type += "; charset=utf-8"
        self._send_bytes(candidate.read_bytes(), content_type=content_type)


def create_server(
    service: WorkbenchService,
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
) -> WorkbenchHTTPServer:
    if host != "127.0.0.1":
        raise WorkbenchError(
            "E_CONFIG", "HTTP workbench may bind only to 127.0.0.1", location="server.host"
        )
    return WorkbenchHTTPServer((host, port), service)
