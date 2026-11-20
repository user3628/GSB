"""Local HTTP JSON API and static application server."""

from __future__ import annotations

import json
import mimetypes
import os
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .canonical import canonical_json
from .errors import InvalidData, NotFound, WorkbenchError
from .workbench import Workbench

STATIC_DIR = Path(__file__).resolve().parent / "static"


class WorkbenchHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], workbench: Workbench) -> None:
        super().__init__(address, WorkbenchRequestHandler)
        self.workbench = workbench


class WorkbenchRequestHandler(BaseHTTPRequestHandler):
    server: WorkbenchHTTPServer

    def log_message(self, fmt: str, *args: Any) -> None:
        # Keep the terminal useful for demos without flooding it during polling.
        if self.path.startswith("/api/status"):
            return
        super().log_message(fmt, *args)

    def _send_bytes(
        self,
        payload: bytes,
        *,
        status: int = 200,
        content_type: str = "application/octet-stream",
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        for key, value in (extra_headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(payload)

    def _send_json(self, value: Any, status: int = 200) -> None:
        payload = (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode(
            "utf-8"
        )
        self._send_bytes(payload, status=status, content_type="application/json; charset=utf-8")

    def _error(self, error: WorkbenchError) -> None:
        self._send_json({"ok": False, "error": error.to_dict()}, status=error.http_status)

    def _body(self) -> dict[str, Any]:
        length_header = self.headers.get("Content-Length", "0")
        try:
            length = int(length_header)
        except ValueError as exc:
            raise InvalidData("Invalid Content-Length") from exc
        if length > 2 * 1024 * 1024:
            raise InvalidData("Request body is too large", details={"limit_bytes": 2097152})
        raw = self.rfile.read(length) if length else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise InvalidData("Request body must be valid JSON", details={"reason": str(exc)}) from exc
        if not isinstance(payload, dict):
            raise InvalidData("JSON request body must be an object")
        return payload

    def _query(self) -> dict[str, list[str]]:
        return parse_qs(urlparse(self.path).query)

    def _session_id(self, payload: dict[str, Any] | None = None) -> str:
        query = self._query()
        if "session_id" in query:
            return query["session_id"][0]
        if "session" in query:
            return query["session"][0]
        if payload and payload.get("session_id"):
            return str(payload["session_id"])
        return "default"

    def _serve_static(self, path: str) -> None:
        relative = "index.html" if path in {"", "/"} else path.lstrip("/")
        if relative.startswith("static/"):
            relative = relative[len("static/") :]
        target = (STATIC_DIR / relative).resolve()
        try:
            target.relative_to(STATIC_DIR.resolve())
        except ValueError as exc:
            raise NotFound("Static asset not found") from exc
        if not target.exists() or not target.is_file():
            raise NotFound("Static asset not found", details={"path": path})
        mime, _ = mimetypes.guess_type(target.name)
        self._send_bytes(target.read_bytes(), content_type=mime or "application/octet-stream")

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        wb = self.server.workbench
        try:
            if path == "/api/health":
                self._send_json(
                    {
                        "ok": True,
                        "service": "deterministic-regularized-linear-workbench",
                        "schema_version": 1,
                        "workspace": str(wb.root),
                        "workspace_writable": os.access(wb.root, os.W_OK),
                    }
                )
            elif path == "/api/datasets":
                self._send_json({"ok": True, "datasets": wb.datasets()})
            elif path == "/api/session":
                self._send_json(
                    {"ok": True, "session": wb.get_session(self._session_id(), include_run=True)}
                )
            elif path == "/api/status":
                self._send_json({"ok": True, "status": wb.status(self._session_id())})
            elif path == "/api/folds":
                self._send_json({"ok": True, **wb.folds(self._session_id())})
            elif path == "/api/fingerprints":
                self._send_json(
                    {"ok": True, **wb.fingerprints(self._session_id())}
                )
            elif path == "/api/invalidations":
                self._send_json(
                    {"ok": True, **wb.invalidations(self._session_id())}
                )
            elif path == "/api/checkpoints":
                self._send_json(
                    {"ok": True, **wb.checkpoints(self._session_id())}
                )
            elif path in {"/api/export/coefficients", "/api/result"}:
                result = wb.result(self._session_id())
                self._send_json(
                    {
                        "ok": True,
                        "run_id": wb.status(self._session_id()).get("active_run_id"),
                        "result": result,
                        "coefficients": [
                            {"name": name, "value": value}
                            for name, value in zip(result["feature_names"], result["coefficients"])
                        ],
                        "intercept": result["intercept"],
                    }
                )
            elif path == "/api/export/trajectory":
                query = self._query()
                task_index = int(query["task_index"][0]) if "task_index" in query else None
                self._send_json(
                    {
                        "ok": True,
                        **wb.trajectory(self._session_id(), task_index=task_index),
                    }
                )
            elif path == "/api/summary":
                self._send_json({"ok": True, "summary": wb.summary(self._session_id())})
            elif path.startswith("/api/"):
                raise NotFound("Unknown API route", details={"path": path})
            else:
                self._serve_static(path)
        except WorkbenchError as exc:
            self._error(exc)
        except (ValueError, TypeError) as exc:
            self._error(InvalidData(str(exc)))
        except Exception as exc:  # pragma: no cover - boundary
            self._error(
                WorkbenchError(
                    "Unexpected server failure",
                    details={"type": type(exc).__name__, "reason": str(exc)},
                )
            )

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        wb = self.server.workbench
        try:
            payload = self._body()
            session_id = self._session_id(payload)
            if path in {"/api/session/config", "/api/config"}:
                patch = payload.get("patch") or payload.get("config") or {
                    key: value for key, value in payload.items() if key != "session_id"
                }
                result = wb.update_config(session_id, patch)
                self._send_json({"ok": True, **result})
            elif path == "/api/import":
                csv_path = payload.get("csv_path")
                dataset_id = payload.get("dataset_id")
                if not csv_path or not dataset_id:
                    raise InvalidData("csv_path and dataset_id are required")
                result = wb.import_dataset(
                    csv_path,
                    str(dataset_id),
                    target=payload.get("target"),
                    features=payload.get("features"),
                    override=bool(payload.get("override", False)),
                )
                self._send_json({"ok": True, "dataset": result}, status=201)
            elif path == "/api/train":
                config_patch = (
                    payload.get("config_patch")
                    or payload.get("patch")
                    or payload.get("config")
                )
                result = wb.start_training(
                    session_id,
                    config_patch=config_patch,
                    stop_after_tasks=payload.get("stop_after_tasks"),
                    background=bool(payload.get("background", True)),
                )
                status = HTTPStatus.ACCEPTED if result.get("status") == "running" else 200
                self._send_json({"ok": True, **result}, status=int(status))
            elif path == "/api/stop":
                self._send_json(
                    {"ok": True, **wb.request_stop(session_id)}, status=202
                )
            elif path == "/api/resume":
                result = wb.resume(
                    session_id,
                    stop_after_tasks=payload.get("stop_after_tasks"),
                    background=bool(payload.get("background", True)),
                )
                status = HTTPStatus.ACCEPTED if result.get("status") == "running" else 200
                self._send_json({"ok": True, **result}, status=int(status))
            else:
                raise NotFound("Unknown API route", details={"path": path})
        except WorkbenchError as exc:
            self._error(exc)
        except (ValueError, TypeError) as exc:
            self._error(InvalidData(str(exc)))
        except Exception as exc:  # pragma: no cover - boundary
            self._error(
                WorkbenchError(
                    "Unexpected server failure",
                    details={"type": type(exc).__name__, "reason": str(exc)},
                )
            )

    def do_PUT(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path not in {"/api/config", "/api/session/config"}:
            self._error(NotFound("Unknown API route", details={"path": path}))
            return
        self.do_POST()


def create_server(
    workspace: str | Path,
    *,
    data_dir: str | Path | None = None,
    host: str = "127.0.0.1",
    port: int = 8765,
) -> WorkbenchHTTPServer:
    workbench = Workbench(workspace, data_dir=data_dir)
    return WorkbenchHTTPServer((host, port), workbench)
