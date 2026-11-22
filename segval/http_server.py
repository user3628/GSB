"""Loopback-only HTTP API and dashboard server using Python's standard library."""

from __future__ import annotations

import ipaddress
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .canonical import canonical_bytes
from .demo_generator import generate_shard
from .diagnostics import checkpoint_fault_probe, doctor
from .engine import (
    collect_exports,
    fingerprints_payload,
    request_stop,
    resume_workbench,
    run_workbench,
    sample_view,
    status_payload,
    update_shard,
    verify_incremental,
)
from .errors import WorkbenchError
from .shards import import_shard
from .ui import INDEX_HTML
from .workspace import Workspace


MAX_BODY_BYTES = 20 * 1024 * 1024


def _json_bytes(value: Any) -> bytes:
    return canonical_bytes(value) + b"\n"


class WorkbenchServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, server_address: tuple[str, int], workspace: Path) -> None:
        self.workspace = Workspace(workspace).initialize()
        super().__init__(server_address, WorkbenchRequestHandler)


class WorkbenchRequestHandler(BaseHTTPRequestHandler):
    server_version = "SegValLocal/1.0"

    @property
    def workspace(self) -> Workspace:
        return self.server.workspace  # type: ignore[attr-defined]

    def log_message(self, format: str, *args: Any) -> None:
        print(f"[segval-http] {self.address_string()} {format % args}")

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, value: Any, status: int = 200) -> None:
        self._send(status, _json_bytes(value), "application/json; charset=utf-8")

    def _error(self, exc: WorkbenchError) -> None:
        self._json({"ok": False, "error": exc.to_dict()}, exc.http_status)

    def _read_json(self) -> Any:
        length_header = self.headers.get("Content-Length")
        if length_header is None:
            raise WorkbenchError("E_INVALID_REQUEST", "Content-Length is required.", http_status=400)
        try:
            length = int(length_header)
        except ValueError as exc:
            raise WorkbenchError("E_INVALID_REQUEST", "Invalid Content-Length.", http_status=400) from exc
        if length < 0 or length > MAX_BODY_BYTES:
            raise WorkbenchError(
                "E_REQUEST_TOO_LARGE",
                f"Request body must be <= {MAX_BODY_BYTES} bytes.",
                http_status=413,
            )
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise WorkbenchError(
                "E_INVALID_JSON",
                "Request body is not valid UTF-8 JSON.",
                {"error": str(exc)},
                http_status=400,
            ) from exc

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        try:
            if parsed.path in {"/", "/index.html"}:
                self._send(200, INDEX_HTML.encode("utf-8"), "text/html; charset=utf-8")
                return
            if parsed.path == "/api/status":
                self._json(status_payload(self.workspace.root))
                return
            if parsed.path == "/api/fingerprints":
                self._json(fingerprints_payload(self.workspace.root))
                return
            if parsed.path == "/api/budget":
                status = status_payload(self.workspace.root)
                self._json(
                    {
                        "budget": status["budget"],
                        "accepted": status["accepted"],
                        "unprocessed": status["unprocessed"],
                        "worker_lanes": status["worker_lanes"],
                        "planned_concurrency": status["planned_concurrency"],
                        "schedule_fingerprint": status["schedule_fingerprint"],
                    }
                )
                return
            if parsed.path == "/api/samples":
                query = parse_qs(parsed.query)
                self._json(
                    sample_view(
                        self.workspace.root,
                        shard_id=(query.get("shard_id") or [None])[0],
                        image_id=(query.get("image_id") or [None])[0],
                    )
                )
                return
            if parsed.path == "/api/export":
                query = parse_qs(parsed.query)
                kind = (query.get("kind") or ["bundle"])[0]
                if kind not in {"matches", "aggregates", "schedule", "diagnostics", "bundle"}:
                    raise WorkbenchError("E_INVALID_REQUEST", "Unknown export kind.", {"kind": kind}, 400)
                payloads = collect_exports(self.workspace.root, write=False)
                self._json(payloads[kind])
                return
            if parsed.path == "/api/doctor":
                self._json(doctor())
                return
            if parsed.path == "/api/diagnostics":
                self._json(
                    {
                        "doctor": doctor(),
                        "status": status_payload(self.workspace.root),
                        "fingerprints": fingerprints_payload(self.workspace.root),
                    }
                )
                return
            raise WorkbenchError("E_NOT_FOUND", "No such endpoint.", {"path": parsed.path}, 404)
        except WorkbenchError as exc:
            self._error(exc)
        except Exception as exc:  # pragma: no cover - defensive server boundary
            self._json(
                {
                    "ok": False,
                    "error": {
                        "code": "E_INTERNAL",
                        "message": f"{type(exc).__name__}: {exc}",
                    },
                },
                500,
            )

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        try:
            body = self._read_json()
            if parsed.path in {"/api/shards", "/api/import-shard"}:
                shard = body.get("shard", body) if isinstance(body, dict) else body
                result = import_shard(self.workspace.root, shard)
                self._json({"ok": True, "import": result})
                return
            if parsed.path == "/api/update-shard":
                shard = body.get("shard", body) if isinstance(body, dict) else body
                result = update_shard(
                    self.workspace.root,
                    shard,
                    workers=body.get("workers") if isinstance(body, dict) else None,
                )
                self._json(result)
                return
            if parsed.path == "/api/run":
                if not isinstance(body, dict):
                    raise WorkbenchError("E_INVALID_REQUEST", "Run body must be an object.")
                result = run_workbench(
                    self.workspace.root,
                    target_stage=str(body.get("stage", "all")),
                    workers=body.get("workers"),
                    stop_after_samples=body.get("stop_after_samples"),
                    clear_stop=bool(body.get("clear_stop", False)),
                )
                self._json(result)
                return
            if parsed.path == "/api/resume":
                result = resume_workbench(
                    self.workspace.root,
                    workers=body.get("workers") if isinstance(body, dict) else None,
                )
                self._json(result)
                return
            if parsed.path == "/api/stop":
                reason = body.get("reason", "http_stop") if isinstance(body, dict) else "http_stop"
                self._json(request_stop(self.workspace.root, str(reason)))
                return
            if parsed.path == "/api/config":
                if not isinstance(body, dict):
                    raise WorkbenchError("E_INVALID_REQUEST", "Config patch must be an object.")
                config = self.workspace.update_config(body)
                self._json({"ok": True, "config": config})
                return
            if parsed.path == "/api/verify-incremental":
                params = body if isinstance(body, dict) else {}
                report = verify_incremental(
                    self.workspace.root,
                    auto_rebuild=bool(params.get("auto_rebuild", True)),
                    workers=int(params.get("workers", 1)),
                    inject_mismatch=bool(params.get("inject_mismatch", False)),
                )
                self._json(report)
                return
            if parsed.path == "/api/init-demo":
                shard_count = int(body.get("shard_count", 4)) if isinstance(body, dict) else 4
                imported = []
                for shard_index in range(1, shard_count + 1):
                    imported.append(import_shard(self.workspace.root, generate_shard(shard_index)))
                self._json({"ok": True, "imported": imported})
                return
            if parsed.path == "/api/probe-checkpoints":
                self._json(checkpoint_fault_probe())
                return
            raise WorkbenchError("E_NOT_FOUND", "No such endpoint.", {"path": parsed.path}, 404)
        except WorkbenchError as exc:
            self._error(exc)
        except Exception as exc:  # pragma: no cover - defensive server boundary
            self._json(
                {
                    "ok": False,
                    "error": {
                        "code": "E_INTERNAL",
                        "message": f"{type(exc).__name__}: {exc}",
                    },
                },
                500,
            )


def serve(workspace: Path, *, host: str = "127.0.0.1", port: int = 8765) -> None:
    if host == "localhost":
        host = "127.0.0.1"
    try:
        address = ipaddress.ip_address(host)
    except ValueError as exc:
        raise WorkbenchError(
            "E_INVALID_HOST",
            "Only numeric loopback hosts are accepted.",
            {"host": host},
            400,
        ) from exc
    if not address.is_loopback:
        raise WorkbenchError(
            "E_INVALID_HOST",
            "HTTP server may bind only to a loopback address.",
            {"host": host},
            400,
        )
    try:
        server = WorkbenchServer((host, int(port)), workspace)
    except OSError as exc:
        raise WorkbenchError(
            "E_BIND_FAILED",
            "Unable to bind the loopback HTTP server.",
            {"host": host, "port": int(port), "error": str(exc)},
            409,
        ) from exc
    try:
        server.serve_forever()
    finally:
        server.server_close()

