"""Small stdlib HTTP workbench for local integration and demonstrations."""

from __future__ import annotations

import base64
import binascii
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .config import validate_config
from .engine import Engine
from .errors import CabinVoiceError
from .manifest import zone_ids as manifest_zone_ids
from .storage import Workspace
from .timeline import EditModel
from .util import deep_merge, nested_set


MAX_BODY_BYTES = 32 * 1024 * 1024


class CabinHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple[str, int], workspace: Workspace):
        super().__init__(address, CabinRequestHandler)
        self.workspace = workspace


class CabinRequestHandler(BaseHTTPRequestHandler):
    server: CabinHTTPServer
    server_version = "CabinVoice/1.0"

    def log_message(self, format_string: str, *args: Any) -> None:  # noqa: A003
        # Keep local demos quiet; tests can observe responses directly.
        return

    @property
    def workspace(self) -> Workspace:
        return self.server.workspace

    def _send(self, status: int, payload: Any) -> None:
        data = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise CabinVoiceError("E_BAD_REQUEST", "invalid Content-Length", {}) from exc
        if length < 0 or length > MAX_BODY_BYTES:
            raise CabinVoiceError(
                "E_BAD_REQUEST", "request body size is invalid", {"length": length, "max": MAX_BODY_BYTES}
            )
        if length == 0:
            return {}
        data = self.rfile.read(length)
        try:
            payload = json.loads(data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise CabinVoiceError("E_BAD_REQUEST", "request body must be JSON", {}) from exc
        if not isinstance(payload, dict):
            raise CabinVoiceError("E_BAD_REQUEST", "request JSON must be an object", {})
        return payload

    def _handle(self, action) -> None:
        try:
            status, payload = action()
            self._send(status, payload)
        except CabinVoiceError as exc:
            self._send(exc.http_status, exc.as_dict())
        except Exception as exc:  # pragma: no cover - defensive boundary
            self._send(
                500,
                {
                    "ok": False,
                    "code": "E_INTERNAL",
                    "message": "unexpected server error",
                    "details": {"type": type(exc).__name__, "error": str(exc)},
                },
            )

    def do_GET(self) -> None:  # noqa: N802
        route = urlparse(self.path).path

        def action():
            if route == "/health":
                return 200, {"ok": True, "service": "cabin-voice", "workspace": str(self.workspace.root)}
            if route == "/status":
                return 200, Engine(self.workspace).status()
            if route == "/fingerprints":
                return 200, Engine(self.workspace).fingerprints()
            if route == "/result":
                return 200, {"ok": True, "result": Engine(self.workspace).result()}
            if route == "/checkpoints":
                log = Engine(self.workspace).checkpoints
                scan = log.load_consistent()
                return 200, {
                    "ok": True,
                    "records": scan["prefix"],
                    "invalid_records": scan["invalid"],
                }
            return 404, {"ok": False, "code": "E_NOT_FOUND", "message": "unknown route", "details": {"path": route}}

        self._handle(action)

    def do_POST(self) -> None:  # noqa: N802
        route = urlparse(self.path).path

        def action():
            body = self._body()
            if route == "/import":
                manifest_path = body.get("manifest_path")
                if not manifest_path:
                    raise CabinVoiceError("E_BAD_REQUEST", "manifest_path is required", {})
                imported = self.workspace.import_bundle(manifest_path)
                config_path = body.get("config_path")
                if config_path:
                    config = json.loads(Path(config_path).read_text(encoding="utf-8"))
                    manifest = self.workspace.require_manifest()
                    validate_config(config, manifest_zone_ids(manifest))
                    self.workspace.save_config(config)
                return 200, {"ok": True, **imported}
            if route == "/build":
                if body.get("manifest_path"):
                    self.workspace.import_bundle(body["manifest_path"])
                result = Engine(self.workspace).build(
                    mode=str(body.get("mode", "incremental")),
                    resume=bool(body.get("resume", False)),
                    verify=bool(body.get("verify", False)),
                    stop_after_units=(
                        int(body["stop_after_units"]) if body.get("stop_after_units") is not None else None
                    ),
                )
                return 200, result
            if route == "/stop":
                return 200, {"ok": True, **self.workspace.request_stop()}
            if route == "/recover":
                result = Engine(self.workspace).build(
                    mode="incremental", resume=True, verify=bool(body.get("verify", False))
                )
                return 200, result
            if route == "/verify/full":
                return 200, {"ok": True, **Engine(self.workspace).verify_full_consistency()}
            if route in {"/samples/replace", "/import/clip"}:
                chunk_id = body.get("chunk_id")
                if not chunk_id:
                    raise CabinVoiceError("E_BAD_REQUEST", "chunk_id is required", {})
                if body.get("wav_base64"):
                    try:
                        data = base64.b64decode(body["wav_base64"], validate=True)
                    except (binascii.Error, ValueError) as exc:
                        raise CabinVoiceError("E_BAD_REQUEST", "wav_base64 is invalid", {}) from exc
                    replacement = self.workspace.replace_chunk_sample_bytes(chunk_id, data)
                elif body.get("wav_path"):
                    replacement = self.workspace.replace_chunk_sample_file(chunk_id, body["wav_path"])
                else:
                    raise CabinVoiceError("E_BAD_REQUEST", "wav_base64 or wav_path is required", {})
                build = Engine(self.workspace).build(
                    mode="incremental", verify=bool(body.get("verify", True))
                )
                return 200, {"ok": True, "replacement": replacement, "build": build}
            if route == "/config":
                if body.get("patch") is not None:
                    config = deep_merge(self.workspace.load_config(), body["patch"])
                    path = "<patch>"
                elif body.get("path"):
                    config = nested_set(
                        self.workspace.load_config(), str(body["path"]), body.get("value")
                    )
                    path = str(body["path"])
                else:
                    raise CabinVoiceError("E_BAD_REQUEST", "path/value or patch is required", {})
                manifest = self.workspace.require_manifest()
                validate_config(config, manifest_zone_ids(manifest))
                self.workspace.save_config(config)
                response: dict[str, Any] = {"ok": True, "path": path}
                if bool(body.get("build", True)):
                    response["build"] = Engine(self.workspace).build(
                        mode="incremental", verify=bool(body.get("verify", True))
                    )
                return 200, response
            if route == "/edits/priority":
                manifest = self.workspace.require_manifest()
                edits = EditModel(self.workspace.edits_path, manifest)
                try:
                    priority = int(body.get("priority"))
                except (TypeError, ValueError) as exc:
                    raise CabinVoiceError("E_BAD_REQUEST", "priority must be an integer", {}) from exc
                entry = edits.set_priority(str(body.get("zone_id")), priority)
                return 200, {"ok": True, "edit": entry}
            if route == "/edits/lexicon-hits":
                manifest = self.workspace.require_manifest()
                edits = EditModel(self.workspace.edits_path, manifest)
                entry = edits.set_hits(
                    str(body.get("chunk_id")), str(body.get("zone_id")), list(body.get("hits", []))
                )
                return 200, {"ok": True, "edit": entry}
            if route == "/edits/split-activity":
                manifest = self.workspace.require_manifest()
                edits = EditModel(self.workspace.edits_path, manifest)
                try:
                    at_ms = int(body.get("at_ms"))
                except (TypeError, ValueError) as exc:
                    raise CabinVoiceError("E_BAD_REQUEST", "at_ms must be an integer", {}) from exc
                entry = edits.split_activity(
                    str(body.get("chunk_id")), str(body.get("zone_id")), at_ms
                )
                return 200, {"ok": True, "edit": entry}
            if route == "/edits/merge-activity":
                manifest = self.workspace.require_manifest()
                edits = EditModel(self.workspace.edits_path, manifest)
                try:
                    left_start_ms = int(body.get("left_start_ms"))
                    right_start_ms = int(body.get("right_start_ms"))
                except (TypeError, ValueError) as exc:
                    raise CabinVoiceError(
                        "E_BAD_REQUEST", "left_start_ms and right_start_ms must be integers", {}
                    ) from exc
                entry = edits.merge_activity(
                    str(body.get("chunk_id")),
                    str(body.get("zone_id")),
                    left_start_ms,
                    right_start_ms,
                )
                return 200, {"ok": True, "edit": entry}
            return 404, {"ok": False, "code": "E_NOT_FOUND", "message": "unknown route", "details": {"path": route}}

        self._handle(action)


def create_http_server(workspace: Workspace, host: str = "127.0.0.1", port: int = 8765) -> CabinHTTPServer:
    workspace.ensure()
    return CabinHTTPServer((host, port), workspace)


def serve(workspace: Workspace, host: str = "127.0.0.1", port: int = 8765) -> None:
    server = create_http_server(workspace, host, port)
    try:
        server.serve_forever()
    finally:
        server.server_close()


def start_background_server(
    workspace: Workspace, host: str = "127.0.0.1", port: int = 0
) -> tuple[CabinHTTPServer, threading.Thread]:
    server = create_http_server(workspace, host, port)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread
