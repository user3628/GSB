"""Local HTTP API and interactive dashboard using only the Python standard library."""

from __future__ import annotations

import json
import mimetypes
import threading
import traceback
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from .checkpoints import checkpoint_status
from .errors import AppError, conflict, invalid_config, not_found
from .pipeline import SeparationPipeline
from .workspace import SAFE_SLICE_ID, Workspace


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 47321
MAX_BODY_BYTES = 4 * 1024 * 1024


class AppService:
    def __init__(self, workspace: Workspace):
        self.workspace = workspace
        self.pipeline = SeparationPipeline(workspace)
        self._build_lock = threading.Lock()

    def import_dataset(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        if not self._build_lock.acquire(blocking=False):
            raise conflict("another build or import is already running")
        try:
            payload = payload or {}
            manifest_path = payload.get("manifest_path")
            config_path = payload.get("config_path")
            return self.workspace.import_dataset(manifest_path, config_path)
        finally:
            self._build_lock.release()

    def build(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        if not self._build_lock.acquire(blocking=False):
            raise conflict("another build is already running")
        try:
            slice_id = payload.get("slice_id", "all")
            if slice_id in (None, "", "all"):
                selected = None
            elif isinstance(slice_id, str):
                selected = [slice_id]
            else:
                raise invalid_config("slice_id must be a string", param="slice_id")
            stop_after = payload.get("stop_after_units")
            if stop_after is not None:
                if isinstance(stop_after, bool) or not isinstance(stop_after, int) or stop_after < 0:
                    raise invalid_config("stop_after_units must be a non-negative integer", param="stop_after_units")
            return self.pipeline.build_all(slice_ids=selected, stop_after_units=stop_after)
        finally:
            self._build_lock.release()

    def incremental(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self._build_lock.acquire(blocking=False):
            raise conflict("another build or import is already running")
        try:
            return self._incremental(payload)
        finally:
            self._build_lock.release()

    def _incremental(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise invalid_config("request body must be an object", param="body")
        slice_id = payload.get("slice_id")
        source_id = payload.get("source_id")
        if not isinstance(slice_id, str) or not slice_id:
            raise invalid_config("slice_id is required", param="slice_id")
        if isinstance(source_id, bool) or not isinstance(source_id, int):
            raise invalid_config("source_id must be an integer", param="source_id")
        if "bands" in payload:
            bands = payload["bands"]
            if (
                not isinstance(bands, list)
                or len(bands) != 2
                or not all(isinstance(value, int) and not isinstance(value, bool) for value in bands)
            ):
                raise invalid_config("bands must be [start, end]", param="bands")
            band_start, band_end = bands
        else:
            band_start = payload.get("band_start")
            band_end = payload.get("band_end")
            if not isinstance(band_start, int) or not isinstance(band_end, int):
                raise invalid_config("band_start and band_end are required", param="band_start")
        f0_min = payload.get("f0_min_hz")
        f0_max = payload.get("f0_max_hz")
        for key, value in (("f0_min_hz", f0_min), ("f0_max_hz", f0_max)):
            if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float))):
                raise invalid_config(f"{key} must be numeric", param=key)
        return self.pipeline.incremental_band_update(
            slice_id,
            source_id=source_id,
            band_start=band_start,
            band_end=band_end,
            f0_min_hz=None if f0_min is None else float(f0_min),
            f0_max_hz=None if f0_max is None else float(f0_max),
            verify=True,
        )

    def resume(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        if not self._build_lock.acquire(blocking=False):
            raise conflict("another build is already running")
        try:
            slice_id = payload.get("slice_id", "all")
            selected = None if slice_id in (None, "", "all") else [slice_id]
            return self.pipeline.resume(slice_ids=selected)
        finally:
            self._build_lock.release()

    def stop(self) -> dict[str, Any]:
        self.workspace.request_stop("api_stop_request")
        return {"ok": True, "status": "stop_requested", "stop_requested": True}

    def status(self, slice_id: str | None = None) -> dict[str, Any]:
        return self.pipeline.status(slice_id=slice_id)

    def version(self, slice_id: str | None = None) -> dict[str, Any]:
        return self.pipeline.version(slice_id=slice_id)

    def invalidations(self, slice_id: str | None = None) -> dict[str, Any]:
        return self.pipeline.invalidations(slice_id=slice_id)

    def mask_changes(self, slice_id: str | None = None) -> dict[str, Any]:
        return self.pipeline.mask_changes(slice_id=slice_id)

    def verify(self, slice_id: str) -> dict[str, Any]:
        return self.pipeline.verify_slice(slice_id)

    def result(self, slice_id: str) -> dict[str, Any]:
        path = self.workspace.slice_dir(slice_id) / "summary.json"
        if not path.is_file():
            raise not_found("slice has not completed a build", slice_id=slice_id)
        with open(path, "r", encoding="utf-8") as handle:
            return {"ok": True, "summary": json.load(handle)}

    def checkpoints(self, slice_id: str | None = None) -> dict[str, Any]:
        return {"ok": True, **checkpoint_status(self.pipeline.checkpoints, slice_id=slice_id)}

    def publish_checkpoint(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise invalid_config("checkpoint body must be an object", param="body")
        record = payload.get("record")
        if not isinstance(record, dict):
            record = payload
        return {
            "ok": True,
            "record": self.pipeline.checkpoints.publish(
                record,
                sequence=payload.get("sequence"),
                block_sequence=payload.get("block_sequence"),
            ),
        }


class AppRequestHandler(BaseHTTPRequestHandler):
    service: AppService
    server_version = "sonic-sep/1.0"

    def log_message(self, format: str, *args: Any) -> None:
        # Keep the local demo output readable while preserving errors.
        if args and str(args[0]).startswith("4"):
            super().log_message(format, *args)

    def _send_bytes(self, status: int, body: bytes, content_type: str, *, headers: dict[str, str] | None = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        if headers:
            for key, value in headers.items():
                self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8")
        self._send_bytes(status, body, "application/json; charset=utf-8")

    def _error(self, exc: BaseException) -> None:
        if isinstance(exc, AppError):
            status = {
                "E_NOT_FOUND": 404,
                "E_CONFLICT": 409,
                "E_STALE_CHECKPOINT": 409,
                "E_INCREMENTAL_MISMATCH": 409,
                "E_INVALID_CONFIG": 400,
                "E_NUMERIC": 400,
                "E_MANIFEST": 400,
            }.get(exc.code, 500)
            self._json(status, exc.to_dict())
            return
        traceback.print_exc()
        self._json(500, {"ok": False, "error": {"code": "E_INTERNAL", "message": str(exc)}})

    def _read_json(self) -> dict[str, Any]:
        length_raw = self.headers.get("Content-Length", "0")
        try:
            length = int(length_raw)
        except ValueError as exc:
            raise invalid_config("invalid Content-Length", param="Content-Length") from exc
        if length <= 0:
            return {}
        if length > MAX_BODY_BYTES:
            raise invalid_config("request body is too large", param="Content-Length")
        body = self.rfile.read(length)
        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise invalid_config("request body is not valid JSON", param="body") from exc
        if not isinstance(payload, dict):
            raise invalid_config("request body must be a JSON object", param="body")
        return payload

    def do_GET(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            path = parsed.path
            query = parse_qs(parsed.query)
            slice_id = query.get("slice_id", [None])[0]
            if path in ("/", "/index.html"):
                self._serve_static("index.html")
                return
            if path.startswith("/static/"):
                self._serve_static(path.removeprefix("/static/"))
                return
            if path == "/api/health":
                self._json(200, {"ok": True, "status": "healthy", "service": "sonic-sep", "port": self.server.server_port})
                return
            if path == "/api/status":
                self._json(200, self.service.status(slice_id))
                return
            if path == "/api/version":
                self._json(200, self.service.version(slice_id))
                return
            if path == "/api/invalidations":
                self._json(200, self.service.invalidations(slice_id))
                return
            if path == "/api/mask-changes":
                self._json(200, self.service.mask_changes(slice_id))
                return
            if path == "/api/checkpoints":
                self._json(200, self.service.checkpoints(slice_id))
                return
            if path == "/api/result":
                if not slice_id:
                    raise invalid_config("slice_id query parameter is required", param="slice_id")
                self._json(200, self.service.result(slice_id))
                return
            if path.startswith("/api/audio/"):
                self._serve_audio(path)
                return
            self._json(404, {"ok": False, "error": {"code": "E_NOT_FOUND", "message": "route not found"}})
        except BaseException as exc:  # handler boundary
            self._error(exc)

    def do_POST(self) -> None:  # noqa: N802
        try:
            path = urlparse(self.path).path
            if path == "/api/import":
                self._json(200, self.service.import_dataset(self._read_json()))
                return
            if path == "/api/build":
                self._json(200, self.service.build(self._read_json()))
                return
            if path == "/api/incremental":
                self._json(200, self.service.incremental(self._read_json()))
                return
            if path == "/api/stop":
                self._read_json()
                self._json(200, self.service.stop())
                return
            if path == "/api/resume":
                self._json(200, self.service.resume(self._read_json()))
                return
            if path == "/api/verify":
                payload = self._read_json()
                slice_id = payload.get("slice_id")
                if not isinstance(slice_id, str) or not slice_id:
                    raise invalid_config("slice_id is required", param="slice_id")
                self._json(200, self.service.verify(slice_id))
                return
            if path == "/api/checkpoints/publish":
                self._json(200, self.service.publish_checkpoint(self._read_json()))
                return
            self._json(404, {"ok": False, "error": {"code": "E_NOT_FOUND", "message": "route not found"}})
        except BaseException as exc:  # handler boundary
            self._error(exc)

    def _serve_static(self, relative: str) -> None:
        web_root = (self.service.workspace.project_root / "web").resolve()
        target = (web_root / unquote(relative)).resolve()
        try:
            target.relative_to(web_root)
        except ValueError:
            raise not_found("static file not found")
        if not target.is_file():
            raise not_found("static file not found")
        content_type = mimetypes.guess_type(target.name)[0] or "application/octet-stream"
        self._send_bytes(200, target.read_bytes(), content_type)

    def _serve_audio(self, path: str) -> None:
        parts = [unquote(item) for item in path.split("/") if item]
        # /api/audio/<slice>/<file>
        if len(parts) != 4:
            raise not_found("audio route not found")
        _, _, slice_id, filename = parts
        if not SAFE_SLICE_ID.match(slice_id) or filename not in {
            f"source-{index:03d}.wav" for index in range(16)
        }:
            raise not_found("audio file not found")
        target = self.service.workspace.output_dir(slice_id) / filename
        if not target.is_file():
            raise not_found("audio file has not been generated", slice_id=slice_id)
        self._send_bytes(200, target.read_bytes(), "audio/wav")


def make_server(workspace: Workspace, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> ThreadingHTTPServer:
    service = AppService(workspace)
    handler = type("BoundAppRequestHandler", (AppRequestHandler,), {"service": service})
    server = ThreadingHTTPServer((host, int(port)), handler)
    server.daemon_threads = True
    server.allow_reuse_address = True
    return server


def serve(workspace: Workspace, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> None:
    server = make_server(workspace, host, port)
    print(f"sonic-sep listening on http://{host}:{server.server_port}", flush=True)
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()

