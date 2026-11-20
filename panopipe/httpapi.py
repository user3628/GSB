"""Small standard-library HTTP API bound to a local loopback address."""

from __future__ import annotations

import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import parse_qs, urlparse

from .errors import E_INVALID_CONFIG, E_NOT_FOUND, PipelineError
from .manifest import ParseOptions
from .pipeline import Pipeline
from .workspace import Workspace


LOOPBACK_HOSTS = {"127.0.0.1", "localhost"}
MAX_BODY_BYTES = 2 * 1024 * 1024


class PanopipeHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], workspace: Workspace):
        self.workspace = workspace
        super().__init__(address, APIHandler)


class APIHandler(BaseHTTPRequestHandler):
    server: PanopipeHTTPServer

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    @property
    def workspace(self) -> Workspace:
        return self.server.workspace

    def _send_json(self, value: Any, status: int = 200) -> None:
        payload = (json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            self.wfile.write(payload)
        except BrokenPipeError:
            pass

    def _send_bytes(self, payload: bytes, content_type: str, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            self.wfile.write(payload)
        except BrokenPipeError:
            pass

    def _read_json(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise PipelineError(E_INVALID_CONFIG, "Invalid Content-Length") from exc
        if length > MAX_BODY_BYTES:
            raise PipelineError(E_INVALID_CONFIG, "Request body is too large", {"limit": MAX_BODY_BYTES})
        raw = self.rfile.read(length) if length else b"{}"
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PipelineError(E_INVALID_CONFIG, "Request body must be valid JSON") from exc
        if not isinstance(value, dict):
            raise PipelineError(E_INVALID_CONFIG, "Request body must be a JSON object")
        return value

    def _handle_error(self, exc: PipelineError) -> None:
        self._send_json(exc.to_dict(), status=exc.http_status)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        try:
            if parsed.path in {"/", "/api"}:
                self._send_json(
                    {
                        "ok": True,
                        "service": "panopipe",
                        "endpoints": [
                            "GET /api/health",
                            "GET /api/status",
                            "GET /api/fingerprints",
                            "GET /api/checkpoints",
                            "GET /api/export?shard_id=...&format=png|rle|coco_json",
                            "POST /api/import",
                            "POST /api/build",
                            "POST /api/build/stage",
                            "POST /api/validate",
                            "POST /api/stop",
                            "POST /api/resume",
                        ],
                    }
                )
                return
            if parsed.path == "/api/health":
                self._send_json(
                    {
                        "ok": True,
                        "service": "panopipe",
                        "workspace": str(self.workspace.root),
                        "workspace_imported": self.workspace.manifest_path.is_file(),
                    }
                )
                return
            query = parse_qs(parsed.query)
            if parsed.path == "/api/status":
                shards = query.get("shard_id") or None
                self._send_json(Pipeline(self.workspace).status(shard_ids=shards))
                return
            if parsed.path == "/api/fingerprints":
                self._send_json(Pipeline(self.workspace).fingerprints())
                return
            if parsed.path == "/api/checkpoints":
                records = self.workspace.checkpoints.list_records()
                try:
                    limit = int(query.get("limit", [str(len(records))])[0])
                except ValueError as exc:
                    raise PipelineError(E_INVALID_CONFIG, "Checkpoint limit must be an integer") from exc
                selected = records[-limit:] if limit > 0 else []
                self._send_json(
                    {
                        "ok": True,
                        "last_sequence": records[-1]["sequence"] if records else 0,
                        "records": selected,
                    }
                )
                return
            if parsed.path == "/api/export":
                shard_id = query.get("shard_id", [""])[0]
                format_name = query.get("format", [""])[0]
                if not shard_id or not format_name:
                    raise PipelineError(
                        E_INVALID_CONFIG,
                        "Export requires shard_id and format query parameters",
                    )
                payload, content_type = Pipeline(self.workspace).export(shard_id, format_name)
                self._send_bytes(payload, content_type)
                return
            raise PipelineError(E_NOT_FOUND, "HTTP endpoint not found", {"path": parsed.path})
        except PipelineError as exc:
            self._handle_error(exc)
        except Exception as exc:
            self._handle_error(PipelineError("E_INTERNAL", "Unexpected server error", {"reason": str(exc)}))

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        try:
            body = self._read_json()
            if parsed.path == "/api/import":
                manifest_path = body.get("manifest_path")
                if not isinstance(manifest_path, str) or not manifest_path:
                    raise PipelineError(E_INVALID_CONFIG, "manifest_path is required")
                options_raw = body.get("parse_options", {})
                if not isinstance(options_raw, Mapping):
                    raise PipelineError(E_INVALID_CONFIG, "parse_options must be an object")
                options = ParseOptions(
                    default_instance_start_id=int(options_raw.get("default_instance_start_id", 100)),
                    min_instance_area=int(options_raw.get("min_instance_area", 4)),
                    confidence_threshold=float(options_raw.get("confidence_threshold", 0.5)),
                    mask_format=str(options_raw.get("mask_format", "png_palette")),
                    v1_drop_alpha=bool(options_raw.get("v1_drop_alpha", True)),
                )
                workspace_config = body.get("workspace_config")
                if workspace_config is not None and not isinstance(workspace_config, Mapping):
                    raise PipelineError(E_INVALID_CONFIG, "workspace_config must be an object")
                manifest = self.workspace.import_manifest(
                    Path(manifest_path),
                    parse_options=options,
                    config=workspace_config,
                )
                self._send_json(
                    {
                        "ok": True,
                        "project": manifest.project,
                        "source_schema_version": manifest.schema_version,
                        "shards": [shard.shard_id for shard in manifest.shards],
                    }
                )
                return
            if parsed.path == "/api/build":
                shard_ids = body.get("shard_ids")
                stages = body.get("stages")
                if shard_ids is not None and not isinstance(shard_ids, list):
                    raise PipelineError(E_INVALID_CONFIG, "shard_ids must be a list")
                if stages is not None and not isinstance(stages, list):
                    raise PipelineError(E_INVALID_CONFIG, "stages must be a list")
                stop_after_shards = body.get("stop_after_shards")
                if stop_after_shards is not None:
                    try:
                        stop_after_shards = int(stop_after_shards)
                    except (TypeError, ValueError) as exc:
                        raise PipelineError(E_INVALID_CONFIG, "stop_after_shards must be an integer") from exc
                result = Pipeline(self.workspace).build(
                    shard_ids=shard_ids,
                    stages=stages,
                    resume=bool(body.get("resume", False)),
                    stop_after_shards=stop_after_shards,
                )
                self._send_json(result)
                return
            if parsed.path == "/api/build/stage":
                shard_id = body.get("shard_id")
                stage = body.get("stage")
                if not isinstance(shard_id, str) or not isinstance(stage, str):
                    raise PipelineError(E_INVALID_CONFIG, "shard_id and stage are required")
                result = Pipeline(self.workspace).build(shard_ids=[shard_id], stages=[stage])
                self._send_json(result)
                return
            if parsed.path in {"/api/validate", "/api/roundtrip"}:
                shard_ids = body.get("shard_ids")
                if shard_ids is not None and not isinstance(shard_ids, list):
                    raise PipelineError(E_INVALID_CONFIG, "shard_ids must be a list")
                result = Pipeline(self.workspace).validate(shard_ids=shard_ids)
                self._send_json(result)
                return
            if parsed.path == "/api/stop":
                self.workspace.request_stop(reason=str(body.get("reason", "http")))
                self._send_json({"ok": True, "stop_requested": True})
                return
            if parsed.path == "/api/resume":
                self.workspace.clear_stop()
                if body.get("build"):
                    shard_ids = body.get("shard_ids")
                    stop_after_shards = body.get("stop_after_shards")
                    if stop_after_shards is not None:
                        try:
                            stop_after_shards = int(stop_after_shards)
                        except (TypeError, ValueError) as exc:
                            raise PipelineError(E_INVALID_CONFIG, "stop_after_shards must be an integer") from exc
                    result = Pipeline(self.workspace).build(
                        shard_ids=shard_ids,
                        resume=True,
                        stop_after_shards=stop_after_shards,
                    )
                    self._send_json(result)
                else:
                    self._send_json({"ok": True, "stop_requested": False})
                return
            raise PipelineError(E_NOT_FOUND, "HTTP endpoint not found", {"path": parsed.path})
        except PipelineError as exc:
            self._handle_error(exc)
        except Exception as exc:
            self._handle_error(PipelineError("E_INTERNAL", "Unexpected server error", {"reason": str(exc)}))


def create_server(workspace: Workspace, host: str = "127.0.0.1", port: int = 8765) -> PanopipeHTTPServer:
    if host not in LOOPBACK_HOSTS:
        raise PipelineError(
            E_INVALID_CONFIG,
            "HTTP server may bind only to a local loopback address",
            {"host": host, "allowed": sorted(LOOPBACK_HOSTS)},
        )
    return PanopipeHTTPServer((host, int(port)), workspace)


def run_server(*, workspace: Workspace, host: str = "127.0.0.1", port: int = 8765) -> None:
    server = create_server(workspace, host, port)
    actual_host, actual_port = server.server_address[:2]
    print(f"panopipe HTTP listening on http://{actual_host}:{actual_port}", flush=True)
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        server.server_close()
