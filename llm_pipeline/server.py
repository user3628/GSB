"""Small dependency-free local HTTP API."""

from __future__ import annotations

import json
import sys
import traceback
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import unquote, urlparse

from .config import STAGES, canonical_stage
from .engine import PipelineEngine
from .errors import E_INVALID_INPUT, PipelineError
from .util import canonical_json_line
from .workspace import Workspace

MAX_BODY_BYTES = 50 * 1024 * 1024


def make_handler(workspace: Workspace) -> type[BaseHTTPRequestHandler]:
    engine = PipelineEngine(workspace)

    class Handler(BaseHTTPRequestHandler):
        server_version = "LLMPipelineHTTP/1.0"

        def log_message(self, fmt: str, *args: Any) -> None:
            sys.stderr.write("http: " + (fmt % args) + "\n")

        def _send_json(self, status: int, payload: Any) -> None:
            body = json.dumps(
                payload, ensure_ascii=False, indent=2, sort_keys=True
            ).encode("utf-8") + b"\n"
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _send_bytes(self, status: int, body: bytes, content_type: str, filename: str | None = None) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            if filename:
                self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
            self.end_headers()
            self.wfile.write(body)

        def _read_body(self) -> dict[str, Any]:
            length_header = self.headers.get("Content-Length")
            if length_header is None:
                return {}
            try:
                length = int(length_header)
            except ValueError as exc:
                raise PipelineError(E_INVALID_INPUT, "invalid Content-Length") from exc
            if length < 0 or length > MAX_BODY_BYTES:
                raise PipelineError(
                    E_INVALID_INPUT,
                    "request body is too large",
                    {"max_bytes": MAX_BODY_BYTES, "actual": length},
                    http_status=413,
                )
            raw = self.rfile.read(length)
            if not raw:
                return {}
            try:
                payload = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise PipelineError(
                    E_INVALID_INPUT,
                    "request body must be a JSON object",
                    {"error": str(exc)},
                ) from exc
            if not isinstance(payload, dict):
                raise PipelineError(E_INVALID_INPUT, "request body must be a JSON object")
            return payload

        def _dispatch(self, method: str) -> None:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            try:
                if method == "GET":
                    self._handle_get(path)
                elif method == "POST":
                    self._handle_post(path)
                else:
                    self._send_json(
                        HTTPStatus.METHOD_NOT_ALLOWED,
                        {"ok": False, "code": "E_METHOD_NOT_ALLOWED", "message": method},
                    )
            except PipelineError as exc:
                self._send_json(exc.http_status, exc.to_dict())
            except Exception as exc:  # pragma: no cover - defensive server boundary
                traceback.print_exc(file=sys.stderr)
                self._send_json(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {
                        "ok": False,
                        "code": "E_INTERNAL",
                        "message": str(exc),
                    },
                )

        def do_GET(self) -> None:  # noqa: N802
            self._dispatch("GET")

        def do_POST(self) -> None:  # noqa: N802
            self._dispatch("POST")

        def _handle_get(self, path: str) -> None:
            if path in {"/", "/health", "/api/health"}:
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "service": "llm-pretraining-pipeline",
                        "workspace": str(workspace.root),
                        "initialized": workspace.initialized(),
                        "stages": list(STAGES),
                    },
                )
                return
            if path in {"/api/status", "/api/stages"}:
                self._send_json(HTTPStatus.OK, engine.status())
                return
            if path == "/api/fingerprints":
                self._send_json(HTTPStatus.OK, engine.fingerprints())
                return
            if path == "/api/checkpoints":
                status = engine.status()
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "checkpoint_sequence": status["checkpoint_sequence"],
                        "stages": {
                            stage: {
                                "seq": details["seq"],
                                "stage_seq": details["stage_seq"],
                                "artifact_digest": details["artifact_digest"],
                            }
                            for stage, details in status["stages"].items()
                        },
                    },
                )
                return
            if path.startswith("/api/stages/"):
                stage = canonical_stage(unquote(path.split("/api/stages/", 1)[1]))
                status = engine.status()
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "stage": stage,
                        "details": status["stages"][stage],
                        "invalidation": status["invalidation"],
                    },
                )
                return
            if path == "/api/report":
                self._send_json(HTTPStatus.OK, engine.report())
                return
            if path == "/api/export/report":
                body = canonical_json_line(engine.report())
                self._send_bytes(
                    HTTPStatus.OK,
                    body,
                    "application/json; charset=utf-8",
                    "validation-report.json",
                )
                return
            if path in {"/api/export/mixed", "/api/export/mixed.jsonl"}:
                body = engine.mixed_bytes()
                self._send_bytes(
                    HTTPStatus.OK,
                    body,
                    "application/x-ndjson; charset=utf-8",
                    "mixed.jsonl",
                )
                return
            raise PipelineError(
                "E_NOT_FOUND", "unknown route", {"path": path}, http_status=404
            )

        def _handle_post(self, path: str) -> None:
            body = self._read_body()
            if path == "/api/run":
                self._send_json(
                    HTTPStatus.OK,
                    engine.run(
                        force=bool(body.get("force", False)),
                        stop_after_samples=body.get("stop_after_samples"),
                        delay_ms=int(body.get("delay_ms", 0)),
                    ),
                )
                return
            if path.startswith("/api/stages/") and path.endswith("/run"):
                stage = canonical_stage(
                    unquote(path[len("/api/stages/") : -len("/run")])
                )
                self._send_json(
                    HTTPStatus.OK,
                    engine.run_stage(
                        stage,
                        stop_after_samples=body.get("stop_after_samples"),
                        delay_ms=int(body.get("delay_ms", 0)),
                    ),
                )
                return
            if path == "/api/shards/import":
                result = {
                    "ok": True,
                    "import": self._shard_operation(body, replace=False),
                }
                if body.get("run", False):
                    result["run"] = engine.run()
                self._send_json(HTTPStatus.OK, result)
                return
            if path == "/api/shards/replace":
                result = {
                    "ok": True,
                    "replace": self._shard_operation(body, replace=True),
                }
                if body.get("run", True):
                    result["run"] = engine.run()
                self._send_json(HTTPStatus.OK, result)
                return
            if path == "/api/config/change":
                patch = body.get("patch", {})
                set_values = body.get("set", {})
                if patch is None:
                    patch = {}
                if set_values is None:
                    set_values = {}
                if not isinstance(patch, dict) or not isinstance(set_values, dict):
                    raise PipelineError(
                        E_INVALID_INPUT, "patch and set must be JSON objects"
                    )
                result = {
                    "ok": True,
                    "change": engine.change_config(patch=patch, set_values=set_values),
                }
                if body.get("run", True):
                    result["run"] = engine.run()
                self._send_json(HTTPStatus.OK, result)
                return
            if path == "/api/stop":
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "stop": workspace.request_stop(reason=str(body.get("reason", "http"))),
                    },
                )
                return
            if path == "/api/resume":
                self._send_json(
                    HTTPStatus.OK,
                    engine.resume(
                        stop_after_samples=body.get("stop_after_samples"),
                        delay_ms=int(body.get("delay_ms", 0)),
                    ),
                )
                return
            if path == "/api/verify-incremental":
                self._send_json(
                    HTTPStatus.OK,
                    engine.verify_incremental(
                        run_if_needed=bool(body.get("run_if_needed", True))
                    ),
                )
                return
            if path == "/api/demo/init":
                manifest = workspace.initialize_demo(
                    body.get("demo_dir", "demo"), force=bool(body.get("force", False))
                )
                result: dict[str, Any] = {"ok": True, "manifest": manifest}
                if body.get("run", True):
                    result["run"] = engine.run(force=True)
                self._send_json(HTTPStatus.OK, result)
                return
            raise PipelineError(
                "E_NOT_FOUND", "unknown route", {"path": path}, http_status=404
            )

        def _shard_operation(self, body: dict[str, Any], *, replace: bool) -> dict[str, Any]:
            name = body.get("name")
            content = body.get("content")
            source_path = body.get("path")
            if not isinstance(name, str) or not name:
                raise PipelineError(E_INVALID_INPUT, "name is required")
            if content is None and source_path is None:
                raise PipelineError(
                    E_INVALID_INPUT, "either path or content must be supplied"
                )
            if source_path is not None:
                if not isinstance(source_path, str):
                    raise PipelineError(E_INVALID_INPUT, "path must be a string")
                return workspace.import_shard_from_path(
                    name, source_path, replace=replace
                )
            if not isinstance(content, str):
                raise PipelineError(E_INVALID_INPUT, "content must be a string")
            return workspace.import_shard(
                name, content.encode("utf-8"), replace=replace
            )

    return Handler


def serve(workspace: Workspace, *, host: str = "127.0.0.1", port: int = 8765) -> None:
    workspace.ensure()
    handler = make_handler(workspace)
    server = ThreadingHTTPServer((host, port), handler)
    sys.stdout.write(
        json.dumps(
            {
                "ok": True,
                "listen": f"http://{host}:{port}",
                "workspace": str(workspace.root),
            },
            ensure_ascii=False,
        )
        + "\n"
    )
    sys.stdout.flush()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
