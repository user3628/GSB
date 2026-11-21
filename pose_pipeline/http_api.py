"""Small standard-library HTTP application exposing the local pipeline."""

from __future__ import annotations

import json
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .config import PipelineConfig
from .constants import ERROR_INVALID_PARAM, ERROR_NOT_FOUND, ERROR_POSE_INVALID, ERROR_STALE_CHECKPOINT
from .engine import PipelineEngine
from .errors import PipelineError
from .generator import generate_dataset
from .util import pretty_json_bytes


_JOBS: dict[int, dict[str, Any]] = {}
_JOBS_LOCK = threading.Lock()
_JOB_COUNTER = 0


class PoseRequestHandler(BaseHTTPRequestHandler):
    server_version = "DeterministicPosePipeline/1.0"

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        route = parsed.path
        query = parse_qs(parsed.query)
        try:
            if route == "/":
                self._send_html(_INDEX_HTML)
                return
            if route == "/health":
                self._send_json({"ok": True, "service": "pose-pipeline"})
                return
            engine = self._engine()
            if route == "/api/status":
                self._send_json(engine.status())
            elif route == "/api/fingerprints":
                self._send_json(engine.fingerprints())
            elif route == "/api/invalidations":
                refresh = query.get("refresh", ["0"])[0] not in {"0", "false", "no"}
                self._send_json(engine.invalidations(refresh=refresh))
            elif route == "/api/errors":
                self._send_json(engine.errors())
            elif route == "/api/keypoint-errors":
                self._send_json(engine.keypoint_errors())
            elif route in {"/api/poses", "/api/export/poses"}:
                self._send_json(engine.poses())
            elif route in {"/api/report", "/api/export/report"}:
                self._send_json(engine.report())
            elif route == "/api/uncommitted":
                self._send_json(engine.uncommitted_artifacts())
            elif route == "/api/checkpoints":
                scan = engine.store.scan()
                self._send_json(
                    {
                        "ok": True,
                        "records": list(scan.records),
                        "head": scan.head,
                        "diagnostics": scan.diagnostics,
                    }
                )
            elif route == "/api/jobs":
                with _JOBS_LOCK:
                    self._send_json({"ok": True, "jobs": dict(_JOBS)})
            else:
                self._not_found(route)
        except PipelineError as exc:
            self._send_pipeline_error(exc)
        except (ValueError, OSError) as exc:
            self._send_json(
                {
                    "ok": False,
                    "code": "E_OPERATION_FAILED",
                    "message": str(exc),
                    "details": {"exception": type(exc).__name__},
                },
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        route = parsed.path
        try:
            body = self._read_json_body()
            if route == "/api/import":
                manifest_path = body.get("manifest_path") or body.get("manifest")
                if not isinstance(manifest_path, str):
                    raise PipelineError(
                        ERROR_INVALID_PARAM,
                        "manifest_path is required",
                        stage="import",
                    )
                self._send_json(self._engine(body.get("config")).import_samples(manifest_path))
                return
            if route == "/api/generate":
                out_dir = body.get("out_dir") or body.get("out")
                if not isinstance(out_dir, str):
                    raise PipelineError(
                        ERROR_INVALID_PARAM, "out_dir is required", stage="generate"
                    )
                self._send_json(
                    generate_dataset(
                        out_dir,
                        seed=int(body.get("seed", 20261122)),
                        width=int(body.get("width", 128)),
                        height=int(body.get("height", 128)),
                    )
                )
                return
            if route == "/api/build":
                if body.get("background", False):
                    job = self._start_build_job(body)
                    self._send_json({"ok": True, "job": job}, status=HTTPStatus.ACCEPTED)
                else:
                    engine = self._engine(body.get("config"))
                    if body.get("manifest_path"):
                        engine.import_samples(body["manifest_path"])
                    self._send_json(
                        engine.build(
                            to_stage=body.get("to_stage"),
                            stop_after_units=body.get("stop_after_units"),
                            clear_stop=bool(body.get("clear_stop", False)),
                            strict_shard_id=body.get("strict_shard_id"),
                            strict_stage=body.get("strict_stage"),
                        )
                    )
                return
            if route == "/api/stop":
                self._send_json(self._engine(body.get("config")).request_stop())
                return
            if route == "/api/resume":
                engine = self._engine(body.get("config"))
                self._send_json(
                    engine.resume(
                        to_stage=body.get("to_stage"),
                        stop_after_units=body.get("stop_after_units"),
                    )
                )
                return
            if route == "/api/export":
                out_dir = body.get("out_dir") or body.get("out")
                self._send_json(self._engine(body.get("config")).export(out_dir))
                return
            self._not_found(route)
        except PipelineError as exc:
            self._send_pipeline_error(exc)
        except (ValueError, OSError) as exc:
            self._send_json(
                {
                    "ok": False,
                    "code": "E_OPERATION_FAILED",
                    "message": str(exc),
                    "details": {"exception": type(exc).__name__},
                },
                status=HTTPStatus.BAD_REQUEST,
            )

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        if not getattr(self.server, "quiet", False):
            super().log_message(format, *args)

    def _engine(self, config_spec: Any = None) -> PipelineEngine:
        server_config = getattr(self.server, "pipeline_config", None)
        if config_spec is None:
            config = server_config
        elif isinstance(config_spec, dict):
            config = PipelineConfig.from_mapping(config_spec)
        elif isinstance(config_spec, str):
            config = PipelineConfig.from_file(config_spec)
        else:
            raise PipelineError(ERROR_INVALID_PARAM, "config must be an object or path")
        return PipelineEngine(self.server.workspace, config=config)

    def _start_build_job(self, body: dict[str, Any]) -> dict[str, Any]:
        global _JOB_COUNTER
        with _JOBS_LOCK:
            _JOB_COUNTER += 1
            job_id = _JOB_COUNTER
            job: dict[str, Any] = {
                "job_id": job_id,
                "status": "running",
                "result": None,
                "error": None,
            }
            _JOBS[job_id] = job

        def target() -> None:
            try:
                engine = self._engine(body.get("config"))
                if body.get("manifest_path"):
                    engine.import_samples(body["manifest_path"])
                result = engine.build(
                    to_stage=body.get("to_stage"),
                    stop_after_units=body.get("stop_after_units"),
                    clear_stop=bool(body.get("clear_stop", False)),
                )
                with _JOBS_LOCK:
                    _JOBS[job_id]["status"] = "completed"
                    _JOBS[job_id]["result"] = result
            except PipelineError as exc:
                with _JOBS_LOCK:
                    _JOBS[job_id]["status"] = "failed"
                    _JOBS[job_id]["error"] = exc.to_dict()
            except BaseException as exc:  # pragma: no cover - defensive thread boundary
                with _JOBS_LOCK:
                    _JOBS[job_id]["status"] = "failed"
                    _JOBS[job_id]["error"] = {
                        "code": "E_OPERATION_FAILED",
                        "message": str(exc),
                        "details": {"exception": type(exc).__name__},
                    }

        thread = threading.Thread(target=target, name=f"pose-build-{job_id}", daemon=True)
        thread.start()
        return job

    def _read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length > 10 * 1024 * 1024:
            raise PipelineError(ERROR_INVALID_PARAM, "request body is too large")
        raw = self.rfile.read(length) if length else b"{}"
        try:
            value = json.loads(raw.decode("utf-8")) if raw else {}
        except (ValueError, UnicodeDecodeError) as exc:
            raise PipelineError(ERROR_INVALID_PARAM, f"invalid JSON body: {exc}") from exc
        if not isinstance(value, dict):
            raise PipelineError(ERROR_INVALID_PARAM, "JSON body must be an object")
        return value

    def _send_pipeline_error(self, error: PipelineError) -> None:
        status = HTTPStatus.BAD_REQUEST
        if error.code in {ERROR_STALE_CHECKPOINT}:
            status = HTTPStatus.CONFLICT
        elif error.code in {ERROR_POSE_INVALID, "E_INVALID_STAGE"}:
            status = HTTPStatus.UNPROCESSABLE_ENTITY
        elif error.code == ERROR_NOT_FOUND:
            status = HTTPStatus.NOT_FOUND
        self._send_json(error.to_dict(), status=status)

    def _not_found(self, route: str) -> None:
        self._send_json(
            {"ok": False, "code": ERROR_NOT_FOUND, "message": f"route not found: {route}"},
            status=HTTPStatus.NOT_FOUND,
        )

    def _send_json(self, value: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
        data = pretty_json_bytes(value)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _send_html(self, value: str) -> None:
        data = value.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def create_server(
    workspace: str | Path,
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
    config: PipelineConfig | None = None,
    quiet: bool = False,
) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), PoseRequestHandler)
    server.workspace = Path(workspace).resolve()  # type: ignore[attr-defined]
    server.pipeline_config = config or PipelineConfig()  # type: ignore[attr-defined]
    server.quiet = quiet  # type: ignore[attr-defined]
    return server


def serve(
    workspace: str | Path,
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
    config_path: str | None = None,
    quiet: bool = False,
) -> None:
    config = PipelineConfig.from_file(config_path) if config_path else PipelineConfig()
    server = create_server(workspace, host=host, port=port, config=config, quiet=quiet)
    try:
        server.serve_forever()
    finally:
        server.server_close()


_INDEX_HTML = """<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><title>Deterministic Pose Pipeline</title>
<style>body{font-family:system-ui,sans-serif;max-width:900px;margin:3rem auto;padding:0 1rem;line-height:1.55}code{background:#eee;padding:.15rem .3rem;border-radius:3px}li{margin:.35rem 0}</style>
</head><body>
<h1>Deterministic Pose Pipeline</h1>
<p>Local CPU-only keypoint and pose pipeline. JSON endpoints:</p>
<ul>
<li><code>POST /api/import</code> {manifest_path}</li>
<li><code>POST /api/build</code> {to_stage?, stop_after_units?, background?}</li>
<li><code>GET /api/status</code>, <code>GET /api/fingerprints</code>, <code>GET /api/invalidations</code></li>
<li><code>POST /api/stop</code>, <code>POST /api/resume</code></li>
<li><code>GET /api/poses</code>, <code>GET /api/report</code>, <code>POST /api/export</code></li>
</ul>
</body></html>"""
