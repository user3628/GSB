"""Local HTTP interface using only the Python standard library."""

from __future__ import annotations

import json
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import unquote, urlparse

from .canonical import dump_bytes
from .errors import (
    E_BUSY,
    E_CHART_AXIS,
    E_INVALID_INPUT,
    E_INVALID_REGION,
    E_INVALID_STAGE,
    E_STALE_CHECKPOINT,
    E_TABLE_STRUCTURE,
    E_WORKER,
    PipelineError,
)
from .export import write_exports
from .importer import import_inputs
from .pipeline import Pipeline


STATUS_FOR_ERROR = {
    E_INVALID_INPUT: HTTPStatus.BAD_REQUEST,
    E_INVALID_REGION: HTTPStatus.UNPROCESSABLE_ENTITY,
    E_TABLE_STRUCTURE: HTTPStatus.UNPROCESSABLE_ENTITY,
    E_CHART_AXIS: HTTPStatus.UNPROCESSABLE_ENTITY,
    E_STALE_CHECKPOINT: HTTPStatus.CONFLICT,
    E_INVALID_STAGE: HTTPStatus.CONFLICT,
    E_BUSY: HTTPStatus.CONFLICT,
    E_WORKER: HTTPStatus.INTERNAL_SERVER_ERROR,
}


class ApiState:
    def __init__(
        self,
        root: str | Path,
        manifest: str | Path,
        work_dir: str | Path,
        output_dir: str | Path,
        config_overrides: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.lock = threading.RLock()
        self.root = Path(root).resolve()
        self.manifest = Path(manifest).resolve()
        self.work_dir = Path(work_dir).resolve()
        self.output_dir = Path(output_dir).resolve()
        self.config_overrides = config_overrides
        self.pipeline = Pipeline(
            self.root,
            self.manifest,
            work_dir=self.work_dir,
            output_dir=self.output_dir,
            config_overrides=self.config_overrides,
        )
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.last_result: Optional[Dict[str, Any]] = None
        self.last_error: Optional[Dict[str, Any]] = None

    def fresh_pipeline(self) -> Pipeline:
        return Pipeline(
            self.root,
            self.manifest,
            work_dir=self.work_dir,
            output_dir=self.output_dir,
            config_overrides=self.config_overrides,
        )

    def start_run(
        self,
        *,
        stage: str = "all",
        resume: bool = False,
        fresh: bool = False,
        stop_after: Optional[int] = None,
        workers: Optional[int] = None,
    ) -> Dict[str, Any]:
        with self.lock:
            if self.running:
                raise PipelineError(
                    E_BUSY,
                    "a pipeline run is already active",
                    diagnostics={"running": True},
                )
            self.running = True
            self.last_error = None
            self.last_result = None

        def target() -> None:
            try:
                pipeline = self.fresh_pipeline()
                result = pipeline.run(
                    stage=stage,
                    resume=resume,
                    fresh=fresh,
                    stop_after=stop_after,
                    execution_workers=workers,
                )
                with self.lock:
                    self.pipeline = pipeline
                    self.last_result = result
            except PipelineError as exc:
                with self.lock:
                    self.last_error = exc.to_dict()
            except Exception as exc:  # pragma: no cover - defensive HTTP boundary
                with self.lock:
                    self.last_error = {
                        "code": E_WORKER,
                        "message": str(exc),
                        "diagnostics": {"exception_type": type(exc).__name__},
                    }
            finally:
                with self.lock:
                    self.running = False

        thread = threading.Thread(target=target, name="gridpipe-http-run", daemon=True)
        self.thread = thread
        thread.start()
        return {
            "started": True,
            "stage": stage,
            "resume": resume,
            "fresh": fresh,
            "workers": workers,
        }

    def status(self) -> Dict[str, Any]:
        with self.lock:
            running = self.running
            last_error = self.last_error
            last_result = self.last_result
        pipeline = self.fresh_pipeline()
        status = pipeline.status()
        status["http_running"] = running
        status["last_error"] = last_error
        status["last_result"] = last_result
        return status


class ApiHandler(BaseHTTPRequestHandler):
    server: "GridPipeHTTPServer"

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def _send_json(self, status: int, value: Dict[str, Any]) -> None:
        payload = dump_bytes(value)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(payload)

    def _read_json(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        if length > 2 * 1024 * 1024:
            raise PipelineError(E_INVALID_INPUT, "request body is too large")
        raw = self.rfile.read(length)
        try:
            value = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise PipelineError(E_INVALID_INPUT, f"invalid JSON body: {exc}") from exc
        if not isinstance(value, dict):
            raise PipelineError(E_INVALID_INPUT, "JSON body must be an object")
        return value

    def _handle_error(self, error: PipelineError) -> None:
        self._send_json(
            int(STATUS_FOR_ERROR.get(error.code, HTTPStatus.BAD_REQUEST)),
            {"ok": False, "error": error.to_dict()},
        )

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        try:
            state = self.server.api_state
            if path == "/health":
                self._send_json(HTTPStatus.OK, {"ok": True, "service": "gridpipe"})
                return
            if path == "/api/status":
                self._send_json(HTTPStatus.OK, {"ok": True, "status": state.status()})
                return
            if path == "/api/fingerprints":
                pipeline = state.fresh_pipeline()
                self._send_json(HTTPStatus.OK, {"ok": True, "fingerprints": pipeline.fingerprints()})
                return
            if path == "/api/budget":
                pipeline = state.fresh_pipeline()
                self._send_json(HTTPStatus.OK, {"ok": True, "budget": pipeline.budget_report()})
                return
            if path == "/api/export":
                pipeline = state.fresh_pipeline()
                self._send_json(HTTPStatus.OK, {"ok": True, "export": pipeline.export_data()})
                return
            if path.startswith("/api/shards/"):
                shard_id = unquote(path[len("/api/shards/") :])
                pipeline = state.fresh_pipeline()
                data = pipeline.export_data()
                for shard in data["shards"]:
                    if shard["shard_id"] == shard_id:
                        self._send_json(HTTPStatus.OK, {"ok": True, "shard": shard})
                        return
                self._send_json(
                    HTTPStatus.NOT_FOUND,
                    {"ok": False, "error": {"code": E_INVALID_INPUT, "message": "unknown shard"}},
                )
                return
            self._send_json(
                HTTPStatus.NOT_FOUND,
                {"ok": False, "error": {"code": E_INVALID_INPUT, "message": "unknown endpoint"}},
            )
        except PipelineError as exc:
            self._handle_error(exc)
        except Exception as exc:  # pragma: no cover - defensive HTTP boundary
            self._handle_error(PipelineError(E_WORKER, str(exc)))

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        try:
            state = self.server.api_state
            body = self._read_json()
            if path == "/api/import":
                with state.lock:
                    if state.running:
                        raise PipelineError(E_BUSY, "cannot import while a run is active")
                    manifest = Path(str(body.get("manifest", state.manifest))).resolve()
                    root = Path(str(body.get("root", state.root))).resolve()
                    work = Path(str(body.get("work_dir", state.work_dir))).resolve()
                    output = Path(str(body.get("output_dir", state.output_dir))).resolve()
                    copy_to = body.get("copy_to")
                    record = import_inputs(
                        root,
                        manifest,
                        work_dir=work,
                        output_dir=output,
                        copy_to=copy_to,
                    )
                    state.root = root
                    state.manifest = Path(record["manifest"])
                    state.work_dir = work
                    state.output_dir = output
                    state.pipeline = Pipeline(root, state.manifest, work_dir=work, output_dir=output)
                self._send_json(HTTPStatus.OK, {"ok": True, "import": record})
                return
            if path == "/api/run":
                result = state.start_run(
                    stage=str(body.get("stage", "all")),
                    resume=bool(body.get("resume", False)),
                    fresh=bool(body.get("fresh", False)),
                    stop_after=body.get("stop_after"),
                    workers=body.get("workers"),
                )
                self._send_json(HTTPStatus.ACCEPTED, {"ok": True, "run": result})
                return
            if path == "/api/resume":
                result = state.start_run(
                    stage=str(body.get("stage", "all")),
                    resume=True,
                    fresh=False,
                    stop_after=body.get("stop_after"),
                    workers=body.get("workers"),
                )
                self._send_json(HTTPStatus.ACCEPTED, {"ok": True, "run": result})
                return
            if path == "/api/stop":
                with state.lock:
                    request = state.pipeline.request_stop()
                self._send_json(HTTPStatus.OK, {"ok": True, "stop": request})
                return
            if path == "/api/export":
                with state.lock:
                    pipeline = state.fresh_pipeline()
                    report = write_exports(pipeline)
                self._send_json(HTTPStatus.OK, {"ok": True, "export": report})
                return
            self._send_json(
                HTTPStatus.NOT_FOUND,
                {"ok": False, "error": {"code": E_INVALID_INPUT, "message": "unknown endpoint"}},
            )
        except PipelineError as exc:
            self._handle_error(exc)
        except Exception as exc:  # pragma: no cover - defensive HTTP boundary
            self._handle_error(PipelineError(E_WORKER, str(exc)))


class GridPipeHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: Tuple[str, int], api_state: ApiState) -> None:
        self.api_state = api_state
        super().__init__(address, ApiHandler)


def create_server(
    root: str | Path,
    manifest: str | Path,
    work_dir: str | Path,
    output_dir: str | Path,
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    config_overrides: Optional[Dict[str, Any]] = None,
) -> GridPipeHTTPServer:
    state = ApiState(root, manifest, work_dir, output_dir, config_overrides=config_overrides)
    return GridPipeHTTPServer((host, int(port)), state)
