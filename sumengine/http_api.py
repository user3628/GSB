"""Local HTTP experiment API built on the standard-library HTTP server."""

from __future__ import annotations

import json
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .config import normalize_config, parse_yaml
from .engine import SummaryEngine
from .errors import EngineError
from .utils import canonical_json


STATUS_FOR_ERROR = {
    "E_INVALID_INPUT": HTTPStatus.BAD_REQUEST,
    "E_CONFIG": HTTPStatus.BAD_REQUEST,
    "E_INPUT_NOT_FOUND": HTTPStatus.NOT_FOUND,
    "E_INPUT_TOO_SHORT": HTTPStatus.UNPROCESSABLE_ENTITY,
    "E_NOT_READY": HTTPStatus.CONFLICT,
    "E_STALE_CHECKPOINT": HTTPStatus.CONFLICT,
    "E_LATENCY_BUDGET": HTTPStatus.REQUEST_TIMEOUT,
    "E_GLOBAL_BUDGET": HTTPStatus.CONFLICT,
    "E_WORKER_QUOTA": HTTPStatus.CONFLICT,
    "E_BUSY": HTTPStatus.CONFLICT,
    "E_WORKSPACE_NOT_WRITABLE": HTTPStatus.INTERNAL_SERVER_ERROR,
}


class ApiState:
    def __init__(self, engine: SummaryEngine) -> None:
        self.engine = engine
        self.lock = threading.Lock()
        self.jobs: dict[str, dict[str, Any]] = {}
        self.job_counter = 0
        self.running_job: str | None = None

    def start_run(self, *, resume: bool, mode: str, stop_after_docs: int | None) -> dict[str, Any]:
        if mode not in {"serial", "parallel"}:
            raise EngineError("E_INVALID_INPUT", "mode must be 'serial' or 'parallel'")
        if stop_after_docs is not None and (
            isinstance(stop_after_docs, bool)
            or not isinstance(stop_after_docs, int)
            or stop_after_docs < 1
        ):
            raise EngineError("E_INVALID_INPUT", "stop_after_docs must be a positive integer")
        with self.lock:
            if self.running_job is not None:
                raise EngineError(
                    "E_BUSY",
                    "A run is already active in this workspace",
                    details={"run_id": self.running_job},
                )
            self.job_counter += 1
            run_id = f"run-{self.job_counter}"
            job = {
                "run_id": run_id,
                "status": "running",
                "mode": mode,
                "resume": resume,
                "result": None,
                "error": None,
            }
            self.jobs[run_id] = job
            self.running_job = run_id

        def target() -> None:
            try:
                result = (
                    self.engine.resume(mode=mode, stop_after_docs=stop_after_docs)
                    if resume
                    else self.engine.run(mode=mode, stop_after_docs=stop_after_docs)
                )
                with self.lock:
                    job["result"] = result
                    job["status"] = result.get("status", "complete")
            except EngineError as exc:
                with self.lock:
                    job["error"] = exc.to_dict()["error"]
                    job["status"] = "error"
            except BaseException as exc:  # pragma: no cover - defensive HTTP boundary
                with self.lock:
                    job["error"] = {
                        "code": "E_WORKER_FAILURE",
                        "message": f"{type(exc).__name__}: {exc}",
                    }
                    job["status"] = "error"
            finally:
                with self.lock:
                    if self.running_job == run_id:
                        self.running_job = None

        thread = threading.Thread(target=target, name=f"sumengine-{run_id}", daemon=True)
        thread.start()
        return {"ok": True, "run_id": run_id, "status": "running"}


def _json_response(handler: BaseHTTPRequestHandler, status: int, value: Any) -> None:
    payload = canonical_json(value, indent=None).encode("utf-8") + b"\n"
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(payload)))
    handler.send_header("Cache-Control", "no-store")
    handler.end_headers()
    handler.wfile.write(payload)


def make_handler(state: ApiState) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "SumEngineHTTP/1.0"

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            return

        def _body(self) -> Any:
            length = int(self.headers.get("Content-Length", "0"))
            if length > 16 * 1024 * 1024:
                raise EngineError("E_INVALID_INPUT", "Request body is too large")
            if length == 0:
                return {}
            raw = self.rfile.read(length)
            try:
                return json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise EngineError("E_INVALID_INPUT", f"Invalid JSON request body: {exc}") from exc

        def _dispatch(self, method: str) -> None:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            query = parse_qs(parsed.query)
            try:
                if method == "GET" and path == "/health":
                    _json_response(
                        self,
                        HTTPStatus.OK,
                        {"ok": True, "service": "sumengine", "workspace": str(state.engine.workspace)},
                    )
                    return
                if method == "GET" and path == "/status":
                    _json_response(self, HTTPStatus.OK, state.engine.status())
                    return
                if method == "GET" and path == "/budget":
                    _json_response(self, HTTPStatus.OK, state.engine.budget_status())
                    return
                if method == "GET" and path == "/candidates":
                    doc_id = query.get("doc_id", [None])[0]
                    if not doc_id:
                        raise EngineError("E_INVALID_INPUT", "doc_id query parameter is required")
                    _json_response(self, HTTPStatus.OK, state.engine.candidates(doc_id))
                    return
                if method == "GET" and path == "/export":
                    _json_response(self, HTTPStatus.OK, state.engine.export_set())
                    return
                if method == "GET" and path.startswith("/runs/"):
                    run_id = path.split("/runs/", 1)[1]
                    with state.lock:
                        job = state.jobs.get(run_id)
                        value = dict(job) if job else None
                    if value is None:
                        raise EngineError("E_INPUT_NOT_FOUND", f"Unknown run_id: {run_id}")
                    _json_response(self, HTTPStatus.OK, {"ok": True, **value})
                    return
                if method == "POST" and path == "/run":
                    body = self._body()
                    _json_response(
                        self,
                        HTTPStatus.ACCEPTED,
                        state.start_run(
                            resume=False,
                            mode=str(body.get("mode", "parallel")),
                            stop_after_docs=body.get("stop_after_docs"),
                        ),
                    )
                    return
                if method == "POST" and path == "/resume":
                    body = self._body()
                    _json_response(
                        self,
                        HTTPStatus.ACCEPTED,
                        state.start_run(
                            resume=True,
                            mode=str(body.get("mode", "parallel")),
                            stop_after_docs=body.get("stop_after_docs"),
                        ),
                    )
                    return
                if method == "POST" and path == "/stop":
                    body = self._body()
                    _json_response(
                        self,
                        HTTPStatus.ACCEPTED,
                        state.engine.request_stop(str(body.get("reason", "http"))),
                    )
                    return
                if method == "POST" and path == "/edit":
                    body = self._body()
                    _json_response(
                        self,
                        HTTPStatus.OK,
                        state.engine.edit_summary(str(body.get("doc_id", "")), str(body.get("text", ""))),
                    )
                    return
                if method == "POST" and path == "/export":
                    _json_response(self, HTTPStatus.OK, state.engine.export())
                    return
                if method == "POST" and path == "/import":
                    body = self._body()
                    if "documents" in body:
                        documents = body["documents"]
                    elif "path" in body:
                        from .samples import load_documents

                        documents = load_documents(str(body["path"]))
                    else:
                        raise EngineError("E_INVALID_INPUT", "Provide documents or path")
                    _json_response(self, HTTPStatus.OK, {"ok": True, **state.engine.import_documents(documents)})
                    return
                if method == "PUT" and path == "/config":
                    body = self._body()
                    if isinstance(body, dict) and "yaml" in body:
                        raw = parse_yaml(str(body["yaml"]), "<http-yaml>")
                    else:
                        raw = body
                    _json_response(self, HTTPStatus.OK, {"ok": True, "config": state.engine.save_config(raw)})
                    return
                if method == "GET" and path == "/config":
                    _json_response(self, HTTPStatus.OK, {"ok": True, "config": state.engine.get_config()})
                    return
                raise EngineError("E_INPUT_NOT_FOUND", f"No route for {method} {path}")
            except EngineError as exc:
                _json_response(self, int(STATUS_FOR_ERROR.get(exc.code, HTTPStatus.BAD_REQUEST)), exc.to_dict())
            except BaseException as exc:  # pragma: no cover - defensive HTTP boundary
                _json_response(
                    self,
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {
                        "ok": False,
                        "error": {"code": "E_WORKER_FAILURE", "message": f"{type(exc).__name__}: {exc}"},
                    },
                )

        def do_GET(self) -> None:  # noqa: N802
            self._dispatch("GET")

        def do_POST(self) -> None:  # noqa: N802
            self._dispatch("POST")

        def do_PUT(self) -> None:  # noqa: N802
            self._dispatch("PUT")

    return Handler


def create_server(engine: SummaryEngine, *, host: str = "127.0.0.1", port: int = 8765) -> ThreadingHTTPServer:
    state = ApiState(engine)
    server = ThreadingHTTPServer((host, port), make_handler(state))
    server.daemon_threads = True
    setattr(server, "api_state", state)
    return server


def run_http_server(engine: SummaryEngine, *, host: str = "127.0.0.1", port: int = 8765) -> None:
    server = create_server(engine, host=host, port=port)
    print(f"sumengine HTTP listening on http://{host}:{server.server_address[1]}")
    try:
        server.serve_forever()
    finally:
        server.server_close()
