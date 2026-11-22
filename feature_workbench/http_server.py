from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

from .engine import Workbench
from .errors import WorkbenchError
from .util import to_jsonable


class WorkbenchService:
    def __init__(self, workbench: Workbench) -> None:
        self.workbench = workbench
        self._state_lock = threading.Lock()
        self._running = False
        self._thread: threading.Thread | None = None
        self._last_result: dict[str, Any] | None = None
        self._last_error: dict[str, Any] | None = None

    def start_run(self, payload: dict[str, Any], *, resume: bool = False) -> dict[str, Any]:
        with self._state_lock:
            if self._running:
                raise WorkbenchError(
                    "E_RUN_IN_PROGRESS",
                    "A pipeline run is already active in this HTTP service",
                )
            until_stage = str(payload.get("until_stage", "aggregate"))
            worker_mode = str(payload.get("worker_mode", "process"))
            self.workbench._normalize_stage(until_stage)
            if worker_mode not in {"process", "sequential"}:
                raise WorkbenchError(
                    "E_CONFIG_INVALID",
                    "worker_mode must be process or sequential",
                    {"value": worker_mode},
                )
            stop_after = payload.get("stop_after_shards")
            if stop_after is not None:
                try:
                    stop_after = int(stop_after)
                except (TypeError, ValueError) as exc:
                    raise WorkbenchError(
                        "E_CONFIG_INVALID",
                        "stop_after_shards must be a positive integer",
                        {"value": payload.get("stop_after_shards")},
                    ) from exc
                if stop_after < 1:
                    raise WorkbenchError(
                        "E_CONFIG_INVALID",
                        "stop_after_shards must be a positive integer",
                        {"value": stop_after},
                    )
            self._running = True
            self._last_error = None
            workbench = self.workbench

            def runner() -> None:
                try:
                    result = workbench.run(
                        until_stage=until_stage,
                        worker_mode=worker_mode,
                        stop_after_shards=stop_after,
                        resume=resume,
                    )
                    with self._state_lock:
                        self._last_result = result
                except WorkbenchError as exc:
                    with self._state_lock:
                        self._last_error = exc.to_dict()["error"]
                except BaseException as exc:
                    with self._state_lock:
                        self._last_error = {
                            "code": "E_INTERNAL",
                            "message": "Unexpected HTTP pipeline failure",
                            "details": {"type": type(exc).__name__, "reason": str(exc)},
                        }
                finally:
                    with self._state_lock:
                        self._running = False

            self._thread = threading.Thread(target=runner, name="feature-workbench-run", daemon=True)
            self._thread.start()
        return {"ok": True, "accepted": True, "resume": resume, "until_stage": until_stage}

    def background_state(self) -> dict[str, Any]:
        with self._state_lock:
            return {
                "running": self._running,
                "last_result": self._last_result,
                "last_error": self._last_error,
                "thread_alive": bool(self._thread and self._thread.is_alive()),
            }

    def ensure_not_running(self) -> None:
        with self._state_lock:
            if self._running:
                raise WorkbenchError(
                    "E_RUN_IN_PROGRESS",
                    "This operation is unavailable while a pipeline run is active",
                )


class WorkbenchHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple[str, int], service: WorkbenchService) -> None:
        self.service = service
        super().__init__(address, WorkbenchRequestHandler)


class WorkbenchRequestHandler(BaseHTTPRequestHandler):
    server: WorkbenchHTTPServer
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:
        print(f"[http] {self.address_string()} {format % args}")

    def _read_json(self) -> dict[str, Any]:
        length_header = self.headers.get("Content-Length", "0")
        try:
            length = int(length_header)
        except ValueError as exc:
            raise WorkbenchError("E_INPUT_INVALID", "Invalid Content-Length") from exc
        if length > 2 * 1024 * 1024:
            raise WorkbenchError("E_INPUT_INVALID", "Request body is too large")
        if length == 0:
            return {}
        body = self.rfile.read(length)
        try:
            value = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            raise WorkbenchError(
                "E_INPUT_INVALID",
                "Request body must be valid JSON",
                {"reason": str(exc)},
            ) from exc
        if not isinstance(value, dict):
            raise WorkbenchError("E_INPUT_INVALID", "JSON request body must be an object")
        return value

    def _send_json(
        self,
        status: int,
        value: Any,
        *,
        started: float,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        body = json.dumps(
            to_jsonable(value),
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        self.send_header("X-Query-Latency-Ms", f"{elapsed_ms:.3f}")
        try:
            budget = float(
                self.server.service.workbench.get_config()["budget"]["query_latency_ms"]
            )
            within_budget = "true" if elapsed_ms <= budget else "false"
        except BaseException:
            within_budget = "unknown"
        self.send_header("X-Query-Within-Budget", within_budget)
        if extra_headers:
            for key, header_value in extra_headers.items():
                self.send_header(key, header_value)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        started = time.perf_counter()
        path = urlparse(self.path).path
        service = self.server.service
        try:
            if path == "/api/health":
                self._send_json(200, {"ok": True, "service": "feature-workbench"}, started=started)
                return
            if path == "/api/status":
                status = service.workbench.status()
                status["background"] = service.background_state()
                self._send_json(200, status, started=started)
                return
            if path == "/api/version":
                version = service.workbench.version_report()
                version["background"] = service.background_state()
                self._send_json(200, version, started=started)
                return
            if path == "/api/budget":
                self._send_json(200, service.workbench.budget_report(), started=started)
                return
            if path == "/api/config":
                self._send_json(
                    200,
                    {"ok": True, "config": service.workbench.get_config()},
                    started=started,
                )
                return
            if path == "/api/result":
                state = service.background_state()
                self._send_json(
                    200,
                    {
                        "ok": state["last_error"] is None,
                        "running": state["running"],
                        "result": state["last_result"],
                        "error": state["last_error"],
                    },
                    started=started,
                )
                return
            if path == "/api/export":
                summary = service.workbench.read_selection_summary()
                if summary is None:
                    raise WorkbenchError(
                        "E_NOT_FOUND",
                        "No aggregate export is available yet",
                    )
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "result_digest": summary["result_digest"],
                        "exports": summary["exports"],
                        "summary": summary,
                    },
                    started=started,
                )
                return
            raise WorkbenchError("E_NOT_FOUND", "HTTP route not found", {"path": path})
        except WorkbenchError as exc:
            self._send_json(exc.http_status, exc.to_dict(), started=started)
        except BaseException as exc:
            self._send_json(
                500,
                WorkbenchError(
                    "E_INTERNAL",
                    "Unexpected HTTP handler failure",
                    {"type": type(exc).__name__, "reason": str(exc)},
                ).to_dict(),
                started=started,
            )

    def do_POST(self) -> None:
        started = time.perf_counter()
        path = urlparse(self.path).path
        service = self.server.service
        try:
            if path == "/api/run":
                payload = self._read_json()
                result = service.start_run(payload, resume=False)
                self._send_json(202, result, started=started)
                return
            if path == "/api/resume":
                payload = self._read_json()
                result = service.start_run(payload, resume=True)
                self._send_json(202, result, started=started)
                return
            if path == "/api/stop":
                payload = self._read_json()
                result = service.workbench.request_stop(str(payload.get("reason", "http_requested")))
                self._send_json(202, result, started=started)
                return
            if path == "/api/import":
                service.ensure_not_running()
                payload = self._read_json()
                source_dir = payload.get("source_dir")
                if not source_dir:
                    raise WorkbenchError("E_INPUT_INVALID", "source_dir is required")
                result = service.workbench.import_samples(str(source_dir))
                self._send_json(200, result, started=started)
                return
            if path == "/api/export":
                service.ensure_not_running()
                result = service.workbench.export_result()
                self._send_json(200, result, started=started)
                return
            raise WorkbenchError("E_NOT_FOUND", "HTTP route not found", {"path": path})
        except WorkbenchError as exc:
            self._send_json(exc.http_status, exc.to_dict(), started=started)
        except BaseException as exc:
            self._send_json(
                500,
                WorkbenchError(
                    "E_INTERNAL",
                    "Unexpected HTTP handler failure",
                    {"type": type(exc).__name__, "reason": str(exc)},
                ).to_dict(),
                started=started,
            )

    def do_PUT(self) -> None:
        started = time.perf_counter()
        path = urlparse(self.path).path
        service = self.server.service
        try:
            if path == "/api/config":
                service.ensure_not_running()
                payload = self._read_json()
                if "config" in payload:
                    from .config import load_yaml, normalize_config, dump_config
                    from pathlib import Path

                    raw = payload["config"]
                    if not isinstance(raw, dict):
                        raise WorkbenchError("E_CONFIG_INVALID", "config must be an object")
                    normalized = normalize_config(raw)
                    dump_config(Path(service.workbench.config_path), normalized)
                    result = {
                        "ok": True,
                        "config": normalized,
                        "path": str(service.workbench.config_path),
                    }
                else:
                    changes = payload.get("changes")
                    if not isinstance(changes, dict) or not changes:
                        raise WorkbenchError(
                            "E_CONFIG_INVALID",
                            "changes must be a non-empty object",
                        )
                    result = service.workbench.update_config(changes)
                self._send_json(200, result, started=started)
                return
            raise WorkbenchError("E_NOT_FOUND", "HTTP route not found", {"path": path})
        except WorkbenchError as exc:
            self._send_json(exc.http_status, exc.to_dict(), started=started)
        except BaseException as exc:
            self._send_json(
                500,
                WorkbenchError(
                    "E_INTERNAL",
                    "Unexpected HTTP handler failure",
                    {"type": type(exc).__name__, "reason": str(exc)},
                ).to_dict(),
                started=started,
            )


def make_server(
    workbench: Workbench,
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
) -> WorkbenchHTTPServer:
    service = WorkbenchService(workbench)
    return WorkbenchHTTPServer((host, int(port)), service)


def serve(workbench: Workbench, *, host: str = "127.0.0.1", port: int = 8765) -> None:
    server = make_server(workbench, host=host, port=port)
    actual_host, actual_port = server.server_address[:2]
    print(f"feature-workbench HTTP listening on http://{actual_host}:{actual_port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()

