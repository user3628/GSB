"""Local-only JSON HTTP application for the motion workspace."""

from __future__ import annotations

import json
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from . import __version__
from .errors import EngineError, invalid_state
from .runner import StopController
from .workspace import Workspace


class ApiState:
    def __init__(self, workspace_root: str | Path) -> None:
        self.workspace_root = Path(workspace_root).expanduser().resolve()
        self._active: dict[str, StopController] = {}
        self._lock = threading.Lock()

    def workspace(self) -> Workspace:
        return Workspace(self.workspace_root)

    def begin_run(self, series_id: str) -> tuple[StopController, Workspace]:
        workspace = self.workspace()
        workspace.series_data(series_id)
        controller = StopController()
        with self._lock:
            if series_id in self._active:
                raise invalid_state(
                    "a run is already active for this series",
                    series_id=series_id,
                    position="run",
                )
            self._active[series_id] = controller
        return controller, workspace

    def end_run(self, series_id: str, controller: StopController) -> None:
        with self._lock:
            if self._active.get(series_id) is controller:
                self._active.pop(series_id, None)

    def request_stop(self, series_id: str) -> dict[str, Any]:
        with self._lock:
            controller = self._active.get(series_id)
            if controller is None:
                raise invalid_state(
                    "no active run to stop",
                    series_id=series_id,
                    position="stop",
                )
            controller.request()
        return {"series_id": series_id, "stop_requested": True}

    def active_series(self) -> list[str]:
        with self._lock:
            return sorted(self._active)


class MotionRequestHandler(BaseHTTPRequestHandler):
    server_version = f"MotionEngine/{__version__}"
    protocol_version = "HTTP/1.1"

    @property
    def state(self) -> ApiState:
        return self.server.api_state  # type: ignore[attr-defined]

    def log_message(self, format: str, *args: Any) -> None:
        print(f"[http] {self.address_string()} {format % args}")

    def _send_json(self, status: int, payload: Any) -> None:
        body = json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        ).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, error: EngineError) -> None:
        if error.code == "E_STALE_CHECKPOINT":
            status = HTTPStatus.CONFLICT
        elif error.code in {"E_INVALID_STATE", "E_TIMELINE_OVERFLOW"}:
            status = HTTPStatus.UNPROCESSABLE_ENTITY
        else:
            status = HTTPStatus.BAD_REQUEST
        self._send_json(status, {"ok": False, "error": error.to_dict()})

    def _read_body(self) -> dict[str, Any]:
        length_header = self.headers.get("Content-Length", "0")
        try:
            length = int(length_header)
        except ValueError as exc:
            raise invalid_state("invalid Content-Length", position="request") from exc
        if length < 0 or length > 1_048_576:
            raise invalid_state("request body is too large", position="request")
        raw = self.rfile.read(length) if length else b"{}"
        try:
            value = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as exc:
            raise invalid_state("request body must be valid JSON", position="request") from exc
        if not isinstance(value, dict):
            raise invalid_state("request JSON must be an object", position="request")
        return value

    def _parts(self) -> list[str]:
        parsed = urlparse(self.path)
        return [unquote(part) for part in parsed.path.split("/") if part]

    def do_GET(self) -> None:  # noqa: N802
        try:
            parts = self._parts()
            if parts == ["api", "health"]:
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "status": "ok",
                        "version": __version__,
                        "workspace": str(self.state.workspace_root),
                        "active_series": self.state.active_series(),
                    },
                )
                return
            if parts == ["api"]:
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "endpoints": [
                            "POST /api/import",
                            "GET /api/series",
                            "GET /api/series/{id}",
                            "GET /api/series/{id}/report",
                            "POST /api/series/{id}/run",
                            "POST /api/series/{id}/step",
                            "POST /api/series/{id}/resume",
                            "POST /api/series/{id}/stop",
                        ],
                    },
                )
                return
            workspace = self.state.workspace()
            if parts == ["api", "series"]:
                self._send_json(HTTPStatus.OK, {"ok": True, **workspace.all_status()})
                return
            if len(parts) == 3 and parts[:2] == ["api", "series"]:
                self._send_json(
                    HTTPStatus.OK,
                    {"ok": True, "series": workspace.inspect_series(parts[2])},
                )
                return
            if (
                len(parts) == 4
                and parts[:2] == ["api", "series"]
                and parts[3] in {"report", "steps", "checkpoints"}
            ):
                if parts[3] == "report":
                    payload = workspace.report(parts[2])
                    self._send_json(HTTPStatus.OK, {"ok": True, "report": payload})
                elif parts[3] == "steps":
                    payload = workspace.report(parts[2])
                    self._send_json(
                        HTTPStatus.OK,
                        {
                            "ok": True,
                            "series_id": parts[2],
                            "steps": payload["steps"],
                            "error_curve": payload["error_curve"],
                            "aggregate": payload["aggregate"],
                        },
                    )
                else:
                    self._send_json(
                        HTTPStatus.OK,
                        {"ok": True, "series": workspace.inspect_series(parts[2])},
                    )
                return
            raise invalid_state("endpoint not found", position="path", path=self.path)
        except EngineError as error:
            self._send_error(error)
        except Exception as exc:  # pragma: no cover - defensive HTTP boundary
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {
                    "ok": False,
                    "error": {
                        "code": "E_INTERNAL",
                        "message": str(exc),
                        "position": "server",
                    },
                },
            )

    def do_POST(self) -> None:  # noqa: N802
        try:
            parts = self._parts()
            body = self._read_body()
            if parts == ["api", "import"]:
                manifest_path = body.get("manifest_path")
                if not isinstance(manifest_path, str) or not manifest_path:
                    raise invalid_state(
                        "manifest_path is required", position="manifest_path"
                    )
                workspace = self.state.workspace()
                result = workspace.import_manifest(
                    manifest_path, reset=bool(body.get("reset", False))
                )
                self._send_json(HTTPStatus.OK, {"ok": True, **result})
                return
            if len(parts) != 4 or parts[:2] != ["api", "series"]:
                raise invalid_state("endpoint not found", position="path", path=self.path)
            series_id, action = parts[2], parts[3]
            if action == "stop":
                self._send_json(
                    HTTPStatus.ACCEPTED, {"ok": True, **self.state.request_stop(series_id)}
                )
                return
            if action not in {"run", "step", "resume"}:
                raise invalid_state("endpoint not found", position="path", path=self.path)

            max_steps = body.get("max_steps")
            if max_steps is not None and (
                isinstance(max_steps, bool) or not isinstance(max_steps, int) or max_steps < 0
            ):
                raise invalid_state(
                    "max_steps must be a non-negative integer",
                    series_id=series_id,
                    position="max_steps",
                )
            if action == "step":
                max_steps = 1
            from_step = body.get("from_step")
            if from_step is not None and (
                isinstance(from_step, bool) or not isinstance(from_step, int)
            ):
                raise invalid_state(
                    "from_step must be an integer",
                    series_id=series_id,
                    position="from_step",
                )
            step_delay = body.get("step_delay", 0.0)
            if not isinstance(step_delay, (int, float)) or isinstance(step_delay, bool):
                raise invalid_state(
                    "step_delay must be a number",
                    series_id=series_id,
                    position="step_delay",
                )
            step_delay = min(1.0, max(0.0, float(step_delay)))

            controller, workspace = self.state.begin_run(series_id)
            try:
                if step_delay:
                    def on_step(record: dict[str, Any]) -> None:
                        time.sleep(step_delay)

                else:
                    on_step = None
                report = workspace.run(
                    series_id,
                    max_steps=max_steps,
                    stop_controller=controller,
                    from_step=from_step,
                    on_step=on_step,
                )
            finally:
                self.state.end_run(series_id, controller)
            domain_report = {
                key: value for key, value in report.items() if key != "run"
            }
            self._send_json(
                HTTPStatus.OK,
                {"ok": True, "report": domain_report, "run": report.get("run")},
            )
        except EngineError as error:
            self._send_error(error)
        except Exception as exc:  # pragma: no cover - defensive HTTP boundary
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {
                    "ok": False,
                    "error": {
                        "code": "E_INTERNAL",
                        "message": str(exc),
                        "position": "server",
                    },
                },
            )


class MotionHttpServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        server_address: tuple[str, int],
        handler_class: type[BaseHTTPRequestHandler],
        api_state: ApiState,
    ) -> None:
        self.api_state = api_state
        super().__init__(server_address, handler_class)


def create_server(
    workspace_root: str | Path,
    host: str = "127.0.0.1",
    port: int = 18765,
) -> MotionHttpServer:
    return MotionHttpServer(
        (host, port), MotionRequestHandler, ApiState(workspace_root)
    )


def serve(workspace_root: str | Path, host: str = "127.0.0.1", port: int = 18765) -> None:
    server = create_server(workspace_root, host, port)
    bound_host, bound_port = server.server_address[:2]
    print(f"Motion engine HTTP server listening on http://{bound_host}:{bound_port}")
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
