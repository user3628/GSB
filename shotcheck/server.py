from __future__ import annotations

import json
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .checkpoints import CheckpointStore
from .errors import BusinessError
from .generator import generate_tier
from .pipeline import GracefulStop, Pipeline, StopController
from .util import canonical_bytes, ensure_dir, read_json
from .version import PIPELINE_VERSION


class ApiState:
    def __init__(self, workspace: str | Path):
        self.workspace = Path(workspace)
        self.root = self.workspace.parent
        self.run_lock = threading.Lock()
        self.controller_lock = threading.Lock()
        self.active_controller: StopController | None = None

    def begin_controller(self) -> StopController:
        controller = StopController()
        with self.controller_lock:
            self.active_controller = controller
        return controller

    def end_controller(self, controller: StopController) -> None:
        with self.controller_lock:
            if self.active_controller is controller:
                self.active_controller = None

    def request_stop(self) -> bool:
        with self.controller_lock:
            controller = self.active_controller
        if controller is None:
            return False
        controller.request()
        return True


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "ShotCheckHTTP/1.0"

    @property
    def state(self) -> ApiState:
        return self.server.state  # type: ignore[attr-defined]

    def log_message(self, format_string: str, *args: Any) -> None:
        print(f"[http] {self.address_string()} {format_string % args}")

    def _send_json(self, status: int, value: Any) -> None:
        payload = canonical_bytes(value)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        if length > 1_000_000:
            raise ValueError("request body is too large")
        raw = self.rfile.read(length)
        value = json.loads(raw.decode("utf-8"))
        if not isinstance(value, dict):
            raise ValueError("JSON request body must be an object")
        return value

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        try:
            if path == "/health":
                self._send_json(
                    HTTPStatus.OK,
                    {"ok": True, "pipeline_version": PIPELINE_VERSION},
                )
                return
            if path == "/status":
                store = CheckpointStore(self.state.workspace)
                manifest_path = self.state.workspace / "inputs/manifest.json"
                manifest = read_json(manifest_path) if manifest_path.exists() else None
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "workspace": str(self.state.workspace),
                        "checkpoints": store.status(),
                        "input_manifest": manifest,
                    },
                )
                return
            if path == "/result":
                result_path = self.state.workspace / "exports/result.json"
                if not result_path.exists():
                    self._send_json(
                        HTTPStatus.NOT_FOUND,
                        {
                            "ok": False,
                            "error": {
                                "code": "E_RESULT_NOT_FOUND",
                                "message": "run the pipeline before requesting the result",
                            },
                        },
                    )
                    return
                self._send_json(HTTPStatus.OK, read_json(result_path))
                return
            self._send_json(
                HTTPStatus.NOT_FOUND,
                {"ok": False, "error": {"code": "E_NOT_FOUND", "message": path}},
            )
        except Exception as exc:
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {
                    "ok": False,
                    "error": {"code": "E_INTERNAL", "message": str(exc)},
                },
            )

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        try:
            body = self._read_json_body()
            if path == "/run":
                self._handle_run(body)
                return
            if path == "/stop":
                requested = self.state.request_stop()
                self._send_json(
                    HTTPStatus.ACCEPTED,
                    {
                        "ok": True,
                        "stop_requested": requested,
                        "message": (
                            "stop will take effect at the next indivisible shot boundary"
                            if requested
                            else "no run is currently active"
                        ),
                    },
                )
                return
            if path == "/generate":
                tier = str(body.get("tier", "large"))
                if tier not in {"small", "large"}:
                    raise ValueError("tier must be 'small' or 'large'")
                seed = int(body.get("seed", 20261123))
                manifest = generate_tier(self.state.root / tier, tier, seed)
                self._send_json(HTTPStatus.OK, {"ok": True, "manifest": manifest})
                return
            self._send_json(
                HTTPStatus.NOT_FOUND,
                {"ok": False, "error": {"code": "E_NOT_FOUND", "message": path}},
            )
        except BusinessError as exc:
            self._send_json(HTTPStatus.CONFLICT, {"ok": False, "error": exc.to_dict()})
        except (ValueError, json.JSONDecodeError) as exc:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "ok": False,
                    "error": {"code": "E_INVALID_REQUEST", "message": str(exc)},
                },
            )
        except Exception as exc:
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {
                    "ok": False,
                    "error": {"code": "E_INTERNAL", "message": str(exc)},
                },
            )

    def _handle_run(self, body: dict[str, Any]) -> None:
        if not self.state.run_lock.acquire(blocking=False):
            self._send_json(
                HTTPStatus.CONFLICT,
                {
                    "ok": False,
                    "error": {"code": "E_RUN_BUSY", "message": "a run is already active"},
                },
            )
            return
        controller = self.state.begin_controller()
        try:
            force = bool(body.get("force", False))
            stopped_shots = body.get("stop_after_shots")
            stop_stage = body.get("stop_after_stage")
            if stopped_shots is not None:
                controller.configure_target(stop_stage, int(stopped_shots))
            try:
                run = Pipeline(
                    self.state.workspace, stop_controller=controller
                ).run(force=force)
            except GracefulStop as exc:
                self._send_json(
                    HTTPStatus.ACCEPTED,
                    {
                        "ok": True,
                        "status": "stopped",
                        "stage": exc.stage,
                        "completed_shots": exc.completed_shots,
                        "shot_id": exc.shot_id,
                    },
                )
                return
            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "status": "complete",
                    "run_meta": run["run_meta"],
                    "summary": run["result"]["summary"],
                },
            )
        finally:
            self.state.end_controller(controller)
            self.state.run_lock.release()


class ShotCheckHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple[str, int], state: ApiState):
        self.state = state
        super().__init__(address, ApiHandler)


def serve(
    workspace: str | Path,
    *,
    host: str = "127.0.0.1",
    port: int = 18765,
) -> ShotCheckHTTPServer:
    workspace_path = Path(workspace)
    ensure_dir(workspace_path)
    state = ApiState(workspace_path)
    return ShotCheckHTTPServer((host, port), state)
