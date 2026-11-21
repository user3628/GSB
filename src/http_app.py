"""Loopback-only HTTP interface for the workbench.

The server binds exclusively to ``127.0.0.1`` and never opens an external
socket.  Long runs execute in a background daemon thread so that stop and
status requests remain available while folds are being processed.
"""
from __future__ import annotations

import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Optional, Tuple

from .checkpoints import (list_checkpoints, read_state, request_stop,
                          reset_workspace)
from .config import load_config
from .errors import WorkbenchError
from .pipeline import run_pipeline
from .report import export_all


class _ServerContext:
    def __init__(self, workspace: str, manifest: str, config: str):
        self.workspace = workspace
        self.manifest = manifest
        self.config = config
        self.lock = threading.Lock()
        self.thread: Optional[threading.Thread] = None
        self.last_error: Optional[Dict[str, Any]] = None

    def _job(self, parallel: bool,
             stop_after_folds: Optional[int], export: bool) -> None:
        try:
            cfg = load_config(self.config)
            state = run_pipeline(self.workspace, self.manifest, cfg,
                                 parallel=parallel,
                                 stop_after_folds=stop_after_folds)
            if export and state.get("run_status") == "completed":
                export_all(self.workspace, state)
        except WorkbenchError as exc:
            self.last_error = exc.to_dict()
        except Exception as exc:  # pragma: no cover - defensive
            self.last_error = {"code": "E_WORKER_FAILED",
                               "message": str(exc), "context": {}}

    def start_job(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        with self.lock:
            if self.thread is not None and self.thread.is_alive():
                return {"accepted": False,
                        "reason": "a pipeline job is already running"}
            self.last_error = None
            parallel = bool(payload.get("parallel", True))
            stop_after_folds = payload.get("stop_after_folds")
            export = bool(payload.get("export", False))
            self.thread = threading.Thread(
                target=self._job,
                args=(parallel, stop_after_folds, export),
                daemon=True)
            self.thread.start()
            return {"accepted": True, "parallel": parallel}

    def job_status(self) -> Dict[str, Any]:
        running = bool(self.thread and self.thread.is_alive())
        return {"job_running": running, "last_error": self.last_error}


class WorkbenchHandler(BaseHTTPRequestHandler):
    context: _ServerContext = None  # type: ignore[assignment]

    def log_message(self, fmt, *args):  # quiet deterministic logging
        return

    def _send(self, status: int, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def _state(self) -> Dict[str, Any]:
        return read_state(self.context.workspace) or \
            {"run_status": "never_run"}

    def do_GET(self) -> None:  # noqa: N802 - http.server API
        try:
            if self.path == "/health":
                self._send(200, {"status": "ok", "bind": "127.0.0.1"})
            elif self.path == "/api/status":
                state = self._state()
                state["job"] = self.context.job_status()
                self._send(200, state)
            elif self.path == "/api/fingerprints":
                state = self._state()
                self._send(200, {
                    "config_fingerprint": state.get("config_fingerprint"),
                    "manifest_fingerprint": state.get("manifest_fingerprint"),
                    "aggregate_fingerprint":
                        state.get("aggregate_fingerprint"),
                    "checkpoints": state.get("checkpoints", []),
                    "invalidation": state.get("invalidation", []),
                })
            elif self.path == "/api/unprocessed":
                state = self._state()
                self._send(200, {
                    "rejected_folds": state.get("rejected_folds", []),
                    "not_started_folds":
                        state.get("not_started_folds", []),
                    "series_errors": state.get("series_errors", []),
                    "failures": state.get("failures", []),
                })
            elif self.path == "/api/checkpoints":
                records = list_checkpoints(self.context.workspace)
                self._send(200, {"checkpoints": [{
                    "shard_id": item["shard_id"],
                    "sequence": item["sequence"],
                    "input_fingerprint": item["input_fingerprint"],
                    "product_fingerprint": item["product_fingerprint"],
                    "completed_folds": item["completed_folds"],
                } for item in records]})
            elif self.path == "/api/metrics":
                state = self._state()
                self._send(200, {"metrics": state.get("metrics"),
                                 "fold_curve": state.get("fold_curve", [])})
            elif self.path == "/api/budget":
                state = self._state()
                self._send(200, {"scheduler": state.get("scheduler"),
                                 "rejected_folds":
                                     state.get("rejected_folds", [])})
            else:
                self._send(404, {"code": "E_NOT_FOUND",
                                 "message": f"unknown path {self.path}"})
        except WorkbenchError as exc:
            self._send(400, exc.to_dict())

    def do_POST(self) -> None:  # noqa: N802 - http.server API
        try:
            payload = self._body()
            if self.path == "/api/run":
                self._send(202, self.context.start_job(payload))
            elif self.path == "/api/resume":
                payload.setdefault("parallel", True)
                self._send(202, self.context.start_job(payload))
            elif self.path == "/api/stop":
                request_stop(self.context.workspace)
                self._send(200, {"stop_requested": True})
            elif self.path == "/api/reset":
                with self.context.lock:
                    reset_workspace(self.context.workspace)
                self._send(200, {"reset": True})
            elif self.path == "/api/export":
                state = self._state()
                if state.get("run_status") != "completed":
                    self._send(409, {"code": "E_INVALID_STAGE",
                                     "message": "pipeline not completed"})
                    return
                paths = export_all(self.context.workspace, state)
                self._send(200, {"exports": paths})
            else:
                self._send(404, {"code": "E_NOT_FOUND",
                                 "message": f"unknown path {self.path}"})
        except WorkbenchError as exc:
            self._send(400, exc.to_dict())
        except json.JSONDecodeError as exc:
            self._send(400, {"code": "E_DATA_INVALID",
                             "message": f"invalid JSON body: {exc}"})


def create_server(workspace: str, manifest: str, config: str,
                  host: str = "127.0.0.1", port: int = 8765
                  ) -> Tuple[ThreadingHTTPServer, _ServerContext]:
    if host not in ("127.0.0.1", "localhost", "::1"):
        raise WorkbenchError("only loopback binds are allowed",
                             code="E_CONFIG_INVALID",
                             context={"host": host})
    context = _ServerContext(workspace, manifest, config)

    class _BoundHandler(WorkbenchHandler):
        pass

    _BoundHandler.context = context
    server = ThreadingHTTPServer((host, port), _BoundHandler)
    return server, context


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="loopback-only workbench API")
    parser.add_argument("--workspace", default="workbench")
    parser.add_argument("--manifest", default="samples/manifest.json")
    parser.add_argument("--config", default="config/backtest.yaml")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args(argv)
    server, _ = create_server(args.workspace, args.manifest, args.config,
                              host=args.host, port=args.port)
    print(f"workbench HTTP API listening on http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
