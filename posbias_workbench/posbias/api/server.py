"""Threaded loopback-only HTTP service (stdlib, no third-party web stack)."""

from __future__ import annotations

import copy
import json
import os
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from ..core.checkpoints import STAGES
from ..core.config import AppConfig
from ..core.injection import INJECTION_KINDS, run_injection
from ..core.pipeline import Pipeline, StopFlag
from ..errors import WorkbenchError


def port_available(host: str, port: int) -> bool:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind((host, port))
    except OSError:
        return False
    finally:
        sock.close()
    return True


class WorkbenchService:
    def __init__(self, *, config_path, work_dir, reference_path=None):
        self.config_path = os.path.abspath(config_path)
        self.work_dir = os.path.abspath(work_dir)
        self.reference_path = reference_path or os.path.join(
            work_dir, "reference", "reference_metrics.yaml")
        self.stop_flag = StopFlag()
        self._lock = threading.Lock()
        self._thread = None
        self._last_result = None

    def load_config(self):
        return AppConfig.load(self.config_path)

    def save_config(self, data):
        cfg = AppConfig(copy.deepcopy(data))
        cfg.validate()
        cfg.dump(self.config_path)
        return cfg

    def make_pipeline(self, config=None):
        return Pipeline(
            config or self.load_config(), self.work_dir,
            shard_dir=os.path.join(self.work_dir, "shards"),
            reference_path=self.reference_path, stop_flag=self.stop_flag)

    def run(self, background=False):
        if background:
            if self._thread and self._thread.is_alive():
                return {"status": "already_running"}
            self.stop_flag.reset()
            self._thread = threading.Thread(
                target=self._run_worker, daemon=True)
            self._thread.start()
            return {"status": "started"}
        return self._run_worker()

    def _run_worker(self):
        try:
            with self._lock:
                report = self.make_pipeline().run()
            self._last_result = {"status": "completed", "report": report}
        except WorkbenchError as exc:
            self._last_result = {"status": "error", "error": exc.to_dict()}
        return self._last_result

    def request_stop(self):
        self.stop_flag.request()
        return {"status": "stop_requested"}

    def resume(self):
        self.stop_flag.reset()
        return self.run(background=False)

    def status(self):
        running = bool(self._thread and self._thread.is_alive())
        return {"running": running,
                "stop_requested": self.stop_flag.requested(),
                "last_result": self._last_result}

    def invalidations(self):
        pipeline = self.make_pipeline()
        store = pipeline.store
        out = []
        for stage in STAGES:
            record = store.load(stage)
            out.append({
                "stage": stage,
                "present": record is not None,
                "sequence": record["sequence"] if record else None,
                "fingerprint": record["fingerprint"] if record else None,
            })
        return {"stages": out,
                "config_fingerprint": pipeline.config.fingerprint(),
                "shard_fingerprint": pipeline._shard_fingerprint()}

    def inject(self, kind):
        if kind not in INJECTION_KINDS:
            raise ValueError(f"unknown injection kind: {kind}")
        return run_injection(kind=kind, config=self.load_config(),
                             work_dir=self.work_dir)


def make_handler(service: WorkbenchService):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):
            return

        def _send(self, payload, status=200):
            body = json.dumps(payload, ensure_ascii=False,
                              allow_nan=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self):
            length = int(self.headers.get("Content-Length", 0))
            if length == 0:
                return {}
            return json.loads(self.rfile.read(length).decode("utf-8"))

        def do_GET(self):
            path = urlparse(self.path).path.rstrip("/") or "/"
            try:
                if path == "/health":
                    return self._send({"status": "ok"})
                if path == "/config":
                    return self._send(service.load_config().data)
                if path == "/status":
                    return self._send(service.status())
                if path == "/invalidations":
                    return self._send(service.invalidations())
                if path == "/report":
                    report_path = os.path.join(
                        service.work_dir, "reports", "diagnostic_report.json")
                    with open(report_path, "r", encoding="utf-8") as handle:
                        return self._send(json.load(handle))
                if path == "/schemes":
                    pipeline = service.make_pipeline()
                    record = pipeline.store.load("binning")
                    if record is None:
                        return self._send(
                            {"error": "binning stage not available"}, 409)
                    return self._send(record["artifact_summary"])
                if path.startswith("/schemes/"):
                    scheme_id = path.split("/", 2)[2]
                    pipeline = service.make_pipeline()
                    record = pipeline.store.load("binning")
                    if record is None:
                        return self._send(
                            {"error": "binning stage not available"}, 409)
                    for scheme in record["artifact_summary"]["schemes"]:
                        if scheme["scheme"] == scheme_id:
                            return self._send(scheme)
                    return self._send({"error": "scheme not found"}, 404)
                return self._send({"error": "not found"}, 404)
            except WorkbenchError as exc:
                return self._send(exc.to_dict(), 400)
            except FileNotFoundError as exc:
                return self._send({"error": str(exc)}, 404)

        def do_POST(self):
            path = urlparse(self.path).path.rstrip("/")
            try:
                if path == "/run":
                    payload = self._read_json()
                    return self._send(
                        service.run(background=bool(payload.get("background"))))
                if path == "/stop":
                    return self._send(service.request_stop())
                if path == "/resume":
                    return self._send(service.resume())
                if path == "/inject":
                    payload = self._read_json()
                    return self._send(service.inject(payload["kind"]))
                return self._send({"error": "not found"}, 404)
            except WorkbenchError as exc:
                return self._send(exc.to_dict(), 400)
            except (ValueError, KeyError) as exc:
                return self._send({"code": "E_INVALID_PARAM",
                                   "message": str(exc)}, 400)

        def do_PUT(self):
            path = urlparse(self.path).path.rstrip("/")
            try:
                if path == "/config":
                    payload = self._read_json()
                    cfg = service.save_config(payload)
                    return self._send({"status": "saved",
                                       "config_fingerprint":
                                           cfg.fingerprint()})
                return self._send({"error": "not found"}, 404)
            except WorkbenchError as exc:
                return self._send(exc.to_dict(), 400)

    return Handler


def serve(service: WorkbenchService, *, host="127.0.0.1", port=8765):
    if host not in ("127.0.0.1", "::1", "localhost"):
        raise ValueError("service must bind to a loopback address")
    if not port_available(host, port):
        raise OSError(f"loopback port {port} on {host} is unavailable")
    httpd = ThreadingHTTPServer((host, port), make_handler(service))
    return httpd
