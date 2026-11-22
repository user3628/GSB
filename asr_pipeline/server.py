"""Stdlib HTTP API for import, staged decoding, status, stop and recovery."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .checkpoint import CheckpointStore
from .config import (
    AlignConfig,
    BudgetConfig,
    DecoderConfig,
    FrameConfig,
    PipelineConfig,
)
from .errors import PipelineError
from .hashing import canonical_json
from .manifest import load_manifest, ordered_shards
from .pipeline import Pipeline, clear_stop, request_stop

DEFAULT_DATA_DIR = "data"
DEFAULT_WORKSPACE = "workspace"


class ApiState:
    def __init__(self):
        self.data_dir = DEFAULT_DATA_DIR
        self.workspace = DEFAULT_WORKSPACE
        self.manifest = None
        self.config = None
        self.last_summary = None
        self.lock = threading.Lock()

    def ensure_loaded(self):
        if self.manifest is None:
            import os

            path = os.path.join(self.data_dir, "manifest.json")
            self.manifest = load_manifest(path)
            self.config = PipelineConfig()
        return self.manifest, self.config


STATE = ApiState()


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "DeterministicASR/1.0"

    def log_message(self, fmt, *args):
        return

    def _send(self, status, payload):
        body = canonical_json(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def do_GET(self):
        if self.path in ("/health", "/healthz"):
            self._send(200, {"status": "ok"})
            return
        if self.path == "/version":
            from . import __version__

            store = CheckpointStore(STATE.workspace)
            state = store.stage_status()
            manifest, config = STATE.ensure_loaded()
            self._send(
                200,
                {
                    "version": __version__ ,
                    "config_fingerprint": config.fingerprint(),
                    "manifest_fingerprint": __import__(
                        "asr_pipeline.hashing", fromlist=["digest_json"]
                    ).digest_json(manifest),
                    "stages": state["shards"],
                    "ignored": state["ignored"],
                    "stop_requested": __import__("os").path.exists(
                        __import__("os").path.join(STATE.workspace, "STOP_REQUESTED")
                    ),
                },
            )
            return
        if self.path == "/status":
            store = CheckpointStore(STATE.workspace)
            state = store.stage_status()
            state["summary"] = store.read_summary()
            self._send(200, state)
            return
        if self.path == "/diagnostics":
            store = CheckpointStore(STATE.workspace)
            summary = store.read_summary() or {}
            self._send(200, {"diagnostics": summary.get("diagnostics", [])})
            return
        self._send(404, {"code": "E_NOT_FOUND", "message": self.path})

    def do_POST(self):
        try:
            self._route_post()
        except PipelineError as exc:
            self._send(409, exc.to_dict())
        except FileNotFoundError as exc:
            self._send(404, {"code": "E_MANIFEST", "message": str(exc)})
        except json.JSONDecodeError as exc:
            self._send(400, {"code": "E_BAD_REQUEST", "message": str(exc)})

    def _route_post(self):
        body = self._read_body()
        if self.path == "/import":
            STATE.data_dir = body.get("data_dir", DEFAULT_DATA_DIR)
            STATE.workspace = body.get("workspace", DEFAULT_WORKSPACE)
            STATE.manifest = load_manifest(
                __import__("os").path.join(STATE.data_dir, "manifest.json")
            )
            frame_args = body.get("frame", {})
            frame = FrameConfig(
                frame_ms=frame_args.get("frame_ms", STATE.manifest["frame_ms"]),
                hop_ms=frame_args.get("hop_ms", STATE.manifest["hop_ms"]),
                bands=frame_args.get("bands", STATE.manifest["bands"]),
                sample_rate=STATE.manifest["sample_rate"],
                marker_bins=frame_args.get("marker_bins", 20),
            )
            frame.validate()
            STATE.config = PipelineConfig(
                frame=frame,
                decoder=DecoderConfig(
                    min_silence_frames=body.get("min_silence_frames", 5),
                    min_word_frames=body.get("min_word_frames", 3),
                ),
                align=AlignConfig(tolerance_ms=body.get("tolerance_ms", 40.0)),
                budget=BudgetConfig(
                    global_frame_budget=body.get("global_budget", 100000),
                    worker_frame_quota=body.get("worker_quota", 60000),
                    shard_frame_latency=body.get("shard_latency", 20000),
                    workers=body.get("workers", 2),
                ),
            )
            self._send(
                200,
                {
                    "data_dir": STATE.data_dir,
                    "workspace": STATE.workspace,
                    "shard_count": len(STATE.manifest["shards"]),
                    "config_fingerprint": STATE.config.fingerprint(),
                },
            )
            return
        manifest, config = STATE.ensure_loaded()
        pipeline = Pipeline(config, manifest, STATE.data_dir, STATE.workspace)
        if self.path == "/run":
            clear_stop(STATE.workspace)
            summary = pipeline.run()
            STATE.last_summary = summary
            self._send(200, summary)
            return
        if self.path == "/stage":
            requested = body.get("stages", ["frontend", "decode", "align", "summary"])
            if "summary" in requested:
                summary = pipeline.run()
                STATE.last_summary = summary
                self._send(200, {"summary": summary})
                return
            plan, _ = pipeline.plan_admission()
            completed = []
            for item in plan:
                if not item["admitted"] or pipeline.stop_requested():
                    break
                entry = next(
                    shard for shard in ordered_shards(manifest)
                    if shard["shard_id"] == item["shard_id"]
                )
                result = pipeline.process_shard_stages(
                    entry,
                    __import__("os").path.join(STATE.data_dir, entry["wav_file"]),
                    requested,
                )
                completed.append(
                    {"shard_id": result["shard_id"], "stages": result["stages"]}
                )
            self._send(200, {"admission": plan, "completed": completed})
            return
        if self.path == "/resume":
            recovery = pipeline.store.recover()
            summary = pipeline.run()
            STATE.last_summary = summary
            self._send(200, {"recovery": recovery, "summary": summary})
            return
        if self.path == "/stop":
            path = request_stop(STATE.workspace)
            self._send(200, {"stop_file": path, "status": "stop_requested"})
            return
        self._send(404, {"code": "E_NOT_FOUND", "message": self.path})


def create_server(host="127.0.0.1", port=8080):
    return ThreadingHTTPServer((host, port), ApiHandler)


def serve(host="127.0.0.1", port=8080):
    server = create_server(host, port)
    server.serve_forever()
