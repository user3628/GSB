"""Batteries-included local HTTP demo API (stdlib only, single background thread)."""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .checkpoints import CheckpointStore
from .codec import write_json
from .config import PipelineConfig
from .errors import ChainError
from .evidence import export_manifest, import_manifest, reconstruct_decisions
from .pipeline import Pipeline
from .samplegen import build_sample


class _Handler(BaseHTTPRequestHandler):
    pipeline = None
    workspace = None

    def log_message(self, format, *args):  # silence default stderr noise
        return

    def _send(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def do_GET(self):
        try:
            if self.path == "/status":
                return self._send(200, self.pipeline.status())
            if self.path == "/fingerprints":
                return self._send(200, self.pipeline.status()["stages"])
            if self.path == "/demotions":
                if not self.pipeline.store.has_stage("resolve"):
                    return self._send(409, {"error": "resolve stage missing"})
                return self._send(200, self.pipeline.store.get_stage("resolve")["demotions"])
            if self.path == "/evidence":
                manifest = import_manifest("%s/evidence_manifest.json" % self.workspace)
                return self._send(200, manifest)
            if self.path == "/reconstruct":
                manifest = import_manifest("%s/evidence_manifest.json" % self.workspace)
                return self._send(200, reconstruct_decisions(manifest))
            if self.path == "/stats/conflicts":
                if not self.pipeline.store.has_stage("resolve"):
                    return self._send(409, {"error": "resolve stage missing"})
                return self._send(200, self.pipeline.store.get_stage("resolve")["stats"])
            return self._send(404, {"error": "not found", "path": self.path})
        except ChainError as exc:
            return self._send(422, exc.to_dict())

    def do_POST(self):
        try:
            body = self._body()
            if self.path == "/sample/import":
                sample = body.get("sample") or build_sample(body.get("seed", 20241123))
                self.pipeline.load_sample(sample)
                return self._send(200, {"imported": sample["sample_id"], "pages": len(sample["pages"])})
            if self.path == "/evidence/import":
                manifest = body["manifest"]
                write_json("%s/imported_evidence.json" % self.workspace, manifest)
                checked = import_manifest("%s/imported_evidence.json" % self.workspace)
                return self._send(200, {"records": checked["record_count"]})
            if self.path == "/config":
                config = PipelineConfig.from_dict(body.get("config", body))
                self.pipeline.update_config(config)
                return self._send(200, {"config_digest": config.full_digest()})
            if self.path == "/run":
                result = self.pipeline.run()
                return self._send(200, result)
            if self.path == "/resume":
                return self._send(200, self.pipeline.resume())
            if self.path == "/run/full":
                return self._send(200, self.pipeline.run_full())
            if self.path == "/run/incremental":
                sample = body.get("sample")
                config = PipelineConfig.from_dict(body["config"]) if body.get("config") else None
                return self._send(200, self.pipeline.run_incremental(sample, config))
            if self.path == "/stop":
                CheckpointStore(self.workspace).request_stop()
                return self._send(200, {"stop": True})
            if self.path == "/evidence/export":
                manifest = import_manifest("%s/evidence_manifest.json" % self.workspace)
                path = export_manifest(manifest, body.get("path", "%s/exported_evidence.json" % self.workspace))
                return self._send(200, {"exported": path})
            return self._send(404, {"error": "not found", "path": self.path})
        except ChainError as exc:
            return self._send(422, exc.to_dict())


def serve(workspace: str, host: str = "127.0.0.1", port: int = 8765):
    pipeline = Pipeline(workspace)
    _Handler.pipeline = pipeline
    _Handler.workspace = workspace
    server = ThreadingHTTPServer((host, port), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread
