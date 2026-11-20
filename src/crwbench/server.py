"""Local-only HTTP interface built on the Python standard library."""
from __future__ import annotations

import json
import os
import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .checkpoints import CheckpointStore, STAGES
from .config import load_config
from .errors import WorkbenchError
from .pipeline import Pipeline, load_shards
from .util import canonical_dumps


class WorkbenchServer:
    def __init__(self, config_path: str, workdir: str):
        self.config_path = config_path
        self.workdir = workdir
        self.last_shards: list[str] = []

    def handle(self, method: str, path: str, body: dict) -> tuple[int, dict]:
        if method == "GET" and path == "/health":
            return 200, {"status": "ok"}
        config = load_config(self.config_path)
        pipeline = Pipeline(config, self.workdir)
        store = CheckpointStore(self.workdir)

        if method == "POST" and path == "/shards/import":
            shards = body.get("shard_paths", [])
            load_shards(shards)
            self.last_shards = shards
            return 200, {"imported": len(shards), "shard_paths": shards}

        if method == "POST" and path == "/pipeline/run":
            shards = body.get("shard_paths") or self.last_shards
            force = bool(body.get("force", False))
            return 200, pipeline.run(shards, force=force)

        if method == "GET" and path == "/stages/status":
            records = store.load_records()
            return 200, {
                "stages": STAGES,
                "records": records,
                "stop_requested": pipeline.stop_requested(),
            }

        if method == "GET" and path == "/stages/fingerprints":
            shards = body.get("shard_paths") or self.last_shards
            clauses, input_digest, _ = load_shards(shards)
            return 200, {
                "input_digest": input_digest,
                "config_fingerprint": config.fingerprint,
                "stage_digests": pipeline.expected_digests(clauses),
            }

        if method == "GET" and path == "/strategy/rationale":
            final = store.load_artifact("summary")
            return 200, {
                "strategy_decision": final["strategy_decision"],
                "candidate_details": final["candidate_details"],
                "plan_membership": final["plan_membership"],
            }

        if method == "GET" and path == "/levels/evidence":
            clause_id = body.get("clause_id")
            final = store.load_artifact("summary")
            levels = final["level_list"]
            if clause_id:
                levels = [r for r in levels if r["clause_id"] == clause_id]
            scored = store.load_artifact("scoring")
            score_map = {s["clause_id"]: s for s in scored}
            enriched = []
            for record in levels:
                enriched.append(
                    {
                        "level_record": record,
                        "score_detail": score_map.get(record["clause_id"]),
                    }
                )
            return 200, {"evidence": enriched}

        if method == "POST" and path == "/stop":
            pipeline.request_stop()
            return 200, {"stop_requested": True}

        if method == "POST" and path == "/resume":
            shards = body.get("shard_paths") or self.last_shards
            return 200, pipeline.resume(shards)

        if method == "GET" and path == "/levels/export":
            final = store.load_artifact("summary")
            return 200, {"level_list": final["level_list"]}

        return 404, {"error_code": "E_INVALID_STAGE", "message": f"unknown route: {method} {path}"}


def build_handler(server: WorkbenchServer):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):  # quiet deterministic logs
            return

        def _send(self, status: int, payload: dict):
            data = canonical_dumps(payload)
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _read_body(self) -> dict:
            length = int(self.headers.get("Content-Length", 0))
            if length == 0:
                return {}
            raw = self.rfile.read(length)
            return json.loads(raw.decode("utf-8"))

        def do_GET(self):
            self._dispatch("GET", read_body=False)

        def do_POST(self):
            self._dispatch("POST", read_body=True)

        def _dispatch(self, method: str, read_body: bool):
            try:
                body = self._read_body() if read_body else {}
                status, payload = server.handle(method, self.path.split("?")[0], body)
                self._send(status, payload)
            except WorkbenchError as exc:
                self._send(409, exc.to_dict())
            except json.JSONDecodeError as exc:
                self._send(400, {"error_code": "E_BAD_INPUT", "message": str(exc)})
            except Exception as exc:  # noqa: BLE001 - stable API envelope
                self._send(500, {"error_code": "E_INTERNAL", "message": str(exc)})

    return Handler


def serve(
    config_path: str,
    workdir: str,
    host: str = "127.0.0.1",
    port: int = 8765,
    max_retries: int = 10,
):
    server = WorkbenchServer(config_path, workdir)
    httpd = None
    last_error = None
    candidates = [port] + [0] * 1  # port 0 lets the OS assign a guaranteed-free port
    for candidate in candidates:
        try:
            httpd = ThreadingHTTPServer((host, candidate), build_handler(server))
            break
        except OSError as exc:
            last_error = exc
    if httpd is None:
        raise last_error
    httpd.actual_port = httpd.server_address[1]
    return httpd
