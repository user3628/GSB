"""Local HTTP interface for the voxel reconstruction engine (stdlib only).

Endpoints (JSON in / JSON out):
    GET  /api/health            - liveness
    POST /api/import            - {manifest_path} import manifest + shards
    POST /api/build             - {mode, stop_after?} run the staged build
    GET  /api/status            - stage states, version fingerprints, sparsity
    GET  /api/invalidations     - last invalidation report (reasons, fps)
    POST /api/stop              - request a cooperative stop between stages
    POST /api/recover           - recover from the checkpoint log
    POST /api/view/replace      - {view_id, image_path} replace one shard
    POST /api/config            - {updates: {key: value}} change config
    POST /api/verify            - incremental vs full byte-consistency check
    GET  /api/export/summary    - export + return the global voxel summary

EngineError responses use HTTP 409 with the structured error body.
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from voxel_engine.engine import Engine
from voxel_engine.errors import EngineError


def _make_handler(engine: Engine):
    class Handler(BaseHTTPRequestHandler):
        server_version = "VoxelEngineHTTP/1.0"

        def log_message(self, fmt, *args):  # keep test output clean
            pass

        def _send(self, obj, status=200):
            body = json.dumps(obj, sort_keys=True, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _body(self) -> dict:
            length = int(self.headers.get("Content-Length") or 0)
            if length == 0:
                return {}
            raw = self.rfile.read(length)
            try:
                return json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError:
                return {}

        def _dispatch(self, method: str):
            path = self.path.split("?", 1)[0].rstrip("/") or "/"
            try:
                if method == "GET" and path == "/api/health":
                    return self._send({"ok": True})
                if method == "POST" and path == "/api/import":
                    return self._send(engine.import_dataset(self._body()["manifest_path"]))
                if method == "POST" and path == "/api/build":
                    body = self._body()
                    return self._send(
                        engine.build(
                            mode=body.get("mode", "incremental"),
                            stop_after=body.get("stop_after"),
                            lock_timeout_ms=int(body.get("lock_timeout_ms", 5000)),
                        )
                    )
                if method == "GET" and path == "/api/status":
                    return self._send(engine.status())
                if method == "GET" and path == "/api/invalidations":
                    status = engine.status()
                    return self._send(
                        {
                            "invalidation": status["invalidation"],
                            "sparsity_stats": status["sparsity_stats"],
                        }
                    )
                if method == "POST" and path == "/api/stop":
                    return self._send(engine.stop())
                if method == "POST" and path == "/api/recover":
                    return self._send(engine.recover())
                if method == "POST" and path == "/api/view/replace":
                    body = self._body()
                    return self._send(engine.replace_view(body["view_id"], body["image_path"]))
                if method == "POST" and path == "/api/config":
                    return self._send(engine.update_config(self._body().get("updates", {})))
                if method == "POST" and path == "/api/verify":
                    return self._send(engine.verify_consistency())
                if method == "GET" and path == "/api/export/summary":
                    result = engine.export_summary()
                    with open(result["summary_path"], "rb") as fh:
                        summary = json.loads(fh.read().decode("utf-8"))
                    return self._send(summary)
                return self._send(
                    {"error": {"code": "E_NOT_FOUND", "message": f"no route {method} {path}",
                               "details": {}}},
                    status=404,
                )
            except EngineError as exc:
                return self._send(exc.to_dict(), status=409)
            except (KeyError, ValueError, OSError) as exc:
                return self._send(
                    {"error": {"code": "E_BAD_REQUEST", "message": str(exc), "details": {}}},
                    status=400,
                )

        def do_GET(self):
            self._dispatch("GET")

        def do_POST(self):
            self._dispatch("POST")

    return Handler


def create_server(workspace, host: str = "127.0.0.1", port: int = 8765) -> ThreadingHTTPServer:
    engine = Engine(workspace)
    server = ThreadingHTTPServer((host, port), _make_handler(engine))
    server.engine = engine  # convenient handle for tests
    return server


def serve(workspace, host: str = "127.0.0.1", port: int = 8765) -> None:
    server = create_server(workspace, host=host, port=port)
    print(f"voxel engine HTTP listening on http://{host}:{server.server_address[1]}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
