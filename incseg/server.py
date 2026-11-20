"""Local HTTP API for the incseg engine (stdlib only, loopback only)."""

from __future__ import annotations

import base64
import json
import shutil
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .config import TileConfig
from .engine import Engine
from .errors import EngineError, WorkspaceError
from .util import pretty_json

LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}


def _json_bytes(obj):
    return pretty_json(obj).encode("utf-8")


class _Handler(BaseHTTPRequestHandler):
    server_version = "incseg/0.1"

    def log_message(self, fmt, *args):
        pass

    def _send(self, status, body, content_type="application/json"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, status, obj):
        self._send(status, _json_bytes(obj))

    def _send_error(self, exc):
        self._send_json(exc.http_status, {"error": exc.to_dict()})

    def _read_json_body(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except ValueError as exc:
            raise WorkspaceError(f"invalid JSON body: {exc}")
        if not isinstance(data, dict):
            raise WorkspaceError("JSON body must be an object")
        return data

    def do_GET(self):
        try:
            self._route_get(urlparse(self.path))
        except EngineError as exc:
            self._send_error(exc)

    def do_POST(self):
        try:
            self._route_post(urlparse(self.path))
        except EngineError as exc:
            self._send_error(exc)

    def _route_get(self, url):
        path = url.path.rstrip("/") or "/"
        engine = self.server.engine
        if path == "/":
            self._send_json(200, {"service": "incseg", "endpoints": [
                "GET /health", "GET /tiles", "GET /tiles/{id}",
                "GET /tiles/{id}/export?kind=semantic|instance|summary",
                "GET /invalidations", "POST /run", "POST /stop", "POST /resume",
                "POST /tiles/import", "POST /tiles/{id}/rebuild", "POST /verify",
            ]})
        elif path == "/health":
            self._send_json(200, {"status": "ok"})
        elif path == "/tiles":
            config = engine.load_config()
            self._send_json(200, {"tiles": [t.tile_id for t in config.tiles]})
        elif path == "/invalidations":
            self._send_json(200, engine.inspect())
        elif path.startswith("/tiles/"):
            parts = path.split("/")
            tile_id = parts[2]
            if len(parts) == 3:
                config = engine.load_config()
                self._send_json(200, engine.inspect_tile(config.tile(tile_id)))
            elif len(parts) == 4 and parts[3] == "export":
                self._handle_export(engine, tile_id, parse_qs(url.query))
            else:
                self._send_json(404, {"error": {"code": "E_NOT_FOUND", "message": path}})
        else:
            self._send_json(404, {"error": {"code": "E_NOT_FOUND", "message": path}})

    def _handle_export(self, engine, tile_id, query):
        kind = (query.get("kind") or ["summary"])[0]
        names = {"semantic": "semantic.png", "instance": "instance.png", "summary": "summary.json"}
        if kind not in names:
            raise WorkspaceError(f"unknown export kind {kind!r}", {"kinds": sorted(names)})
        path = engine.artifacts_dir(tile_id, "quantize") / names[kind]
        if not path.exists():
            raise WorkspaceError(f"tile {tile_id}: {names[kind]} not built yet")
        content_type = "image/png" if names[kind].endswith(".png") else "application/json"
        self._send(200, path.read_bytes(), content_type)

    def _route_post(self, url):
        path = url.path.rstrip("/")
        engine = self.server.engine
        if path == "/run":
            body = self._read_json_body()
            self._send_json(200, engine.run(tile_ids=body.get("tiles")))
        elif path == "/stop":
            engine.request_stop()
            self._send_json(200, {"status": "ok", "stop_requested": True})
        elif path == "/resume":
            body = self._read_json_body()
            self._send_json(200, engine.run(tile_ids=body.get("tiles")))
        elif path == "/verify":
            body = self._read_json_body()
            self._send_json(200, engine.verify(tile_ids=body.get("tiles")))
        elif path == "/tiles/import":
            self._handle_import(engine, self._read_json_body())
        elif path.startswith("/tiles/") and path.endswith("/rebuild"):
            tile_id = path.split("/")[2]
            self._send_json(200, engine.run(tile_ids=[tile_id]))
        else:
            self._send_json(404, {"error": {"code": "E_NOT_FOUND", "message": path}})

    def _handle_import(self, engine, body):
        tile_id = body.get("tile_id")
        if not tile_id:
            raise WorkspaceError("tile_id is required")
        tiles_dir = engine.workspace / "tiles"
        tiles_dir.mkdir(parents=True, exist_ok=True)
        image_rel = f"tiles/{tile_id}.png"
        image_dest = engine.workspace / image_rel
        if "image_base64" in body:
            image_dest.write_bytes(base64.b64decode(body["image_base64"]))
        elif "image_path" in body:
            shutil.copyfile(body["image_path"], image_dest)
        else:
            raise WorkspaceError("image_base64 or image_path is required")
        config = engine.load_config()
        overrides = body.get("config") or {}
        if "like" in body:
            data = config.tile(body["like"]).to_dict()
        elif config.tiles:
            data = config.tiles[0].to_dict()
        else:
            raise WorkspaceError("no template tile available; provide full config")
        data.update(overrides)
        data["tile_id"] = tile_id
        data["image"] = image_rel
        config.replace_tile(TileConfig.from_dict(data))
        config.save(engine.config_path)
        self._send_json(200, {"status": "ok", "imported": tile_id})


def serve(workspace, host="127.0.0.1", port=8765):
    if host not in LOOPBACK_HOSTS:
        raise WorkspaceError(
            f"refusing to bind non-loopback address {host!r}; loopback only",
            {"allowed": sorted(LOOPBACK_HOSTS)},
        )
    handler = type("BoundHandler", (_Handler,), {})
    server = ThreadingHTTPServer((host, port), handler)
    server.engine = Engine(workspace)
    actual_host, actual_port = server.server_address[:2]
    print(f"incseg HTTP API listening on http://{actual_host}:{actual_port} (loopback only)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return server
