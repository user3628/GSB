"""Local HTTP interface (stdlib only) on 127.0.0.1.

Endpoints (JSON in/out):
  GET  /health            liveness
  GET  /status            journal head, config versions, stage fingerprints
  GET  /config            current config store
  PUT  /config            replace config (bumps version)
  PATCH /config           partial update: {"set": {"a.b": value, ...}}
  POST /sample            import sample: {"path": "..."}
  POST /run               run pipeline: {"budget_ms": ..., "stage_sleep_ms": ...}
  POST /stop              request graceful stop
  POST /resume            resume from last consistent record
  POST /verify            full-rebuild consistency check
  GET  /stages/<name>     stage artifact
  GET  /selection         kept/dropped feature lists
"""

from __future__ import annotations

import json
import re
import signal
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .config import bump_config, validate_config
from .engine import Engine
from .errors import ConfigError, WorkbenchError
from .cli import _cmd_import_sample, _set_dotted


class _Args:
    def __init__(self, **kw):
        self.__dict__.update(kw)


def _make_handler(state_dir):
    class Handler(BaseHTTPRequestHandler):
        server_version = "workbench-http/0.1"

        def log_message(self, fmt, *args):
            pass

        def _send(self, obj, status=200):
            body = json.dumps(obj, indent=2, ensure_ascii=False, sort_keys=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _body(self):
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0:
                return {}
            raw = self.rfile.read(length)
            try:
                return json.loads(raw.decode("utf-8"))
            except ValueError:
                raise ConfigError("request body is not valid JSON")

        def _dispatch(self, method):
            try:
                result = self._route(method)
            except WorkbenchError as exc:
                self._send({"ok": False, "error": exc.to_dict()}, status=400)
                return
            except Exception as exc:  # unexpected
                self._send(
                    {"ok": False,
                     "error": {"code": "E_INTERNAL", "message": str(exc), "details": {}}},
                    status=500,
                )
                return
            self._send(result if result is not None else {"ok": True})

        def _route(self, method):
            engine = Engine(state_dir)
            path = self.path.split("?", 1)[0]
            if method == "GET" and path == "/health":
                return {"ok": True}
            if method == "GET" and path == "/status":
                return engine.status()
            if method == "GET" and path == "/config":
                from .store import Store

                obj = Store(state_dir).load_config_store()
                if obj is None:
                    raise ConfigError("config not initialized")
                return obj
            if method in ("PUT", "PATCH") and path == "/config":
                return self._update_config(method)
            if method == "POST" and path == "/sample":
                body = self._body()
                return _cmd_import_sample(_Args(path=body.get("path", ""), state=state_dir))
            if method == "POST" and path == "/run":
                body = self._body()
                return engine.run(
                    budget_ms=body.get("budget_ms"),
                    stage_sleep_ms=body.get("stage_sleep_ms", 0),
                )
            if method == "POST" and path == "/resume":
                return engine.run()
            if method == "POST" and path == "/stop":
                engine.store.ensure()
                engine.store.request_stop()
                return {"stop_requested": True}
            if method == "POST" and path == "/verify":
                return engine.verify()
            if method == "GET" and path == "/selection":
                return engine.selection()
            m = re.fullmatch(r"/stages/([a-z_]+)", path)
            if method == "GET" and m:
                return engine.stage_artifact(m.group(1))
            raise ConfigError("unknown route", {"method": method, "path": path})

        def _update_config(self, method):
            from .store import Store

            store = Store(state_dir)
            obj = store.load_config_store()
            if obj is None:
                raise ConfigError("config not initialized")
            body = self._body()
            if method == "PUT":
                new_cfg = body.get("config", body)
            else:
                import copy

                new_cfg = copy.deepcopy(obj["config"])
                for key, value in (body.get("set") or {}).items():
                    _set_dotted(new_cfg, key, value)
            validate_config(new_cfg)
            new_obj = bump_config(obj, new_cfg)
            store.write_config_store(new_obj)
            return {
                "config_version": new_obj["version"],
                "section_versions": new_obj["section_versions"],
            }

        do_GET = lambda self: self._dispatch("GET")
        do_POST = lambda self: self._dispatch("POST")
        do_PUT = lambda self: self._dispatch("PUT")
        do_PATCH = lambda self: self._dispatch("PATCH")

    return Handler


def serve(state_dir, host, port):
    server = ThreadingHTTPServer((host, port), _make_handler(state_dir))

    def _shutdown(signum, frame):
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)
    try:
        server.serve_forever()
    finally:
        server.server_close()
    return {"server": "stopped", "host": host, "port": port}
