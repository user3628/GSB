"""Local-only HTTP workbench built on the standard library http.server.

No third-party web framework is used.  ThreadingHTTPServer serves one request
per thread; all mutating operations are serialized through the workspace file
locks inside the Workbench facade.
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, Tuple
from urllib.parse import urlparse

from .config import default_config
from .errors import WorkbenchError
from .workbench import Workbench


class WorkbenchHolder:
    """Shared, lazily-constructed workbench handle with a config lock."""

    def __init__(self, root: str):
        self.root = root
        self._wb: Workbench | None = None
        self._lock = threading.Lock()

    def get(self) -> Workbench:
        with self._lock:
            if self._wb is None:
                self._wb = Workbench(self.root, default_config())
            return self._wb


def _json_response(handler: BaseHTTPRequestHandler, status: int,
                   payload: Dict[str, Any]) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


class Handler(BaseHTTPRequestHandler):
    holder: WorkbenchHolder = None  # injected by make_server

    def log_message(self, fmt: str, *args: Any) -> None:  # quiet by default
        return

    def _read_json(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise WorkbenchError(f"invalid JSON body: {exc}")

    def do_GET(self) -> None:  # noqa: N802 - stdlib naming
        self._dispatch(read_only=True)

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch(read_only=False)

    def _dispatch(self, read_only: bool) -> None:
        route = urlparse(self.path).path.rstrip("/") or "/"
        try:
            body = {} if read_only else self._read_json()
            status, payload = ROUTES[route](self.holder.get(), body,
                                            urlparse(self.path).query)
            _json_response(self, status, payload)
        except KeyError:
            _json_response(self, 404,
                           {"code": "E_NOT_FOUND",
                            "message": f"unknown route {route}",
                            "details": {"route": route}})
        except WorkbenchError as exc:
            _json_response(self, exc.http_status, exc.to_dict())
        except Exception as exc:  # pragma: no cover - defensive boundary
            _json_response(self, 500,
                           {"code": "E_INTERNAL", "message": str(exc),
                            "details": {}})


def _ok(data: Any) -> Tuple[int, Dict[str, Any]]:
    return 200, {"ok": True, "data": data}


def _import(wb: Workbench, body: Dict[str, Any], _: str) -> Tuple[int, Dict[str, Any]]:
    return _ok(wb.import_workspace(body.get("manifest_path")))


def _run_stage(wb: Workbench, body: Dict[str, Any], _: str) -> Tuple[int, Dict[str, Any]]:
    return _ok(wb.run_stage(body["shard_id"], body["stage"]))


def _run_all(wb: Workbench, body: Dict[str, Any], _: str) -> Tuple[int, Dict[str, Any]]:
    return _ok(wb.run_all())


def _status(wb: Workbench, body: Dict[str, Any], _: str) -> Tuple[int, Dict[str, Any]]:
    return _ok({"stages": wb.stage_status(),
                "config_version": wb.cfg.fingerprint()})


def _timeline(wb: Workbench, body: Dict[str, Any], _: str) -> Tuple[int, Dict[str, Any]]:
    return _ok(wb.get_timeline(body["shard_id"]))


def _query(wb: Workbench, body: Dict[str, Any], _: str) -> Tuple[int, Dict[str, Any]]:
    return _ok(wb.query(body["shard_id"], body["kind"],
                        body.get("budget_ms")))


def _draft(wb: Workbench, body: Dict[str, Any], _: str) -> Tuple[int, Dict[str, Any]]:
    return _ok(wb.save_draft(body["shard_id"], body["operation"],
                             body["params"], body.get("origin", "http")))


def _discard(wb: Workbench, body: Dict[str, Any], _: str) -> Tuple[int, Dict[str, Any]]:
    return _ok(wb.discard_draft(body["shard_id"]))


def _publish(wb: Workbench, body: Dict[str, Any], _: str) -> Tuple[int, Dict[str, Any]]:
    if "operation" in body:
        return _ok(wb.publish_edit(body["shard_id"], body["operation"],
                                   body["params"], int(body["expected_version"]),
                                   body.get("origin", "http")))
    return _ok(wb.publish_draft(body["shard_id"],
                                int(body["expected_version"])))


def _unify(wb: Workbench, body: Dict[str, Any], _: str) -> Tuple[int, Dict[str, Any]]:
    return _ok(wb.unify_identity(body["mappings"],
                                 body.get("origin", "http")))


def _config(wb: Workbench, body: Dict[str, Any], _: str) -> Tuple[int, Dict[str, Any]]:
    changes = {k: v for k, v in body.items() if k != "validate_only"}
    return _ok(wb.update_config(**changes) if changes else
               {"config": wb.cfg.canonical(),
                "config_version": wb.cfg.fingerprint()})


def _stop(wb: Workbench, body: Dict[str, Any], _: str) -> Tuple[int, Dict[str, Any]]:
    return _ok(wb.request_stop())


def _recover(wb: Workbench, body: Dict[str, Any], _: str) -> Tuple[int, Dict[str, Any]]:
    return _ok(wb.recover())


def _consistency(wb: Workbench, body: Dict[str, Any], _: str) -> Tuple[int, Dict[str, Any]]:
    report = wb.verify_consistency()
    return _ok({"status": report["status"], "shards": report["shards"]})


def _export(wb: Workbench, body: Dict[str, Any], _: str) -> Tuple[int, Dict[str, Any]]:
    return _ok({"path": wb.export_results()})


ROUTES: Dict[str, Callable[..., Tuple[int, Dict[str, Any]]]] = {
    "/health": lambda wb, b, q: _ok({"status": "up"}),
    "/import": _import,
    "/run-stage": _run_stage,
    "/run-all": _run_all,
    "/status": _status,
    "/timeline": _timeline,
    "/query": _query,
    "/draft": _draft,
    "/draft/discard": _discard,
    "/publish": _publish,
    "/unify-identity": _unify,
    "/config": _config,
    "/stop": _stop,
    "/recover": _recover,
    "/consistency": _consistency,
    "/export": _export,
}


def make_server(root: str, host: str = "127.0.0.1",
                port: int = 8765) -> ThreadingHTTPServer:
    holder = WorkbenchHolder(root)
    handler = Handler
    handler.holder = holder
    server = ThreadingHTTPServer((host, port), handler)
    server.daemon_threads = True
    return server


def serve(root: str, host: str = "127.0.0.1", port: int = 8765) -> None:
    server = make_server(root, host, port)
    print(f"diarwb workbench listening on http://{host}:{port} (root={root})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
