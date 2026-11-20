"""Local standard-library HTTP application."""

from __future__ import annotations

import json
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

from .engine import Workbench, request_stop
from .errors import InvalidRequestError, NotFoundError, WorkbenchError
from .ops import ExecOptions


MAX_BODY = 16 * 1024 * 1024


class WorkbenchServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple[str, int], workbench: Workbench) -> None:
        super().__init__(address, WorkbenchRequestHandler)
        self.workbench = workbench
        self.run_lock = threading.Lock()


class WorkbenchRequestHandler(BaseHTTPRequestHandler):
    server: WorkbenchServer
    server_version = "ConvWorkbench/0.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[http] {self.address_string()} {fmt % args}", flush=True)

    def _send_json(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_bytes(self, status: int, body: bytes, content_type: str, filename: str | None = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        if filename:
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise InvalidRequestError("invalid Content-Length") from exc
        if length < 0 or length > MAX_BODY:
            raise InvalidRequestError("request body is too large", limit=MAX_BODY)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise InvalidRequestError("request body must be valid JSON") from exc
        if not isinstance(value, dict):
            raise InvalidRequestError("request body must be a JSON object")
        return value

    def _error(self, exc: WorkbenchError) -> None:
        self._send_json(exc.http_status, exc.to_dict())

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlsplit(self.path)
        path = unquote(parsed.path)
        query = parse_qs(parsed.query)
        try:
            if path in {"/", "/index.html"}:
                static = Path(__file__).with_name("static") / "index.html"
                self._send_bytes(HTTPStatus.OK, static.read_bytes(), "text/html; charset=utf-8")
                return
            if path == "/favicon.ico":
                self.send_response(HTTPStatus.NO_CONTENT)
                self.end_headers()
                return
            if path == "/api/health":
                workbench = self.server.workbench
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "status": "healthy",
                        "workspace": str(workbench.workspace.root),
                        "run_dir": str(workbench.run_dir),
                        "port": self.server.server_address[1],
                    },
                )
                return
            if path == "/api/shards":
                self._send_json(
                    HTTPStatus.OK,
                    {"ok": True, "shards": self.server.workbench.workspace.list_shards()},
                )
                return
            if path == "/api/status":
                shard = query.get("shard", [None])[0]
                self._send_json(HTTPStatus.OK, self.server.workbench.status(shard))
                return
            if path == "/api/fingerprints":
                shard = query.get("shard", [None])[0]
                if not shard:
                    raise InvalidRequestError("query parameter 'shard' is required")
                self._send_json(HTTPStatus.OK, self.server.workbench.fingerprints(shard))
                return
            if path == "/api/trace":
                trace = self.server.workbench.run_dir / "outputs" / "trace.json"
                if not trace.is_file():
                    raise NotFoundError("trace has not been generated yet", path=str(trace))
                self._send_bytes(HTTPStatus.OK, trace.read_bytes(), "application/json; charset=utf-8")
                return
            if path.startswith("/api/outputs/"):
                shard = path[len("/api/outputs/") :]
                if not shard or "/" in shard or "\\" in shard:
                    raise InvalidRequestError("invalid shard id")
                output = self.server.workbench.run_dir / "outputs" / f"{shard}.npy"
                if not output.is_file():
                    raise NotFoundError("exported output is not available", shard_id=shard)
                self._send_bytes(
                    HTTPStatus.OK,
                    output.read_bytes(),
                    "application/octet-stream",
                    filename=f"{shard}.npy",
                )
                return
            raise NotFoundError("endpoint not found", path=path)
        except WorkbenchError as exc:
            self._error(exc)
        except Exception as exc:  # Keep the local API deterministic and inspectable.
            self._send_json(
                500,
                {"ok": False, "error": {"code": "E_INTERNAL", "message": str(exc), "detail": {}}},
            )

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlsplit(self.path)
        path = parsed.path
        try:
            payload = self._read_json()
            workbench = self.server.workbench
            if path == "/api/import":
                result = workbench.workspace.import_shard(payload)
                self._send_json(HTTPStatus.CREATED, {"ok": True, **result})
                return
            if path == "/api/stop":
                request = request_stop(workbench.run_dir, str(payload.get("reason", "HTTP stop request")))
                self._send_json(
                    HTTPStatus.ACCEPTED,
                    {
                        "ok": True,
                        "status": "stop_requested",
                        "request": request,
                        "note": "the worker exits after completing the current indivisible stage",
                    },
                )
                return
            if path in {"/api/run", "/api/resume"}:
                if not self.server.run_lock.acquire(blocking=False):
                    raise InvalidRequestError("another run is active in this server process")
                try:
                    shard_ids = payload.get("shard_ids")
                    if shard_ids is not None and not isinstance(shard_ids, list):
                        raise InvalidRequestError("shard_ids must be an array")
                    stop_after = payload.get("stop_after")
                    if stop_after is not None and (isinstance(stop_after, bool) or not isinstance(stop_after, int)):
                        raise InvalidRequestError("stop_after must be an integer")
                    delay = payload.get("delay_between_stages", 0.0)
                    if isinstance(delay, bool) or not isinstance(delay, (int, float)):
                        raise InvalidRequestError("delay_between_stages must be numeric")
                    result = workbench.run(
                        shard_ids=shard_ids,
                        stop_after=stop_after,
                        delay_between_stages=float(delay),
                    )
                    self._send_json(HTTPStatus.OK, result)
                finally:
                    self.server.run_lock.release()
                return
            if path == "/api/stage":
                shard_id = payload.get("shard_id")
                stage_index = payload.get("stage_index")
                if not isinstance(shard_id, str) or not shard_id:
                    raise InvalidRequestError("shard_id is required")
                if isinstance(stage_index, bool) or not isinstance(stage_index, int):
                    raise InvalidRequestError("stage_index must be an integer")
                self._send_json(HTTPStatus.OK, workbench.execute_stage(shard_id, stage_index))
                return
            if path == "/api/export":
                shard_ids = payload.get("shard_ids")
                if shard_ids is not None and not isinstance(shard_ids, list):
                    raise InvalidRequestError("shard_ids must be an array")
                self._send_json(HTTPStatus.OK, workbench.export(shard_ids=shard_ids, write_trace=True))
                return
            raise NotFoundError("endpoint not found", path=path)
        except WorkbenchError as exc:
            self._error(exc)
        except Exception as exc:
            self._send_json(
                500,
                {"ok": False, "error": {"code": "E_INTERNAL", "message": str(exc), "detail": {}}},
            )


def create_server(
    workspace: str | Path,
    run_dir: str | Path,
    *,
    host: str = "127.0.0.1",
    port: int = 8766,
    exec_options: ExecOptions | None = None,
) -> WorkbenchServer:
    workbench = Workbench(workspace, run_dir, exec_options=exec_options)
    return WorkbenchServer((host, port), workbench)


def serve(
    workspace: str | Path,
    run_dir: str | Path,
    *,
    host: str = "127.0.0.1",
    port: int = 8766,
    exec_options: ExecOptions | None = None,
) -> None:
    server = create_server(workspace, run_dir, host=host, port=port, exec_options=exec_options)
    actual_host, actual_port = server.server_address[:2]
    print(f"conv-workbench listening on http://{actual_host}:{actual_port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()

