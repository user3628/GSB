"""Local-only JSON HTTP API backed by the same engine as the CLI."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .canonical import atomic_write_bytes, read_json
from .engine import Engine
from .errors import BusinessError, E_INTERNAL, E_STALE_MAPPING


MAX_BODY = 4 * 1024 * 1024


class PosAlignHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address: tuple[str, int], workspace: Path) -> None:
        self.workspace = workspace
        super().__init__(server_address, PosAlignHandler)


class PosAlignHandler(BaseHTTPRequestHandler):
    server: PosAlignHTTPServer

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"posalign-http {self.address_string()} {fmt % args}")

    def _send(self, status: int, payload: dict[str, Any]) -> None:
        data = (json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _read_body(self) -> dict[str, Any]:
        length_header = self.headers.get("Content-Length")
        if length_header is None:
            return {}
        try:
            length = int(length_header)
        except ValueError as exc:
            raise BusinessError("E_INVALID_INPUT", "invalid Content-Length") from exc
        if length < 0 or length > MAX_BODY:
            raise BusinessError("E_INVALID_INPUT", "request body is too large")
        raw = self.rfile.read(length)
        try:
            value = json.loads(raw or b"{}")
        except json.JSONDecodeError as exc:
            raise BusinessError("E_INVALID_INPUT", "request body must be JSON") from exc
        if not isinstance(value, dict):
            raise BusinessError("E_INVALID_INPUT", "request JSON root must be an object")
        return value

    def _error(self, error: BusinessError) -> None:
        self._send(error.http_status, error.to_dict())

    def do_GET(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            engine = Engine(self.server.workspace)
            if parsed.path == "/health":
                self._send(200, {"ok": True, "service": "posalign", "workspace": str(self.server.workspace)})
            elif parsed.path in {"/status", "/state"}:
                self._send(200, engine.status())
            elif parsed.path == "/mapping/resolved":
                allow_stale = query.get("allow_stale", ["0"])[0] in {"1", "true", "yes"}
                self._send(200, engine.mapping_resolved(allow_stale=allow_stale))
            elif parsed.path == "/mapping/candidates":
                label = query.get("label", [None])[0]
                allow_stale = query.get("allow_stale", ["0"])[0] in {"1", "true", "yes"}
                self._send(
                    200,
                    engine.mapping_candidates(label, allow_stale=allow_stale),
                )
            elif parsed.path == "/migration":
                stale = read_json(engine.stale_output_path, default=None)
                if stale is not None:
                    raise BusinessError(
                        E_STALE_MAPPING,
                        "migration report is stale after an upstream invalidation",
                        {"stale": stale},
                    )
                data = read_json(engine.output_dir / "migration.json", default=None)
                if data is None:
                    raise BusinessError("E_INVALID_STAGE", "migration report is not available yet")
                self._send(200, data)
            elif parsed.path == "/summary":
                stale = read_json(engine.stale_output_path, default=None)
                if stale is not None:
                    raise BusinessError(
                        E_STALE_MAPPING,
                        "summary is stale after an upstream invalidation",
                        {"stale": stale},
                    )
                data = read_json(engine.output_dir / "summary.json", default=None)
                if data is None:
                    raise BusinessError("E_INVALID_STAGE", "summary is not available yet")
                self._send(200, data)
            else:
                self._send(404, {"ok": False, "error": {"code": "E_NOT_FOUND", "message": "unknown endpoint"}})
        except BusinessError as error:
            self._error(error)
        except Exception as error:
            self._error(BusinessError(E_INTERNAL, "internal server error", {"cause": str(error)}))

    def do_POST(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            body = self._read_body()
            engine = Engine(self.server.workspace)
            if parsed.path == "/run":
                shards = body.get("shards")
                config = body.get("config")
                if not isinstance(shards, list) or not all(isinstance(item, str) for item in shards):
                    raise BusinessError("E_INVALID_INPUT", "shards must be a list of paths")
                if not isinstance(config, str):
                    raise BusinessError("E_INVALID_INPUT", "config must be a path")
                result = engine.run(
                    shards,
                    config_path=config,
                    reset=bool(body.get("reset", False)),
                    stop_after_sentences=body.get("stop_after_sentences"),
                    clear_stop=bool(body.get("clear_stop", False)),
                )
                self._send(200, result)
            elif parsed.path == "/stop":
                target = engine.stop_path
                atomic_write_bytes(target, b"stop requested\n")
                self._send(200, {"ok": True, "status": "stop_requested", "stop_file": str(target)})
            elif parsed.path == "/clear-stop":
                engine.clear_stop()
                self._send(200, {"ok": True, "status": "stop_cleared"})
            elif parsed.path == "/export-v1":
                shards = body.get("shards")
                config = body.get("config")
                output = body.get("output")
                if not isinstance(shards, list) or not isinstance(config, str) or not isinstance(output, str):
                    raise BusinessError("E_INVALID_INPUT", "shards, config, and output are required")
                self._send(200, engine.export_v1(shards, config_path=config, output_path=output))
            else:
                self._send(404, {"ok": False, "error": {"code": "E_NOT_FOUND", "message": "unknown endpoint"}})
        except BusinessError as error:
            self._error(error)
        except Exception as error:
            self._error(BusinessError(E_INTERNAL, "internal server error", {"cause": str(error)}))


def start_server_thread(
    workspace: str | Path,
    *,
    host: str = "127.0.0.1",
    port: int = 0,
) -> tuple[PosAlignHTTPServer, threading.Thread]:
    server = PosAlignHTTPServer((host, port), Path(workspace).expanduser().resolve())
    thread = threading.Thread(target=server.serve_forever, name="posalign-http", daemon=True)
    thread.start()
    return server, thread


def run_server(workspace: str | Path, *, host: str = "127.0.0.1", port: int = 8765) -> None:
    server, thread = start_server_thread(workspace, host=host, port=port)
    print(f"posalign HTTP API listening on http://{host}:{server.server_port}")
    try:
        thread.join()
    except KeyboardInterrupt:
        server.shutdown()
        server.server_close()

