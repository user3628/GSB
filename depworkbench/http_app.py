"""Local-only HTTP interface built on the Python standard library."""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .errors import WorkbenchError
from .workspace import WorkbenchService, Workspace

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765


class WorkbenchHTTPHandler(BaseHTTPRequestHandler):
    server_version = "DepWorkbench/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:
        self.server.log.append(f"{self.address_string()} {fmt % args}")  # type: ignore[attr-defined]

    @property
    def service(self) -> WorkbenchService:
        return self.server.service  # type: ignore[attr-defined]

    def _send(self, payload: Any, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        try:
            if parsed.path == "/health":
                self._send({"status": "ok"})
            elif parsed.path == "/status":
                self._send(self.service.status())
            elif parsed.path == "/plan":
                self._send(self.service.plan())
            elif parsed.path == "/stats":
                up_to = query.get("up_to_shard", [None])[0]
                self._send(self.service.stats(
                    int(up_to) if up_to is not None else None))
            elif parsed.path == "/export":
                output = query.get("output", ["export.json"])[0]
                path = self.service.export(output)
                self._send({"exported": str(path)})
            else:
                self._send({"error": {"code": "E_NOT_FOUND",
                                      "message": parsed.path, "details": {}}}, 404)
        except WorkbenchError as exc:
            self._send({"error": exc.to_dict()}, 400)
        except json.JSONDecodeError as exc:
            self._send({"error": {"code": "E_INVALID_CONFIG",
                                  "message": f"bad JSON: {exc}", "details": {}}},
                       400)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/import":
                body = self._read_json()
                self._send(self.service.import_bundle(
                    body["shards_dir"], body["manifest"], body["config"]))
            elif parsed.path == "/run":
                body = self._read_json()
                self._send(self.service.run(
                    sequential=bool(body.get("sequential", False))).to_dict())
            elif parsed.path == "/resume":
                body = self._read_json()
                self._send(self.service.resume(
                    sequential=bool(body.get("sequential", False))).to_dict())
            elif parsed.path == "/stop":
                self._send(self.service.stop())
            elif parsed.path == "/reset":
                self.service.reset()
                self._send({"reset": True})
            else:
                self._send({"error": {"code": "E_NOT_FOUND",
                                      "message": parsed.path, "details": {}}}, 404)
        except WorkbenchError as exc:
            self._send({"error": exc.to_dict()}, 400)
        except (KeyError, json.JSONDecodeError) as exc:
            self._send({"error": {"code": "E_INVALID_CONFIG",
                                  "message": f"bad request: {exc}", "details": {}}},
                       400)


def build_server(host: str, port: int, workspace: Path,
                 checkpoint_dir: Path | None = None) -> ThreadingHTTPServer:
    ws = Workspace.create(workspace, checkpoint_root=checkpoint_dir)
    server = ThreadingHTTPServer((host, port), WorkbenchHTTPHandler)
    server.service = WorkbenchService(ws)  # type: ignore[attr-defined]
    server.log = []  # type: ignore[attr-defined]
    server.thread_id = threading.get_ident()  # type: ignore[attr-defined]
    return server


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Run the workbench HTTP API")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--workspace", default="workspaces/http_default")
    parser.add_argument("--checkpoint-dir", default=None)
    args = parser.parse_args(argv)
    server = build_server(args.host, args.port, Path(args.workspace),
                          Path(args.checkpoint_dir) if args.checkpoint_dir
                          else None)
    print(f"depworkbench HTTP API listening on http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
