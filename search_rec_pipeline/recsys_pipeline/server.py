"""Standard-library local HTTP test service sharing the CLI pipeline manager."""

from __future__ import annotations

import argparse
import json
import sys
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .errors import (
    E_INVALID_REQUEST,
    E_NOT_FOUND,
    PipelineError,
    http_status_for,
)
from .pipeline import PipelineManager


MAX_BODY_BYTES = 1024 * 1024


class PipelineHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple[str, int], manager: PipelineManager) -> None:
        super().__init__(address, PipelineRequestHandler)
        self.manager = manager


class PipelineRequestHandler(BaseHTTPRequestHandler):
    server: PipelineHTTPServer

    def log_message(self, format_string: str, *args: Any) -> None:
        sys.stderr.write(
            "%s - %s\n" % (self.address_string(), format_string % args)
        )

    def _send_json(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_bytes(self, status: int, body: bytes, content_type: str, filename: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.end_headers()
        self.wfile.write(body)

    def _read_json_body(self) -> dict[str, Any]:
        length_header = self.headers.get("Content-Length", "0")
        try:
            length = int(length_header)
        except ValueError as exc:
            raise PipelineError(E_INVALID_REQUEST, "Content-Length is invalid") from exc
        if length > MAX_BODY_BYTES:
            raise PipelineError(E_INVALID_REQUEST, "request body is too large", max_bytes=MAX_BODY_BYTES)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PipelineError(E_INVALID_REQUEST, "request body must be UTF-8 JSON") from exc
        if not isinstance(value, dict):
            raise PipelineError(E_INVALID_REQUEST, "request body must be a JSON object")
        return value

    @staticmethod
    def _query_value(query: dict[str, list[str]], key: str, default: str | None = None) -> str | None:
        values = query.get(key)
        return values[0] if values else default

    def do_GET(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            manager = self.server.manager
            if parsed.path == "/health":
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "data": {
                            "status": "healthy",
                            "config_version": manager.config.config_version,
                            "workdir": str(manager.workdir),
                            "scales": ["small", "medium", "large"],
                        },
                    },
                )
                return
            if parsed.path == "/":
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "data": {
                            "endpoints": [
                                "GET /health",
                                "POST /import",
                                "POST /run",
                                "POST /resume",
                                "POST /stop",
                                "GET /status?scale=",
                                "GET /fingerprints?scale=",
                                "GET /decision?scale=",
                                "GET /encoding?scale=",
                                "GET /budget?scale=",
                                "GET /export?scale=&kind=",
                            ]
                        },
                    },
                )
                return
            scale = self._query_value(query, "scale")
            if parsed.path in {
                "/status",
                "/fingerprints",
                "/decision",
                "/encoding",
                "/budget",
            }:
                if not scale:
                    raise PipelineError(E_INVALID_REQUEST, "scale query parameter is required")
                if parsed.path == "/status":
                    data = (
                        {name: manager.status(name) for name in ("small", "medium", "large")}
                        if scale == "all"
                        else manager.status(scale)
                    )
                elif parsed.path == "/fingerprints":
                    data = (
                        {name: manager.get_fingerprints(name) for name in ("small", "medium", "large")}
                        if scale == "all"
                        else manager.get_fingerprints(scale)
                    )
                elif parsed.path == "/decision":
                    data = manager.get_decision(scale)
                elif parsed.path == "/encoding":
                    data = manager.get_encoding_summary(scale)
                else:
                    data = manager.get_budget(scale)
                self._send_json(HTTPStatus.OK, {"ok": True, "data": data})
                return
            if parsed.path == "/export":
                if not scale:
                    raise PipelineError(E_INVALID_REQUEST, "scale query parameter is required")
                kind = self._query_value(query, "kind", "all")
                assert kind is not None
                info = manager.export(scale, kind)
                if kind == "all":
                    self._send_json(HTTPStatus.OK, {"ok": True, "data": info})
                    return
                path = Path(info["path"])
                self._send_bytes(
                    HTTPStatus.OK,
                    path.read_bytes(),
                    str(info.get("content_type", "application/octet-stream")),
                    path.name,
                )
                return
            raise PipelineError(E_NOT_FOUND, "unknown endpoint", path=parsed.path)
        except PipelineError as exc:
            self._send_json(http_status_for(exc.code), {"ok": False, "error": exc.to_dict()})
        except Exception as exc:  # pragma: no cover - defensive HTTP boundary
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {
                    "ok": False,
                    "error": {
                        "code": "E_INTERNAL",
                        "message": str(exc),
                        "details": {"exception_type": type(exc).__name__},
                    },
                },
            )

    def do_POST(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            manager = self.server.manager
            body = self._read_json_body()
            if parsed.path == "/import":
                data = manager.import_samples()
            elif parsed.path in {"/run", "/resume"}:
                scale = body.get("scale", "all")
                stop_after_stage = body.get("stop_after_stage")
                if not isinstance(scale, str):
                    raise PipelineError(E_INVALID_REQUEST, "scale must be a string")
                if stop_after_stage is not None and not isinstance(stop_after_stage, str):
                    raise PipelineError(E_INVALID_REQUEST, "stop_after_stage must be a string or null")
                if parsed.path == "/resume":
                    data = (
                        manager.run_all(stop_after_stage=stop_after_stage)
                        if scale == "all"
                        else manager.resume(scale, stop_after_stage=stop_after_stage)
                    )
                else:
                    data = (
                        manager.run_all(stop_after_stage=stop_after_stage)
                        if scale == "all"
                        else manager.run(scale, stop_after_stage=stop_after_stage)
                    )
            elif parsed.path == "/stop":
                scope = body.get("scope", body.get("scale", "all"))
                if not isinstance(scope, str):
                    raise PipelineError(E_INVALID_REQUEST, "stop scope must be a string")
                data = manager.request_stop(scope)
            else:
                raise PipelineError(E_NOT_FOUND, "unknown endpoint", path=parsed.path)
            self._send_json(HTTPStatus.OK, {"ok": True, "data": data})
        except PipelineError as exc:
            self._send_json(http_status_for(exc.code), {"ok": False, "error": exc.to_dict()})
        except Exception as exc:  # pragma: no cover - defensive HTTP boundary
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {
                    "ok": False,
                    "error": {
                        "code": "E_INTERNAL",
                        "message": str(exc),
                        "details": {"exception_type": type(exc).__name__},
                    },
                },
            )


def create_server(
    project_root: Path,
    *,
    config_path: Path | None = None,
    host: str = "127.0.0.1",
    port: int = 0,
    workdir_override: Path | None = None,
) -> PipelineHTTPServer:
    manager = PipelineManager(
        Path(project_root),
        config_path=config_path,
        workdir_override=workdir_override,
    )
    return PipelineHTTPServer((host, port), manager)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default=str(Path.cwd()))
    parser.add_argument("--config", default=None)
    parser.add_argument("--workdir", default=None)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    server = create_server(
        Path(args.project),
        config_path=Path(args.config) if args.config else None,
        host=args.host,
        port=args.port,
        workdir_override=Path(args.workdir) if args.workdir else None,
    )
    host, port = server.server_address[:2]
    print(
        json.dumps(
            {
                "ok": True,
                "data": {
                    "service": "local-search-rec-pipeline",
                    "url": f"http://{host}:{port}",
                    "config_version": server.manager.config.config_version,
                    "workdir": str(server.manager.workdir),
                },
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
