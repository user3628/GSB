"""Small standard-library HTTP service exposing local index status/queries."""

from __future__ import annotations

import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .config import IndexConfig
from .errors import PipelineError, as_error_payload
from .pipeline import Pipeline
from .query import QueryEngine
from .utils import canonical_json_bytes, load_json


class LocalIndexService:
    def __init__(self, config: IndexConfig, workspace: Path):
        self.config = config
        self.workspace = workspace
        self.pipeline = Pipeline(config, workspace)
        self.engine = QueryEngine(config, self.pipeline.artifacts_dir)

    def summary(self) -> dict[str, Any]:
        payload = load_json(self.pipeline.stage_dir(4) / "query_results.json")
        if not isinstance(payload, dict):
            raise PipelineError("E_INVALID_STAGE", "缺少阶段 4 查询摘要")
        return {
            "ok": True,
            "index_version": self.config.index_version,
            "summary": payload.get("summary"),
            "queries": payload.get("queries"),
        }

    def query(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.engine.execute(
            query_id=str(payload.get("query_id", "http-query")),
            terms=payload.get("terms"),
            mode=str(payload.get("mode", "and")),
            limit=payload.get("limit", int(self.config.value("query.default_limit", 20))),
            use_cache=bool(payload.get("use_cache", True)),
        )


def make_handler(service: LocalIndexService) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "SparseIndexHTTP/1.0"

        def log_message(self, format: str, *args: Any) -> None:
            # Keep the local service quiet; the demo emits its own stable JSON.
            return

        def _send(self, status: int, payload: Any) -> None:
            body = canonical_json_bytes(payload)
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _path(self) -> str:
            return urlparse(self.path).path

        def do_GET(self) -> None:  # noqa: N802
            path = self._path()
            try:
                if path == "/health":
                    self._send(
                        HTTPStatus.OK,
                        {
                            "ok": True,
                            "status": "healthy",
                            "index_version": service.config.index_version,
                            "config_fingerprint": service.config.config_fingerprint,
                        },
                    )
                elif path == "/status":
                    self._send(HTTPStatus.OK, service.pipeline.status())
                elif path == "/summary":
                    self._send(HTTPStatus.OK, service.summary())
                elif path == "/":
                    self._send(
                        HTTPStatus.OK,
                        {
                            "ok": True,
                            "routes": ["GET /health", "GET /status", "GET /summary", "POST /query"],
                        },
                    )
                else:
                    self._send(HTTPStatus.NOT_FOUND, {"ok": False, "error": {"code": "E_NOT_FOUND"}})
            except Exception as exc:  # still return a structured business response
                self._send(HTTPStatus.INTERNAL_SERVER_ERROR, as_error_payload(exc))

        def do_POST(self) -> None:  # noqa: N802
            if self._path() != "/query":
                self._send(HTTPStatus.NOT_FOUND, {"ok": False, "error": {"code": "E_NOT_FOUND"}})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length <= 0 or length > 1024 * 1024:
                    raise PipelineError(
                        "E_QUERY_OUT_OF_BOUNDS",
                        "请求体大小越界",
                        details={"content_length": length},
                    )
                raw = self.rfile.read(length)
                payload = json.loads(raw.decode("utf-8"))
                if not isinstance(payload, dict):
                    raise PipelineError("E_QUERY_OUT_OF_BOUNDS", "请求体必须是 JSON 对象")
                result = service.query(payload)
                self._send(HTTPStatus.OK, {"ok": True, **result})
            except PipelineError as exc:
                status = HTTPStatus.SERVICE_UNAVAILABLE if exc.code == "E_LATENCY_BUDGET" else HTTPStatus.BAD_REQUEST
                self._send(status, exc.to_dict())
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                self._send(
                    HTTPStatus.BAD_REQUEST,
                    {
                        "ok": False,
                        "error": {
                            "code": "E_QUERY_OUT_OF_BOUNDS",
                            "message": "请求体不是合法 JSON",
                            "details": {"reason": str(exc)},
                        },
                    },
                )
            except Exception as exc:
                self._send(HTTPStatus.INTERNAL_SERVER_ERROR, as_error_payload(exc))

    return Handler


def serve(config: IndexConfig, workspace: Path, host: str, port: int) -> None:
    service = LocalIndexService(config, workspace)
    server = ThreadingHTTPServer((host, port), make_handler(service))
    print(
        json.dumps(
            {
                "ok": True,
                "status": "listening",
                "host": host,
                "port": server.server_address[1],
                "config_fingerprint": config.config_fingerprint,
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        flush=True,
    )
    try:
        server.serve_forever()
    finally:
        server.server_close()
