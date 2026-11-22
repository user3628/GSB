from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from .canonical import PipelineError, canonical_bytes
from .pipeline import Pipeline
from .query import QueryService


_ERROR_STATUS = {
    "E_INVALID_CONFIG": 400,
    "E_INVALID_RECORD": 400,
    "E_INVALID_JSONL": 400,
    "E_EMPTY_INPUT": 400,
    "E_DUPLICATE_SAMPLE_ID": 400,
    "E_SHARD_NOT_FOUND": 404,
    "E_SAMPLE_NOT_FOUND": 404,
    "E_NOT_FOUND": 404,
    "E_NOT_INITIALIZED": 409,
    "E_STAGE_NOT_READY": 409,
    "E_STOPPED": 409,
    "E_RUN_BUSY": 409,
    "E_STALE_CHECKPOINT": 409,
    "E_LATENCY_BUDGET": 408,
}


def _status_for(code: str) -> int:
    return _ERROR_STATUS.get(code, 500)


class PipelineRequestHandler(BaseHTTPRequestHandler):
    server_version = "HaluPipelineHTTP/1.0"
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        if getattr(self.server, "verbose", False):
            super().log_message(format, *args)

    @property
    def run_dir(self) -> Path:
        return self.server.run_dir  # type: ignore[attr-defined]

    def _send(self, status: int, payload: dict[str, Any]) -> None:
        data = canonical_bytes(payload)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            self.wfile.write(data)
        except BrokenPipeError:
            pass

    def _error(self, exc: PipelineError) -> None:
        self._send(_status_for(exc.code), exc.to_dict())

    def _body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length > 2_000_000:
            raise PipelineError("E_INVALID_REQUEST", "请求体过大。")
        if length == 0:
            return {}
        try:
            value = json.loads(self.rfile.read(length).decode("utf-8"))
        except Exception as exc:
            raise PipelineError(
                "E_INVALID_REQUEST",
                "请求体必须是合法 JSON。",
                {"error": f"{type(exc).__name__}: {exc}"},
            ) from exc
        if not isinstance(value, dict):
            raise PipelineError("E_INVALID_REQUEST", "请求体必须是 JSON 对象。")
        return value

    def _guard(self, function: Any) -> None:
        try:
            function()
        except PipelineError as exc:
            self._error(exc)
        except Exception as exc:  # pragma: no cover - defensive boundary
            self._send(
                500,
                PipelineError(
                    "E_INTERNAL", "服务内部错误。", {"error": f"{type(exc).__name__}: {exc}"}
                ).to_dict(),
            )

    def do_GET(self) -> None:  # noqa: N802
        self._guard(self._do_get)

    def _do_get(self) -> None:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        pipeline = Pipeline(self.run_dir)
        service = QueryService(self.run_dir)

        if parsed.path == "/health":
            self._send(200, {"code": "OK", "status": "healthy"})
            return
        if parsed.path == "/status":
            self._send(200, pipeline.status())
            return
        if parsed.path == "/config":
            config = pipeline.load_config()
            self._send(200, {"code": "OK", "config": config.to_dict(), "config_version": config.version})
            return
        if parsed.path == "/summary":
            result = service.query_summary(
                source=(query.get("source") or [None])[0],
                lang=(query.get("lang") or [None])[0],
                budget_ms=float(query["budget_ms"][0]) if query.get("budget_ms") else None,
                query_id=(query.get("query_id") or [None])[0],
                use_cache=(query.get("no_cache") or ["0"])[0] not in {"1", "true"},
            )
            self._send(_status_for(result["code"]) if result["code"] != "OK" else 200, result)
            return
        if parsed.path.startswith("/sample/"):
            sample_id = unquote(parsed.path[len("/sample/") :])
            result = service.query_sample(
                sample_id,
                budget_ms=float(query["budget_ms"][0]) if query.get("budget_ms") else None,
                query_id=(query.get("query_id") or [None])[0],
                use_cache=(query.get("no_cache") or ["0"])[0] not in {"1", "true"},
            )
            self._send(_status_for(result["code"]) if result["code"] != "OK" else 200, result)
            return
        if parsed.path == "/validation":
            state = pipeline.load_state()
            if not pipeline._artifact_is_valid(4, state):
                raise PipelineError("E_STAGE_NOT_READY", "阶段 4 验证产物尚未完成。")
            self._send(200, pipeline.load_stage_payload(4))
            return
        if parsed.path == "/report":
            self._send(200, pipeline.export_report())
            return
        if parsed.path == "/checkpoints":
            self._send(
                200,
                {
                    "code": "OK",
                    "last_sequence": pipeline.checkpoints.last_sequence(),
                    "checkpoints": pipeline.checkpoints.list(),
                },
            )
            return
        if parsed.path == "/cache":
            self._send(200, service.cache_stats())
            return
        self._send(404, {"code": "E_NOT_FOUND", "message": "接口不存在。", "details": {"path": parsed.path}})

    def do_POST(self) -> None:  # noqa: N802
        self._guard(self._do_post)

    def _do_post(self) -> None:
        parsed = urlparse(self.path)
        pipeline = Pipeline(self.run_dir)
        service = QueryService(self.run_dir)
        body = self._body()

        if parsed.path == "/import":
            shards = body.get("shards")
            if not isinstance(shards, list) or not shards:
                raise PipelineError("E_INVALID_REQUEST", "shards 必须是非空路径数组。")
            overrides = body.get("config_overrides") or None
            self._send(200, pipeline.import_shards([Path(item) for item in shards], overrides))
            return
        if parsed.path == "/config":
            assignments = body.get("assignments")
            updates = body.get("updates")
            if assignments is not None and not isinstance(assignments, list):
                raise PipelineError("E_INVALID_REQUEST", "assignments 必须是字符串数组。")
            self._send(200, pipeline.update_config(assignments=assignments, updates=updates))
            return
        if parsed.path == "/run":
            self._send(
                200,
                pipeline.run(
                    verify_incremental=bool(body.get("verify_incremental", True)),
                    until_stage=int(body.get("until_stage", 4)),
                ),
            )
            return
        if parsed.path == "/stop":
            self._send(200, pipeline.request_stop())
            return
        if parsed.path == "/resume":
            self._send(200, pipeline.resume())
            return
        if parsed.path == "/query/sample":
            result = service.query_sample(
                str(body.get("sample_id", "")),
                budget_ms=body.get("budget_ms"),
                query_id=body.get("query_id"),
                use_cache=bool(body.get("use_cache", True)),
            )
            self._send(_status_for(result["code"]) if result["code"] != "OK" else 200, result)
            return
        if parsed.path == "/query/summary":
            result = service.query_summary(
                source=body.get("source"),
                lang=body.get("lang"),
                budget_ms=body.get("budget_ms"),
                query_id=body.get("query_id"),
                use_cache=bool(body.get("use_cache", True)),
            )
            self._send(_status_for(result["code"]) if result["code"] != "OK" else 200, result)
            return
        if parsed.path == "/cache/clear":
            self._send(200, service.clear_cache())
            return
        if parsed.path == "/checkpoint/publish":
            record = pipeline.checkpoints.publish(
                stage=str(body.get("stage", "")),
                input_fingerprint=str(body.get("input_fingerprint", "")),
                config_version=str(body.get("config_version", "")),
                artifact_digest=str(body.get("artifact_digest", "")),
                sequence=body.get("sequence"),
            )
            self._send(200, {"code": "OK", "checkpoint": record})
            return
        self._send(404, {"code": "E_NOT_FOUND", "message": "接口不存在。", "details": {"path": parsed.path}})


class PipelineHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], run_dir: Path | str, verbose: bool = False) -> None:
        self.run_dir = Path(run_dir)
        self.verbose = verbose
        super().__init__(address, PipelineRequestHandler)


def serve(run_dir: Path | str, host: str = "127.0.0.1", port: int = 8765, verbose: bool = False) -> None:
    if host not in {"127.0.0.1", "localhost", "::1"}:
        raise PipelineError(
            "E_INVALID_CONFIG", "HTTP 服务仅允许绑定 loopback 地址。", {"host": host}
        )
    server = PipelineHTTPServer((host, port), run_dir, verbose=verbose)
    actual_host, actual_port = server.server_address[:2]
    print(
        json.dumps(
            {"event": "listening", "host": actual_host, "port": actual_port, "run_dir": str(Path(run_dir))},
            ensure_ascii=False,
            sort_keys=True,
        ),
        flush=True,
    )
    try:
        server.serve_forever(poll_interval=0.1)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
