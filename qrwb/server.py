"""本地 HTTP 接口（仅标准库 http.server，无外部依赖）。

端点：
* POST /import                 导入分片与配置（确定性生成样本）
* POST /run?parallel=0|1       执行流水线
* GET  /status?query_id=...    查询状态与版本指纹
* GET  /budget                 预算消耗与未处理原因
* GET  /trace?query_id=...     裁决轨迹
* POST /stop                   请求优雅停止
* POST /resume                 从检查点恢复执行
* GET  /health                 健康检查
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Tuple
from urllib.parse import parse_qs, urlparse

from .cli import _load
from .config import load_config, write_default_config
from .sampling import generate_workspace_inputs

_LOCK = threading.Lock()


def _json_response(handler: BaseHTTPRequestHandler, status: int,
                   payload: Dict[str, Any]) -> None:
    body = (json.dumps(payload, ensure_ascii=False, sort_keys=True,
                       indent=2) + "\n").encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


class Handler(BaseHTTPRequestHandler):
    server_version = "qrwb-http/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:  # 安静日志
        return

    @property
    def workspace(self) -> str:
        return self.server.workspace  # type: ignore[attr-defined]

    @property
    def config_path(self) -> str:
        return self.server.config_path  # type: ignore[attr-defined]

    def _read_body(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", 0) or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8")) if raw else {}

    def _query(self) -> Dict[str, str]:
        parsed = urlparse(self.path)
        return {k: v[0] for k, v in parse_qs(parsed.query).items()}

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        try:
            with _LOCK:
                if path == "/health":
                    _json_response(self, 200, {"status": "ok"})
                    return
                cfg, pipeline = _load(self.workspace, self.config_path)
                if path == "/status":
                    query_id = self._query().get("query_id", "")
                    _json_response(self, 200,
                                   pipeline.query_status(query_id))
                elif path == "/budget":
                    _json_response(self, 200, pipeline.budget_view())
                elif path == "/trace":
                    query_id = self._query().get("query_id", "")
                    _json_response(self, 200,
                                   pipeline.adjudication_trace(query_id))
                else:
                    _json_response(self, 404, {"error": "not found",
                                               "path": path})
        except FileNotFoundError as exc:
            _json_response(self, 409, {"error": str(exc),
                                       "hint": "请先 POST /import"})
        except Exception as exc:  # noqa: BLE001
            _json_response(self, 500,
                           {"error": f"{type(exc).__name__}: {exc}"})

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        try:
            body = self._read_body()
            with _LOCK:
                if path == "/import":
                    workspace = body.get("workspace", self.workspace)
                    self.server.workspace = workspace  # type: ignore[attr-defined]
                    config_path = body.get("config_path")
                    if config_path:
                        self.server.config_path = config_path  # type: ignore[attr-defined]
                    else:
                        self.server.config_path = \
                            f"{workspace}/config.yaml"  # type: ignore[attr-defined]
                        write_default_config(self.server.config_path)
                    cfg = load_config(self.server.config_path)
                    paths = generate_workspace_inputs(
                        cfg, f"{workspace}/inputs",
                        shard_count=int(body.get("shards", 2)),
                        rows_per_shard=int(body.get("rows", 12)))
                    _json_response(self, 200,
                                   {"workspace": workspace, "inputs": paths})
                    return

                cfg, pipeline = _load(self.workspace, self.config_path)
                if path == "/run":
                    parallel = self._query().get("parallel", "1") != "0"
                    _json_response(self, 200, pipeline.run(parallel=parallel))
                elif path == "/stop":
                    pipeline.request_stop()
                    _json_response(self, 200, {"stop_requested": True})
                elif path == "/resume":
                    pipeline.clear_stop()
                    _json_response(self, 200, pipeline.run(parallel=True))
                else:
                    _json_response(self, 404,
                                   {"error": "not found", "path": path})
        except Exception as exc:  # noqa: BLE001
            _json_response(self, 500,
                           {"error": f"{type(exc).__name__}: {exc}"})


def serve(workspace: str, config_path: str, host: str = "127.0.0.1",
          port: int = 8765) -> Tuple[ThreadingHTTPServer, str]:
    server = ThreadingHTTPServer((host, port), Handler)
    server.workspace = workspace  # type: ignore[attr-defined]
    server.config_path = config_path  # type: ignore[attr-defined]
    return server, f"http://{host}:{server.server_address[1]}"
