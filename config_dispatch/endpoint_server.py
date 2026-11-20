"""本地回环 HTTP 测试端点：模拟组件对配置推送的确定性响应。

端点规则来自端点脚本 JSON（固定样本），支持：
- success: 成功应用（按 idempotency_key 幂等，重复请求返回同一结果）
- reject:  业务拒绝（HTTP 422，返回结构化拒绝原因）
- delay:   延时响应，用于超时与强制终止场景
- 连接失败场景通过指向未监听端口模拟，无需服务端逻辑
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


class _Handler(BaseHTTPRequestHandler):
    server_version = "ConfigDispatchTestEndpoint/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:  # 静默，保持演示输出干净
        return

    def _send_json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict | None:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return None
        try:
            return json.loads(self.rfile.read(length).decode("utf-8"))
        except json.JSONDecodeError:
            return None

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/health":
            self._send_json(200, {"status": "ok", "component_id": self.server.component_id})
            return
        self._send_json(404, {"error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802
        payload = self._read_json()
        if payload is None:
            self._send_json(400, {"error": "invalid_json"})
            return
        rule = self.server.rules.get(self.path)
        if rule is None:
            self._send_json(404, {"error": "unknown_path", "path": self.path})
            return
        behavior = rule.get("behavior", "success")
        idem = str(payload.get("idempotency_key", ""))
        with self.server.counter_lock:
            self.server.request_counts[self.path] = self.server.request_counts.get(self.path, 0) + 1
            seen = self.server.idempotency_cache.setdefault(self.path, {})
            if idem and idem in seen:
                self._send_json(200, seen[idem])
                return
            if behavior == "delay":
                import time

                time.sleep(float(rule.get("delay_seconds", 5.0)))
            if behavior == "reject":
                response = {
                    "status": "rejected",
                    "component_id": self.server.component_id,
                    "reason": rule.get("reason", "business_rejection"),
                    "idempotency_key": idem,
                }
                self._send_json(422, response)
                return
            response = {
                "status": "applied",
                "component_id": self.server.component_id,
                "version": payload.get("version"),
                "idempotency_key": idem,
            }
            if idem:
                seen[idem] = response
            try:
                self._send_json(200, response)
            except (BrokenPipeError, ConnectionResetError):
                # 客户端因超时主动关闭连接，属超时测试的预期行为
                return


class ComponentHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], component_id: str, rules: dict):
        super().__init__(address, _Handler)
        self.component_id = component_id
        self.rules = rules
        self.request_counts: dict[str, int] = {}
        self.idempotency_cache: dict[str, dict] = {}
        self.counter_lock = threading.Lock()


def parse_endpoint(url: str) -> tuple[str, int]:
    # 仅支持 http://host:port 形式的本地回环样本地址
    rest = url.removeprefix("http://").removeprefix("https://")
    host, port_text = rest.split("/", 1)[0].split(":")
    return host, int(port_text)


class EndpointCluster:
    """按组件声明在回环端口启动一组测试端点。"""

    def __init__(self, declarations: dict, endpoint_script: dict):
        self._declarations = declarations
        self._script = endpoint_script
        self._servers: dict[str, ComponentHTTPServer] = {}
        self._threads: dict[str, threading.Thread] = {}

    def start(self) -> None:
        for component_id, declaration in self._declarations.items():
            endpoint = declaration.endpoint if hasattr(declaration, "endpoint") else declaration["endpoint"]
            # 指向失效端口的组件不启动监听，用于真实模拟连接拒绝
            rules = self._script.get(component_id, {})
            if "endpoint_override" in rules:
                continue
            host, port = parse_endpoint(endpoint)
            server = ComponentHTTPServer((host, port), component_id, rules)
            thread = threading.Thread(
                target=server.serve_forever, name=f"endpoint-{component_id}", daemon=True
            )
            thread.start()
            self._servers[component_id] = server
            self._threads[component_id] = thread

    def stop(self) -> None:
        for server in self._servers.values():
            server.shutdown()
            server.server_close()
        for thread in self._threads.values():
            thread.join(timeout=5)
        self._servers.clear()
        self._threads.clear()

    def __enter__(self) -> "EndpointCluster":
        self.start()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.stop()

    @property
    def request_counts(self) -> dict[str, int]:
        return {
            component_id: dict(server.request_counts)
            for component_id, server in self._servers.items()
        }


def load_endpoint_script(path: str | Path) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)
