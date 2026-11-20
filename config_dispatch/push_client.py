"""推送客户端：区分业务拒绝、连接失败与超时，超时真实关闭底层连接。"""

from __future__ import annotations

import http.client
import json
import socket
from typing import Any

from .endpoint_server import parse_endpoint
from .errors import (
    EndpointConnectionError,
    EndpointRejectedError,
    EndpointTimeoutError,
)


class PushClient:
    def __init__(self, timeout_seconds: float = 2.0):
        self.timeout_seconds = timeout_seconds

    def _open(self, endpoint: str) -> http.client.HTTPConnection:
        host, port = parse_endpoint(endpoint)
        return http.client.HTTPConnection(host, port, timeout=self.timeout_seconds)

    def post(self, endpoint: str, path: str, payload: dict[str, Any]) -> dict:
        connection = self._open(endpoint)
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        try:
            connection.request(
                "POST",
                path,
                body=body,
                headers={"Content-Type": "application/json; charset=utf-8"},
            )
            response = connection.getresponse()
            data = response.read()
            if response.status == 200:
                return json.loads(data.decode("utf-8")) if data else {"status": "applied"}
            try:
                details = json.loads(data.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                details = {"raw": data.decode("utf-8", errors="replace")}
            raise EndpointRejectedError(
                f"端点业务拒绝 HTTP {response.status}",
                details={"http_status": response.status, "response": details},
            )
        except socket.timeout as exc:
            # 必须真实关闭连接并释放资源，而不是继续等待冒充终止
            connection.close()
            raise EndpointTimeoutError(
                f"推送超时（{self.timeout_seconds}s），连接已关闭",
                details={"endpoint": endpoint, "path": path, "timeout_seconds": self.timeout_seconds},
            ) from exc
        except (ConnectionRefusedError, socket.gaierror, OSError) as exc:
            connection.close()
            raise EndpointConnectionError(
                f"端点连接失败: {exc}",
                details={"endpoint": endpoint, "path": path, "reason": type(exc).__name__},
            ) from exc
        finally:
            connection.close()
