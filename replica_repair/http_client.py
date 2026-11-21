from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from typing import Any


class LocalHttpClient:
    def __init__(self, base_url: str = "http://127.0.0.1:18765") -> None:
        self.base_url = base_url.rstrip("/")

    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> tuple[int, dict[str, Any]]:
        data = None
        headers: dict[str, str] = {"Accept": "application/json"}
        if payload is not None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json; charset=utf-8"
        request = urllib.request.Request(
            self.base_url + path,
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=60.0) as response:
                body = response.read().decode("utf-8")
                return response.status, json.loads(body)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8")
            try:
                parsed = json.loads(body)
            except json.JSONDecodeError:
                parsed = {
                    "ok": False,
                    "code": "E_HTTP_NON_JSON",
                    "message": body,
                }
            exc.close()
            return exc.code, parsed

    def get(self, path: str) -> tuple[int, dict[str, Any]]:
        return self.request("GET", path)

    def post(self, path: str, payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        return self.request("POST", path, payload)

    def wait_until_healthy(self, timeout_seconds: float = 10.0) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_seconds
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                status, payload = self.get("/health")
                if status == 200 and payload.get("ok"):
                    return payload
            except Exception as exc:  # connection refused while process starts
                last_error = exc
            time.sleep(0.05)
        raise RuntimeError(f"本地 HTTP 服务未在限定时间内就绪: {last_error}")

