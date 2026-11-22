"""Collect the local test service's throughput/error counters over loopback HTTP."""

from __future__ import annotations

import ipaddress
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from .errors import bad_request, not_found


def validate_loopback_url(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme != "http":
        raise bad_request("本地测试服务指标地址仅支持 http", url=url)
    host = (parsed.hostname or "").strip().lower()
    if not host:
        raise bad_request("指标地址缺少主机", url=url)
    if host != "localhost":
        try:
            address = ipaddress.ip_address(host)
        except ValueError as exc:
            raise bad_request(
                "仅允许回环指标地址（localhost、127.0.0.0/8 或 ::1）", url=url
            ) from exc
        if not address.is_loopback:
            raise bad_request("禁止从非回环地址采集指标", url=url, host=host)
    if parsed.port is None:
        raise bad_request("指标地址必须显式包含端口", url=url)
    if parsed.path in {"", "/"}:
        path = "/metrics"
    elif parsed.path == "/metrics":
        path = "/metrics"
    else:
        raise bad_request("指标地址路径必须为 /metrics", url=url)
    return urllib.parse.urlunsplit(("http", parsed.netloc, path, "", ""))


def fetch_service_metrics(url: str, *, timeout_seconds: float = 2.0) -> dict[str, Any]:
    safe_url = validate_loopback_url(url)
    request = urllib.request.Request(safe_url, method="GET", headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            raw = response.read(1024 * 1024 + 1)
    except urllib.error.HTTPError as exc:
        raise not_found("本地测试服务指标接口返回 HTTP 错误", status=exc.code, url=safe_url) from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise not_found("无法采集本地测试服务指标", url=safe_url, reason=str(exc)) from exc
    if len(raw) > 1024 * 1024:
        raise bad_request("测试服务指标响应超过 1 MiB 上限", url=safe_url)
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise bad_request("测试服务指标不是合法 JSON", url=safe_url) from exc
    if not isinstance(payload, dict):
        raise bad_request("测试服务指标必须是 JSON 对象", url=safe_url)
    return {
        "source_url": safe_url,
        "collected_at_ms": time.time_ns() // 1_000_000,
        "metrics": payload,
    }
