from __future__ import annotations

import hashlib
import ipaddress
import json
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import httpx

from .config import WorkbenchConfig
from .errors import WorkbenchError, wb_error
from .injection import InjectionPlan


@dataclass(slots=True)
class OriginResponse:
    body: bytes
    content_type: str
    etag: str
    version: str
    tags: tuple[str, ...]
    http_status: int
    duration_ms: float
    origin_url: str


class OriginClient:
    """Edge-to-origin client that always uses loopback HTTP by default."""

    def __init__(self, config: WorkbenchConfig):
        self.config = config

    @staticmethod
    def _assert_loopback(url: str) -> None:
        hostname = urlparse(url).hostname or ""
        if hostname == "localhost":
            return
        try:
            address = ipaddress.ip_address(hostname)
        except ValueError as exc:
            raise wb_error(
                "E_ORIGIN_PROTOCOL",
                "Origin URL must resolve to a literal loopback address",
                origin_url=url,
            ) from exc
        if not address.is_loopback:
            raise wb_error(
                "E_ORIGIN_PROTOCOL",
                "External origin addresses are not permitted by this workbench",
                origin_url=url,
            )

    async def fetch(self, path: str, plan: InjectionPlan) -> OriginResponse:
        base = self.config.refused_origin_url if plan.connection_refused else self.config.effective_origin_base_url
        self._assert_loopback(base)
        url = f"{base.rstrip('/')}/origin/content"
        params = {"path": path, **plan.origin_params()}
        timeout_ms = plan.timeout_ms or self.config.origin_timeout_ms
        timeout_seconds = max(0.001, timeout_ms / 1000.0)
        timeout = httpx.Timeout(timeout_seconds)
        started = time.perf_counter()
        try:
            async with httpx.AsyncClient(
                timeout=timeout,
                trust_env=False,
                follow_redirects=False,
                headers={"X-Workbench-Origin-Client": "edge"},
            ) as client:
                response = await client.get(url, params=params)
        except httpx.TimeoutException as exc:
            duration_ms = (time.perf_counter() - started) * 1000.0
            raise wb_error(
                "E_ORIGIN_TIMEOUT",
                "The edge closed the origin connection after the injected timeout",
                duration_ms=duration_ms,
                timeout_ms=timeout_ms,
                origin_url=url,
            ) from exc
        except httpx.ConnectError as exc:
            duration_ms = (time.perf_counter() - started) * 1000.0
            raise wb_error(
                "E_ORIGIN_CONNECT",
                "The edge could not establish a connection to the selected loopback origin",
                duration_ms=duration_ms,
                origin_url=url,
            ) from exc
        except httpx.HTTPError as exc:
            duration_ms = (time.perf_counter() - started) * 1000.0
            raise wb_error(
                "E_ORIGIN_PROTOCOL",
                "The loopback origin request failed",
                duration_ms=duration_ms,
                origin_url=url,
            ) from exc
        duration_ms = (time.perf_counter() - started) * 1000.0
        if response.status_code >= 500:
            raise wb_error(
                "E_ORIGIN_5XX",
                "The origin returned an injected 5xx response",
                origin_status=response.status_code,
                duration_ms=duration_ms,
                origin_url=url,
                body_preview=response.text[:256],
            )
        if response.status_code != 200:
            raise wb_error(
                "E_ORIGIN_PROTOCOL",
                "The origin returned an unexpected status",
                origin_status=response.status_code,
                origin_url=url,
            )
        etag = response.headers.get("ETag")
        version = response.headers.get("X-Content-Version")
        if not etag or not version:
            raise wb_error(
                "E_ORIGIN_PROTOCOL",
                "Origin response is missing ETag or X-Content-Version",
                origin_url=url,
            )
        body = response.content
        calculated = hashlib.sha256(body).hexdigest()
        if version != calculated:
            raise wb_error(
                "E_ORIGIN_PROTOCOL",
                "Origin content version does not match the received body digest",
                declared_version=version,
                calculated_version=calculated,
                origin_url=url,
            )
        try:
            tags = tuple(json.loads(response.headers.get("X-Content-Tags", "[]")))
        except (json.JSONDecodeError, TypeError):
            tags = ()
        return OriginResponse(
            body=body,
            content_type=response.headers.get("Content-Type", "application/octet-stream").split(";", 1)[0],
            etag=etag,
            version=version,
            tags=tags,
            http_status=response.status_code,
            duration_ms=duration_ms,
            origin_url=url,
        )


def content_identity(body: bytes) -> tuple[str, str]:
    digest = hashlib.sha256(body).hexdigest()
    return f'"{digest}"', digest

