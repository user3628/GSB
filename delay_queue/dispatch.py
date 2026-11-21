from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

import httpx

from .errors import (
    E_DISPATCH_CONNECTION,
    E_DISPATCH_HTTP_ERROR,
    E_DISPATCH_TIMEOUT,
)


@dataclass(frozen=True)
class DispatchResult:
    outcome: str
    http_status: int | None
    duration_ms: int
    error_code: str | None = None
    error_detail: str | None = None


async def dispatch_once(
    endpoint: str,
    payload: dict[str, Any],
    timeout_seconds: float,
) -> DispatchResult:
    """POST once with a real client-side timeout.

    A fresh AsyncClient is used per delivery and is explicitly closed in a
    finally block. ``Connection: close`` avoids reusing the HTTP connection.
    Cancellation/timeout is handled by httpx inside the running event loop; it
    is not emulated by sleeping and it does not claim to terminate a runaway
    thread.
    """

    started = time.perf_counter()
    client = httpx.AsyncClient(
        timeout=httpx.Timeout(timeout_seconds),
        follow_redirects=False,
        trust_env=False,
        headers={"Connection": "close"},
    )
    try:
        response = await asyncio.wait_for(
            client.post(endpoint, json=payload), timeout=timeout_seconds
        )
        duration_ms = max(0, int((time.perf_counter() - started) * 1_000))
        status = response.status_code
        await response.aclose()
        if 200 <= status < 300:
            return DispatchResult("SUCCEEDED", status, duration_ms)
        return DispatchResult(
            "FAILED",
            status,
            duration_ms,
            E_DISPATCH_HTTP_ERROR,
            f"endpoint returned HTTP {status}",
        )
    except (httpx.TimeoutException, asyncio.TimeoutError) as exc:
        duration_ms = max(0, int((time.perf_counter() - started) * 1_000))
        return DispatchResult(
            "TIMEOUT",
            None,
            duration_ms,
            E_DISPATCH_TIMEOUT,
            f"request timed out after {timeout_seconds:.3f}s: {type(exc).__name__}",
        )
    except httpx.RequestError as exc:
        duration_ms = max(0, int((time.perf_counter() - started) * 1_000))
        return DispatchResult(
            "FAILED",
            None,
            duration_ms,
            E_DISPATCH_CONNECTION,
            f"connection error: {type(exc).__name__}: {exc}",
        )
    finally:
        await client.aclose()

