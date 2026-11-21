"""HTTP push adapter with explicit failure classification and socket closure."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import httpx

from .errors import (
    E_CONFLICTING_PUSH,
    E_PUSH_CONNECTION,
    E_PUSH_REJECTED,
    E_PUSH_TIMEOUT,
)


@dataclass
class PushResult:
    ok: bool
    code: str | None
    message: str
    status_code: int | None
    elapsed_ms: float
    response: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "code": self.code,
            "message": self.message,
            "status_code": self.status_code,
            "elapsed_ms": round(self.elapsed_ms, 3),
            "response": self.response,
        }


async def post_json(
    url: str,
    payload: dict[str, Any],
    *,
    timeout_seconds: float,
) -> PushResult:
    """POST JSON and distinguish timeout, connection and business rejection.

    ``httpx`` receives connect/read/write/pool deadlines.  On a read timeout the
    client is explicitly closed, which closes the underlying socket instead of
    merely returning early from a sleep/wait loop.
    """

    timeout = httpx.Timeout(
        connect=min(timeout_seconds, 1.0),
        read=timeout_seconds,
        write=timeout_seconds,
        pool=min(timeout_seconds, 1.0),
    )
    started = time.perf_counter()
    client = httpx.AsyncClient(timeout=timeout, trust_env=False)
    try:
        response = await client.post(url, json=payload)
    except httpx.TimeoutException as exc:
        elapsed = (time.perf_counter() - started) * 1000
        return PushResult(
            False,
            E_PUSH_TIMEOUT,
            f"push exceeded {timeout_seconds}s and the client socket was closed",
            None,
            elapsed,
            {"exception": type(exc).__name__},
        )
    except httpx.HTTPError as exc:
        elapsed = (time.perf_counter() - started) * 1000
        return PushResult(
            False,
            E_PUSH_CONNECTION,
            "connection to cluster endpoint failed",
            None,
            elapsed,
            {"exception": type(exc).__name__, "detail": str(exc)},
        )
    finally:
        await client.aclose()

    elapsed = (time.perf_counter() - started) * 1000
    try:
        body = response.json()
    except ValueError:
        body = {"raw": response.text[:1000]}

    if 200 <= response.status_code < 300:
        return PushResult(
            True,
            None,
            "endpoint accepted configuration",
            response.status_code,
            elapsed,
            body if isinstance(body, dict) else {"body": body},
        )

    remote_code = body.get("code") if isinstance(body, dict) else None
    if response.status_code == 409 or remote_code == E_CONFLICTING_PUSH:
        code = E_CONFLICTING_PUSH
        message = "endpoint reported a conflicting idempotency request"
    else:
        code = E_PUSH_REJECTED
        message = "endpoint rejected the configuration"
    return PushResult(
        False,
        code,
        message,
        response.status_code,
        elapsed,
        body if isinstance(body, dict) else {"body": body},
    )

