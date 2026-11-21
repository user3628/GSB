"""HTTP client for the local transaction coordinator test service."""

from __future__ import annotations

import time
from dataclasses import dataclass
from urllib.parse import quote

import httpx

from . import codes


@dataclass(frozen=True)
class CoordinatorResult:
    outcome: str
    source: str
    http_status: int
    elapsed_ms: int
    response: dict
    injection: str | None


class CoordinatorFailure(Exception):
    def __init__(self, code: str, message: str, details: dict | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}


class CoordinatorClient:
    def __init__(
        self,
        base_url: str,
        unreachable_url: str | None = None,
        default_timeout_seconds: float = 0.5,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.unreachable_url = (unreachable_url or "http://127.0.0.1:1").rstrip("/")
        self.default_timeout_seconds = default_timeout_seconds

    def _override_for(self, injection: str | None) -> str | None:
        if injection in {"commit", "rollback", "unknown", "hang"}:
            return injection.upper()
        return None

    def query(
        self,
        txn_id: str,
        idempotency_key: str,
        timeout_seconds: float | None = None,
        injection: str | None = None,
        force: bool = False,
    ) -> CoordinatorResult:
        timeout_s = max(0.01, float(timeout_seconds or self.default_timeout_seconds))
        base = self.unreachable_url if injection == "refuse" else self.base_url
        url = f"{base}/txn/{quote(txn_id, safe='')}"
        headers = {"Idempotency-Key": idempotency_key}
        override = self._override_for(injection)
        if override:
            headers["X-Fault-Injection"] = override
        if force:
            headers["X-Force-Query"] = "1"

        timeout = httpx.Timeout(
            timeout_s,
            connect=min(timeout_s, 0.5),
            read=timeout_s,
            write=timeout_s,
            pool=timeout_s,
        )
        started = time.monotonic()
        try:
            # A per-query client is intentional: leaving the with-block closes
            # the socket, including on read timeout.
            with httpx.Client(timeout=timeout, trust_env=False) as client:
                response = client.get(url, headers=headers)
        except httpx.TimeoutException as exc:
            elapsed = int((time.monotonic() - started) * 1000)
            raise CoordinatorFailure(
                codes.E_TXN_TIMEOUT,
                "coordinator query timed out; connection was closed",
                {"error": type(exc).__name__, "elapsed_ms": elapsed, "url": url},
            ) from exc
        except (httpx.ConnectError, httpx.NetworkError, httpx.RemoteProtocolError) as exc:
            elapsed = int((time.monotonic() - started) * 1000)
            raise CoordinatorFailure(
                codes.E_TXN_UNREACHABLE,
                "coordinator is unreachable",
                {"error": type(exc).__name__, "elapsed_ms": elapsed, "url": url},
            ) from exc
        except httpx.HTTPError as exc:
            elapsed = int((time.monotonic() - started) * 1000)
            raise CoordinatorFailure(
                codes.E_TXN_UNREACHABLE,
                "coordinator transport failed",
                {"error": type(exc).__name__, "elapsed_ms": elapsed, "url": url},
            ) from exc

        elapsed = int((time.monotonic() - started) * 1000)
        if response.status_code != 200:
            raise CoordinatorFailure(
                codes.E_TXN_UNKNOWN,
                "coordinator did not return a usable conclusion",
                {
                    "http_status": response.status_code,
                    "body": response.text[:500],
                    "elapsed_ms": elapsed,
                },
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise CoordinatorFailure(
                codes.E_TXN_UNKNOWN,
                "coordinator response is not JSON",
                {"body": response.text[:500], "elapsed_ms": elapsed},
            ) from exc

        outcome = str(payload.get("outcome", payload.get("decision", ""))).upper()
        if outcome not in {"COMMIT", "ROLLBACK"}:
            raise CoordinatorFailure(
                codes.E_TXN_UNKNOWN,
                "coordinator returned an unknown conclusion",
                {"response": payload, "elapsed_ms": elapsed},
            )
        source = str(payload.get("source") or "coordinator")
        if injection:
            source = f"{source}:injected:{injection}"
        return CoordinatorResult(
            outcome=outcome,
            source=source,
            http_status=response.status_code,
            elapsed_ms=elapsed,
            response=payload,
            injection=injection,
        )

