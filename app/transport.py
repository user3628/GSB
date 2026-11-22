from __future__ import annotations

import http.client
import socket
import time
from dataclasses import dataclass
from urllib.parse import urlencode, urlsplit

LOCAL_HOSTS = {"127.0.0.1", "localhost", "::1"}


@dataclass(frozen=True)
class TransportResult:
    status: int
    body: bytes
    elapsed_s: float


class TransportFailure(Exception):
    def __init__(self, reason: str, *, timed_out: bool, elapsed_s: float, cause: str) -> None:
        super().__init__(reason)
        self.reason = reason
        self.timed_out = timed_out
        self.elapsed_s = elapsed_s
        self.cause = cause


class LocalHTTPClient:
    """One request per connection, with an explicit close on success and timeout.

    A client timeout aborts this HTTP connection. It deliberately does not claim
    that already-running remote code has been terminated.
    """

    def __init__(self, base_url: str) -> None:
        parsed = urlsplit(base_url)
        if parsed.scheme not in {"http", "https"}:
            raise ValueError("test endpoint URL must use http or https")
        if parsed.hostname not in LOCAL_HOSTS:
            raise ValueError("test endpoint must resolve to a loopback host")
        self.base_url = base_url.rstrip("/")
        self._parsed = parsed

    def get_json(
        self,
        path: str,
        params: dict[str, str | int | bool],
        *,
        timeout_s: float,
    ) -> TransportResult:
        query = urlencode({key: str(value).lower() if isinstance(value, bool) else value for key, value in params.items()})
        base_path = self._parsed.path.rstrip("/")
        target = f"{base_path}{path}"
        if query:
            target = f"{target}?{query}"
        port = self._parsed.port or (443 if self._parsed.scheme == "https" else 80)
        started = time.monotonic()
        connection: http.client.HTTPConnection | None = None
        try:
            if self._parsed.scheme == "https":
                connection = http.client.HTTPSConnection(
                    self._parsed.hostname, port, timeout=timeout_s
                )
            else:
                connection = http.client.HTTPConnection(
                    self._parsed.hostname, port, timeout=timeout_s
                )
            connection.request(
                "GET",
                target,
                headers={"Connection": "close", "Accept": "application/json"},
            )
            response = connection.getresponse()
            body = response.read()
            elapsed = time.monotonic() - started
            if response.status >= 400:
                raise TransportFailure(
                    f"test endpoint returned HTTP {response.status}",
                    timed_out=False,
                    elapsed_s=elapsed,
                    cause=body.decode("utf-8", errors="replace")[:500],
                )
            return TransportResult(status=response.status, body=body, elapsed_s=elapsed)
        except (TimeoutError, socket.timeout) as exc:
            elapsed = time.monotonic() - started
            raise TransportFailure(
                "socket read timed out; the HTTP connection was closed",
                timed_out=True,
                elapsed_s=elapsed,
                cause=f"{type(exc).__name__}: {exc}",
            ) from exc
        except TransportFailure:
            raise
        except (ConnectionError, OSError, http.client.HTTPException) as exc:
            elapsed = time.monotonic() - started
            raise TransportFailure(
                "local test endpoint connection failed or was refused",
                timed_out=False,
                elapsed_s=elapsed,
                cause=f"{type(exc).__name__}: {exc}",
            ) from exc
        finally:
            if connection is not None:
                connection.close()

