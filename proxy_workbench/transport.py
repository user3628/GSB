"""Minimal HTTP/1.1 upstream transport with deterministic socket cleanup."""

from __future__ import annotations

import asyncio
import errno
import time
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import urlsplit

from .errors import (
    E_CONNECT_FAILED,
    E_CONNECT_TIMEOUT,
    E_CONNECTION_REFUSED,
    E_READ_TIMEOUT,
    E_UPSTREAM_PROTOCOL,
    E_WRITE_TIMEOUT,
    ProxyError,
)
from .models import HOP_BY_HOP_HEADERS


MAX_UPSTREAM_BODY = 32 * 1024 * 1024
MAX_HEADER_BYTES = 128 * 1024


@dataclass(frozen=True)
class UpstreamResponse:
    status: int
    reason: str
    headers: tuple[tuple[str, str], ...]
    body: bytes

    def header(self, name: str) -> str | None:
        lower = name.lower()
        for key, value in self.headers:
            if key.lower() == lower:
                return value
        return None


class _ReaderFailure(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def join_upstream_path(base_path: str, request_path: str) -> str:
    path = request_path or "/"
    if not path.startswith("/"):
        path = "/" + path
    if not base_path or base_path == "/":
        return path
    return base_path.rstrip("/") + "/" + path.lstrip("/")


def _authority_from_upstream(upstream: str) -> str:
    parts = urlsplit(upstream)
    return parts.netloc


def request_target_path(upstream: str, path_query: str) -> str:
    parts = urlsplit(upstream)
    path, separator, query = path_query.partition("?")
    joined = join_upstream_path(parts.path, path)
    return joined + (separator + query if separator else "")


def _refused(exc: BaseException) -> bool:
    if isinstance(exc, ConnectionRefusedError):
        return True
    if isinstance(exc, OSError):
        return exc.errno in (errno.ECONNREFUSED, getattr(errno, "WSAECONNREFUSED", 10061)) or getattr(
            exc, "winerror", None
        ) == 10061
    return False


async def _close_writer(writer: asyncio.StreamWriter | None) -> None:
    if writer is None:
        return
    writer.close()
    try:
        await asyncio.wait_for(writer.wait_closed(), timeout=0.5)
    except (asyncio.TimeoutError, ConnectionError, OSError):
        pass


async def _read_with_timeout(awaitable, timeout: float, code: str, message: str):
    try:
        return await asyncio.wait_for(awaitable, timeout=timeout)
    except asyncio.TimeoutError as exc:
        raise _ReaderFailure(code, message) from exc


async def _read_headers(reader: asyncio.StreamReader, timeout: float) -> tuple[int, str, list[tuple[str, str]]]:
    try:
        raw = await _read_with_timeout(
            reader.readuntil(b"\r\n\r\n"), timeout, E_READ_TIMEOUT, "upstream response header read timed out"
        )
    except asyncio.IncompleteReadError as exc:
        raise _ReaderFailure(E_UPSTREAM_PROTOCOL, "upstream closed before a complete response header") from exc
    except asyncio.LimitOverrunError as exc:
        raise _ReaderFailure(E_UPSTREAM_PROTOCOL, "upstream response header too large") from exc
    if len(raw) > MAX_HEADER_BYTES:
        raise _ReaderFailure(E_UPSTREAM_PROTOCOL, "upstream response header too large")
    try:
        text = raw.decode("latin-1")
        lines = text.split("\r\n")
        status_line = lines[0]
        status_parts = status_line.split(" ", 2)
        if len(status_parts) < 2:
            raise ValueError("missing status code")
        version, status_text = status_parts[0], status_parts[1]
        reason = status_parts[2] if len(status_parts) > 2 else ""
    except (UnicodeDecodeError, ValueError) as exc:
        raise _ReaderFailure(E_UPSTREAM_PROTOCOL, "malformed upstream response status line") from exc
    if not version.startswith("HTTP/") or not status_text.isdigit():
        raise _ReaderFailure(E_UPSTREAM_PROTOCOL, "malformed upstream response status line")
    status = int(status_text)
    headers: list[tuple[str, str]] = []
    for line in lines[1:]:
        if not line:
            continue
        if line.startswith((" ", "\t")):
            raise _ReaderFailure(E_UPSTREAM_PROTOCOL, "obsolete header folding is not accepted")
        name, separator, value = line.partition(":")
        if not separator or not name:
            raise _ReaderFailure(E_UPSTREAM_PROTOCOL, "malformed upstream response header")
        headers.append((name, value.strip()))
    return status, reason, headers


async def _read_exact(reader: asyncio.StreamReader, size: int, timeout: float) -> bytes:
    if size < 0 or size > MAX_UPSTREAM_BODY:
        raise _ReaderFailure(E_UPSTREAM_PROTOCOL, "invalid or excessive upstream Content-Length")
    try:
        return await _read_with_timeout(
            reader.readexactly(size), timeout, E_READ_TIMEOUT, "upstream response body read timed out"
        )
    except asyncio.IncompleteReadError as exc:
        raise _ReaderFailure(E_UPSTREAM_PROTOCOL, "upstream response body ended early") from exc


async def _read_chunked(reader: asyncio.StreamReader, timeout: float) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        try:
            line = await _read_with_timeout(
                reader.readline(), timeout, E_READ_TIMEOUT, "upstream chunk-size read timed out"
            )
        except ValueError as exc:
            raise _ReaderFailure(E_UPSTREAM_PROTOCOL, "upstream chunk line too large") from exc
        if not line.endswith(b"\r\n"):
            raise _ReaderFailure(E_UPSTREAM_PROTOCOL, "malformed upstream chunk size")
        size_text = line[:-2].split(b";", 1)[0].strip()
        try:
            size = int(size_text, 16)
        except ValueError as exc:
            raise _ReaderFailure(E_UPSTREAM_PROTOCOL, "invalid upstream chunk size") from exc
        if size == 0:
            while True:
                try:
                    trailer = await _read_with_timeout(
                        reader.readline(), timeout, E_READ_TIMEOUT, "upstream trailer read timed out"
                    )
                except ValueError as exc:
                    raise _ReaderFailure(E_UPSTREAM_PROTOCOL, "upstream trailer line too large") from exc
                if trailer in (b"\r\n", b"\n", b""):
                    break
            break
        total += size
        if total > MAX_UPSTREAM_BODY:
            raise _ReaderFailure(E_UPSTREAM_PROTOCOL, "upstream response body exceeds limit")
        chunks.append(await _read_exact(reader, size, timeout))
        ending = await _read_exact(reader, 2, timeout)
        if ending != b"\r\n":
            raise _ReaderFailure(E_UPSTREAM_PROTOCOL, "malformed upstream chunk terminator")
    return b"".join(chunks)


def _connection_tokens(headers: Iterable[tuple[str, str]]) -> set[str]:
    tokens: set[str] = set()
    for name, value in headers:
        if name.lower() == "connection":
            tokens.update(item.strip().lower() for item in value.split(",") if item.strip())
    return tokens


def filter_hop_by_hop(
    headers: Iterable[tuple[str, str]], *, extra_remove: Iterable[str] = ()
) -> list[tuple[str, str]]:
    remove = set(HOP_BY_HOP_HEADERS)
    remove.update(item.lower() for item in extra_remove)
    header_list = list(headers)
    remove.update(_connection_tokens(header_list))
    return [(name, value) for name, value in header_list if name.lower() not in remove]


async def perform_request(
    *,
    upstream: str,
    method: str,
    path_query: str,
    headers: Iterable[tuple[str, str]],
    body: bytes,
    connect_timeout: float,
    read_timeout: float,
) -> UpstreamResponse:
    """Open one TCP connection, exchange one request, and always close it."""
    parts = urlsplit(upstream)
    host = parts.hostname
    if not host:
        raise ProxyError(E_CONNECT_FAILED, "upstream URL has no host", 502, {"upstream": upstream})
    port = parts.port or 80
    authority = _authority_from_upstream(upstream)
    target = request_target_path(upstream, path_query)
    filtered = filter_hop_by_hop(headers, extra_remove={"host", "content-length"})
    request_headers: list[tuple[str, str]] = [("Host", authority)]
    request_headers.extend(filtered)
    request_headers.append(("Content-Length", str(len(body))))
    request_headers.append(("Connection", "close"))
    lines = [f"{method} {target} HTTP/1.1"]
    lines.extend(f"{name}: {value}" for name, value in request_headers)
    raw_request = ("\r\n".join(lines) + "\r\n\r\n").encode("latin-1") + body

    writer: asyncio.StreamWriter | None = None
    reader: asyncio.StreamReader | None = None
    started = time.monotonic()
    try:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port, limit=MAX_HEADER_BYTES), timeout=connect_timeout
            )
        except asyncio.TimeoutError as exc:
            raise ProxyError(
                E_CONNECT_TIMEOUT,
                "upstream connection timed out",
                504,
                {"upstream": upstream, "timeout": connect_timeout, "elapsed_ms": int((time.monotonic() - started) * 1000)},
            ) from exc
        except BaseException as exc:
            if _refused(exc):
                raise ProxyError(
                    E_CONNECTION_REFUSED,
                    "upstream refused the connection",
                    502,
                    {"upstream": upstream},
                ) from exc
            if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
                raise
            raise ProxyError(
                E_CONNECT_FAILED,
                "upstream connection failed",
                502,
                {"upstream": upstream, "error": type(exc).__name__},
            ) from exc

        try:
            writer.write(raw_request)
            try:
                await asyncio.wait_for(writer.drain(), timeout=read_timeout)
            except asyncio.TimeoutError as exc:
                raise ProxyError(
                    E_WRITE_TIMEOUT,
                    "upstream request write timed out",
                    504,
                    {"upstream": upstream, "timeout": read_timeout},
                ) from exc

            status, reason, response_headers = await _read_headers(reader, read_timeout)
            while 100 <= status < 200 and status != 101:
                status, reason, response_headers = await _read_headers(reader, read_timeout)
            if status == 101:
                raise _ReaderFailure(E_UPSTREAM_PROTOCOL, "upstream protocol upgrades are not supported")

            header_map: dict[str, list[str]] = {}
            for name, value in response_headers:
                header_map.setdefault(name.lower(), []).append(value)
            transfer_encoding = ",".join(header_map.get("transfer-encoding", [])).lower()
            content_length_values = header_map.get("content-length", [])
            if len(set(content_length_values)) > 1:
                raise _ReaderFailure(E_UPSTREAM_PROTOCOL, "conflicting upstream Content-Length headers")
            no_body = method.upper() == "HEAD" or status in (204, 304)
            if no_body:
                response_body = b""
            elif "chunked" in transfer_encoding:
                response_body = await _read_chunked(reader, read_timeout)
            elif content_length_values:
                try:
                    length = int(content_length_values[0])
                except ValueError as exc:
                    raise _ReaderFailure(E_UPSTREAM_PROTOCOL, "invalid upstream Content-Length") from exc
                response_body = await _read_exact(reader, length, read_timeout)
            else:
                chunks: list[bytes] = []
                total = 0
                while True:
                    try:
                        chunk = await _read_with_timeout(
                            reader.read(65536),
                            read_timeout,
                            E_READ_TIMEOUT,
                            "upstream response body read timed out",
                        )
                    except _ReaderFailure:
                        raise
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > MAX_UPSTREAM_BODY:
                        raise _ReaderFailure(E_UPSTREAM_PROTOCOL, "upstream response body exceeds limit")
                    chunks.append(chunk)
                response_body = b"".join(chunks)
            return UpstreamResponse(
                status=status,
                reason=reason,
                headers=tuple(response_headers),
                body=response_body,
            )
        except _ReaderFailure as exc:
            raise ProxyError(
                exc.code,
                exc.message,
                504 if exc.code == E_READ_TIMEOUT else 502,
                {"upstream": upstream},
            ) from exc
        except ProxyError:
            raise
        except asyncio.CancelledError:
            raise
        except (ConnectionError, OSError) as exc:
            raise ProxyError(
                E_UPSTREAM_PROTOCOL,
                "upstream connection failed during response exchange",
                502,
                {"upstream": upstream, "error": type(exc).__name__},
            ) from exc
    finally:
        await _close_writer(writer)

