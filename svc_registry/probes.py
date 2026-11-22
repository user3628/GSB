from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from .db import InstanceRow, now_ms
from .errors import E_PROBE_FAILED, E_PROBE_TIMEOUT


@dataclass(frozen=True)
class ProbeExecution:
    success: bool
    status: str
    outcome: str
    latency_ms: int
    error_code: str | None = None
    detail: dict[str, Any] | None = None
    executed: bool = True


def _effective_timeout_ms(row: InstanceRow, params: dict[str, Any]) -> int:
    timeout = int(params.get("timeout_ms", 1000))
    if row.probe_window_ms is not None:
        timeout = min(timeout, int(row.probe_window_ms))
    return max(1, timeout)


async def _close_writer(writer: asyncio.StreamWriter | None) -> None:
    if writer is None:
        return
    try:
        writer.close()
        await asyncio.wait_for(writer.wait_closed(), timeout=0.25)
    except (asyncio.TimeoutError, ConnectionError, OSError):
        transport = writer.transport
        if transport is not None:
            transport.abort()


async def _http_probe(row: InstanceRow, timeout_ms: int) -> ProbeExecution:
    started = time.perf_counter()
    parsed = urlsplit(row.endpoint)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    params = row.probe_params
    configured_path = params.get("path")
    if configured_path:
        path = configured_path
        if parsed.query and "?" not in path:
            path += "?" + parsed.query
    else:
        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query
    expected_status = int(params.get("expected_status", 200))
    expected_marker = str(params.get("expected_body_contains", "OK"))
    writer: asyncio.StreamWriter | None = None
    reader: asyncio.StreamReader | None = None
    try:
        async with asyncio.timeout(timeout_ms / 1000):
            reader, writer = await asyncio.open_connection(host, port)
            request = (
                f"GET {path} HTTP/1.1\r\n"
                f"Host: {host}:{port}\r\n"
                "Connection: close\r\n"
                "Accept: */*\r\n"
                "User-Agent: local-registry-probe/2\r\n"
                "\r\n"
            ).encode("ascii")
            writer.write(request)
            await writer.drain()
            header_blob = await reader.readuntil(b"\r\n\r\n")
            lines = header_blob.decode("iso-8859-1").split("\r\n")
            status_parts = lines[0].split(" ", 2)
            if len(status_parts) < 2 or not status_parts[1].isdigit():
                raise ValueError("invalid HTTP status line")
            status_code = int(status_parts[1])
            headers: dict[str, str] = {}
            for line in lines[1:]:
                if ":" in line:
                    name, value = line.split(":", 1)
                    headers[name.strip().lower()] = value.strip()
            body = b""
            if headers.get("transfer-encoding", "").lower() == "chunked":
                chunks: list[bytes] = []
                while True:
                    size_line = await reader.readuntil(b"\r\n")
                    size = int(size_line.split(b";", 1)[0].strip(), 16)
                    if size == 0:
                        await reader.readuntil(b"\r\n")
                        break
                    chunks.append(await reader.readexactly(size))
                    await reader.readexactly(2)
                body = b"".join(chunks)
            elif "content-length" in headers:
                length = int(headers["content-length"])
                if length < 0 or length > 4 * 1024 * 1024:
                    raise ValueError("HTTP response body length is invalid")
                body = await reader.readexactly(length)
            else:
                body = await reader.read(4 * 1024 * 1024 + 1)
                if len(body) > 4 * 1024 * 1024:
                    raise ValueError("HTTP response body is too large")
        latency_ms = int((time.perf_counter() - started) * 1000)
        text = body.decode("utf-8", errors="replace")
        status_ok = status_code == expected_status
        marker_ok = expected_marker in text
        success = status_ok and marker_ok
        outcome = (
            "http_expected_marker"
            if success
            else "http_unexpected_status"
            if not status_ok
            else "http_marker_missing"
        )
        return ProbeExecution(
            success=success,
            status="UP" if success else "DOWN",
            outcome=outcome,
            latency_ms=latency_ms,
            detail={
                "http_status": status_code,
                "expected_status": expected_status,
                "marker_found": marker_ok,
                "path": path,
            },
        )
    except TimeoutError:
        latency_ms = int((time.perf_counter() - started) * 1000)
        return ProbeExecution(
            success=False,
            status="DOWN",
            outcome="http_timeout",
            latency_ms=latency_ms,
            error_code=E_PROBE_TIMEOUT,
            detail={"timeout_ms": timeout_ms, "path": path, "connection_closed": True},
        )
    except (ConnectionError, OSError, ValueError, asyncio.IncompleteReadError) as exc:
        latency_ms = int((time.perf_counter() - started) * 1000)
        return ProbeExecution(
            success=False,
            status="DOWN",
            outcome="http_connection_error",
            latency_ms=latency_ms,
            error_code=E_PROBE_FAILED,
            detail={"error": type(exc).__name__, "message": str(exc), "path": path},
        )
    finally:
        await _close_writer(writer)


async def _tcp_probe(row: InstanceRow, timeout_ms: int) -> ProbeExecution:
    started = time.perf_counter()
    writer: asyncio.StreamWriter | None = None
    reader: asyncio.StreamReader | None = None
    parsed = urlsplit(row.endpoint)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 0
    try:
        async with asyncio.timeout(timeout_ms / 1000):
            reader, writer = await asyncio.open_connection(host, port)
        latency_ms = int((time.perf_counter() - started) * 1000)
        return ProbeExecution(
            success=True,
            status="UP",
            outcome="tcp_connected",
            latency_ms=latency_ms,
            detail={"host": host, "port": port, "socket_closed": True},
        )
    except TimeoutError:
        latency_ms = int((time.perf_counter() - started) * 1000)
        return ProbeExecution(
            success=False,
            status="DOWN",
            outcome="tcp_timeout",
            latency_ms=latency_ms,
            error_code=E_PROBE_TIMEOUT,
            detail={"timeout_ms": timeout_ms, "socket_closed": True},
        )
    except (ConnectionRefusedError, ConnectionError, OSError) as exc:
        latency_ms = int((time.perf_counter() - started) * 1000)
        return ProbeExecution(
            success=False,
            status="DOWN",
            outcome="tcp_refused" if isinstance(exc, ConnectionRefusedError) else "tcp_error",
            latency_ms=latency_ms,
            error_code=E_PROBE_FAILED,
            detail={"error": type(exc).__name__, "message": str(exc)},
        )
    finally:
        await _close_writer(writer)


def _lease_probe(row: InstanceRow) -> ProbeExecution:
    checked_at = now_ms()
    remaining_ms = row.lease_expires_at_ms - checked_at
    if remaining_ms > 0:
        return ProbeExecution(
            success=True,
            status="UP",
            outcome="lease_alive",
            latency_ms=0,
            detail={
                "lease_version": row.lease_version,
                "remaining_ms": remaining_ms,
                "last_heartbeat_at_ms": row.last_heartbeat_at_ms,
                "lease_expires_at_ms": row.lease_expires_at_ms,
            },
        )
    return ProbeExecution(
        success=False,
        status="EXPIRED",
        outcome="lease_expired",
        latency_ms=0,
        detail={
            "lease_version": row.lease_version,
            "remaining_ms": 0,
            "last_heartbeat_at_ms": row.last_heartbeat_at_ms,
            "lease_expires_at_ms": row.lease_expires_at_ms,
        },
    )


async def execute_probe(row: InstanceRow) -> ProbeExecution:
    timeout_ms = _effective_timeout_ms(row, row.probe_params)
    if row.probe_method == "http":
        return await _http_probe(row, timeout_ms)
    if row.probe_method == "tcp":
        return await _tcp_probe(row, timeout_ms)
    if row.probe_method == "lease":
        return _lease_probe(row)
    return ProbeExecution(
        success=False,
        status="DOWN",
        outcome="unsupported_probe_method",
        latency_ms=0,
        error_code=E_PROBE_FAILED,
        detail={"probe_method": row.probe_method},
    )
