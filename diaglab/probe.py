"""Real loopback TCP probes with deterministic framing and explicit failure classes."""

from __future__ import annotations

import errno
import ipaddress
import socket
import time

from .errors import bad_request
from .models import DiagnosticRecord, ProbeObservation, ProbeRequest


def validate_loopback_target(request: ProbeRequest) -> None:
    host = request.host.strip().lower()
    if host == "localhost":
        pass
    else:
        try:
            address = ipaddress.ip_address(host.strip("[]"))
        except ValueError as exc:
            raise bad_request(
                "仅允许本地回环目标（localhost、127.0.0.0/8 或 ::1）",
                host=request.host,
            ) from exc
        if not address.is_loopback:
            raise bad_request(
                "仅允许本地回环目标，禁止访问外部或未授权地址",
                host=request.host,
                resolved=str(address),
            )
    if not 1 <= request.port <= 65535:
        raise bad_request("端口必须在 1..65535", port=request.port)
    if request.protocol != "tcp":
        raise bad_request("真实探测当前仅支持 TCP", protocol=request.protocol)
    if not 50 <= request.timeout_ms <= 30_000:
        raise bad_request("timeout_ms 必须在 50..30000", timeout_ms=request.timeout_ms)
    if not 1 <= request.payload_bytes <= 65_507:
        raise bad_request("payload_bytes 必须在 1..65507", payload_bytes=request.payload_bytes)
    if not request.probe_id.strip():
        raise bad_request("probe_id 不得为空")
    if not request.path_node.strip():
        raise bad_request("path_node 不得为空")


def _record(
    request: ProbeRequest,
    *,
    timestamp_ms: int,
    success: bool,
    rtt_us: int,
    failure_type: str,
) -> DiagnosticRecord:
    return DiagnosticRecord(
        timestamp_ms=timestamp_ms,
        probe_id=request.probe_id,
        path_node=request.path_node,
        protocol=request.protocol,
        success=success,
        rtt_us=rtt_us,
        payload_bytes=request.payload_bytes,
        failure_type=failure_type,
        ordinal=0,
        source_format="socket-probe",
        source_version=2,
    )


def probe_tcp(request: ProbeRequest) -> ProbeObservation:
    """Perform one real TCP request/echo probe against a local loopback endpoint.

    The payload sent is deterministic (ASCII 'D' repeated to the requested size).
    Success requires connect + send + complete echoed payload. A hang endpoint
    therefore naturally produces E_PROBE_TIMEOUT through recv timeout and the
    socket is closed in all paths.
    """

    validate_loopback_target(request)
    host_for_socket = request.host.strip().strip("[]")
    family = socket.AF_INET6 if ":" in host_for_socket else socket.AF_INET
    payload = b"D" * request.payload_bytes
    timestamp_ms = time.time_ns() // 1_000_000
    started_ns = time.monotonic_ns()
    sock: socket.socket | None = None
    socket_closed = False
    failure_type = "error"
    outcome = "error"
    code = "E_PROBE_FAILED"
    message = "本地 TCP 探测失败"
    http_status = 502
    success = False
    rtt_us = 0

    try:
        sock = socket.socket(family, socket.SOCK_STREAM)
        sock.settimeout(request.timeout_ms / 1_000.0)
        sock.connect((host_for_socket, request.port))
        sock.sendall(payload)
        try:
            sock.shutdown(socket.SHUT_WR)
        except OSError:
            pass
        echoed = bytearray()
        while len(echoed) < len(payload):
            chunk = sock.recv(len(payload) - len(echoed))
            if not chunk:
                break
            echoed += chunk
        elapsed_us = max(1, (time.monotonic_ns() - started_ns) // 1_000)
        if bytes(echoed) == payload:
            success = True
            outcome = "success"
            code = None
            message = "连接成功并收到完整回显"
            http_status = 200
            rtt_us = elapsed_us
            failure_type = "none"
        else:
            failure_type = "reset"
            outcome = "error"
            code = "E_PROBE_PARTIAL_ECHO"
            message = "连接成功但回显不完整"
            http_status = 502
    except ConnectionRefusedError:
        outcome = "refused"
        code = "E_CONNECTION_REFUSED"
        message = "本地端点明确拒绝连接"
        failure_type = "refused"
        http_status = 502
    except (socket.timeout, TimeoutError):
        outcome = "timeout"
        code = "E_PROBE_TIMEOUT"
        message = "套接字在声明超时内未响应，已释放并关闭"
        failure_type = "timeout"
        http_status = 504
    except OSError as exc:
        failure_type = "reset" if exc.errno in {errno.ECONNRESET, errno.EPIPE} else "error"
        outcome = "error"
        code = "E_PROBE_FAILED"
        message = f"本地 TCP 探测失败：{exc.strerror or exc}"
        http_status = 502
    finally:
        if sock is not None:
            try:
                sock.close()
            finally:
                socket_closed = True

    elapsed_us = max(0, (time.monotonic_ns() - started_ns) // 1_000)
    record = _record(
        request,
        timestamp_ms=timestamp_ms,
        success=success,
        rtt_us=rtt_us,
        failure_type=failure_type,
    )
    return ProbeObservation(
        record=record,
        outcome=outcome,
        code=code,
        message=message,
        socket_closed=socket_closed,
        elapsed_us=elapsed_us,
        http_status=http_status,
    )

