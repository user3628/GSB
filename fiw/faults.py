"""Real fault execution against local HTTP test endpoints.

Every verdict is produced by a genuine socket interaction:
  * connection_refused  -> real connect() to a port expected to be unlistened
  * connection_timeout  -> real connect()+request to a socket that never
                           responds; the socket is always closed afterwards
  * corrupt_body        -> real HTTP GET whose body must fail JSON parsing

No fixed sleeps are used to simulate timeouts.
"""

from __future__ import annotations

import http.client
import json
import socket
import time
from dataclasses import dataclass, field

from .errors import E_FAULT_UNSUPPORTED, E_INJECTION_TIMEOUT
from .models import (
    FAULT_CONNECTION_REFUSED,
    FAULT_CONNECTION_TIMEOUT,
    FAULT_CORRUPT_BODY,
    OUTCOME_INJECTED,
    OUTCOME_TIMEOUT,
    OUTCOME_UNSUPPORTED,
)


@dataclass
class FaultVerdict:
    outcome: str
    code: str | None = None
    reason: str | None = None
    elapsed_ms: float = 0.0
    evidence: dict = field(default_factory=dict)

    def to_dict(self):
        return {
            "outcome": self.outcome,
            "code": self.code,
            "reason": self.reason,
            "elapsed_ms": round(self.elapsed_ms, 3),
            "evidence": self.evidence,
        }


def _unsupported(reason, started, **evidence):
    return FaultVerdict(
        outcome=OUTCOME_UNSUPPORTED, code=E_FAULT_UNSUPPORTED, reason=reason,
        elapsed_ms=(time.monotonic() - started) * 1000.0, evidence=evidence,
    )


def execute_refused(host, port, timeout_ms):
    """Confirm the fault by a real connect() that must be refused."""
    started = time.monotonic()
    sock = None
    try:
        sock = socket.create_connection((host, port), timeout=timeout_ms / 1000.0)
    except ConnectionRefusedError as exc:
        return FaultVerdict(
            outcome=OUTCOME_INJECTED,
            elapsed_ms=(time.monotonic() - started) * 1000.0,
            evidence={"syscall": "connect", "result": "ECONNREFUSED", "errno": exc.errno},
        )
    except socket.timeout:
        return _unsupported("port does not refuse connections (filtered/blackhole)",
                            started, syscall="connect", result="timeout")
    except OSError as exc:
        return _unsupported(f"connect failed unexpectedly: {exc}", started,
                            syscall="connect", result=type(exc).__name__)
    if sock is not None:
        sock.close()
    return _unsupported("port is listening; connection_refused cannot take effect",
                        started, syscall="connect", result="established")


def execute_timeout(host, port, timeout_ms):
    """Wait for a response that never comes; always release the socket."""
    started = time.monotonic()
    sock = None
    try:
        try:
            sock = socket.create_connection((host, port), timeout=timeout_ms / 1000.0)
        except ConnectionRefusedError:
            return _unsupported("port is not listening; connection_timeout requires a "
                                "real non-responding socket", started,
                                syscall="connect", result="ECONNREFUSED")
        sock.settimeout(timeout_ms / 1000.0)
        sock.sendall(b"GET /data HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        try:
            data = sock.recv(1)
        except socket.timeout:
            return FaultVerdict(
                outcome=OUTCOME_TIMEOUT, code=E_INJECTION_TIMEOUT,
                reason="endpoint accepted the connection but never responded; "
                       "socket closed after timeout",
                elapsed_ms=(time.monotonic() - started) * 1000.0,
                evidence={"syscall": "recv", "result": "timeout",
                          "timeout_ms": timeout_ms, "socket_closed": True},
            )
        if data:
            return _unsupported("endpoint responded; connection_timeout cannot take effect",
                                started, syscall="recv", result="data")
        return _unsupported("endpoint closed the connection without responding",
                            started, syscall="recv", result="eof")
    finally:
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()


def _http_get_body(host, port, path, timeout_ms, headers=None):
    conn = http.client.HTTPConnection(host, port, timeout=timeout_ms / 1000.0)
    try:
        conn.request("GET", path, headers=headers or {})
        resp = conn.getresponse()
        return resp.status, resp.read()
    finally:
        conn.close()


def execute_corrupt_body(host, port, timeout_ms):
    """Baseline body must parse; the fault-armed body must fail parsing."""
    started = time.monotonic()
    try:
        status, body = _http_get_body(host, port, "/data", timeout_ms)
    except (OSError, http.client.HTTPException) as exc:
        return _unsupported(f"endpoint does not return a parseable response: {exc}",
                            started, phase="baseline")
    try:
        json.loads(body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return _unsupported("baseline response body is not parseable; corrupt_body "
                            "requires an endpoint that normally returns parseable data",
                            started, phase="baseline", status=status)
    try:
        _, corrupted = _http_get_body(host, port, "/data", timeout_ms,
                                      headers={"X-Fault": "corrupt-body"})
    except (OSError, http.client.HTTPException) as exc:
        return _unsupported(f"fault-armed request failed at transport level: {exc}",
                            started, phase="injected")
    try:
        json.loads(corrupted.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        return FaultVerdict(
            outcome=OUTCOME_INJECTED,
            elapsed_ms=(time.monotonic() - started) * 1000.0,
            evidence={"phase": "injected", "parse_error": str(exc),
                      "body_prefix": corrupted[:40].decode("utf-8", "replace")},
        )
    return _unsupported("endpoint did not honor the corrupt-body fault "
                        "(response still parses)", started, phase="injected")


_EXECUTORS = {
    FAULT_CONNECTION_REFUSED: execute_refused,
    FAULT_CONNECTION_TIMEOUT: execute_timeout,
    FAULT_CORRUPT_BODY: execute_corrupt_body,
}


def execute_fault(endpoint, fault_type, timeout_ms) -> FaultVerdict:
    return _EXECUTORS[fault_type](endpoint.host, endpoint.port, timeout_ms)
