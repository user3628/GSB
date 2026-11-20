"""Loopback-only upstream client with per-hop deadlines and real socket teardown."""

from __future__ import annotations

import ipaddress
import random
import socket
import struct
import threading
import time
from typing import Callable

from dnslib import CLASS, EDNS0, QTYPE, DNSHeader, DNSLabel, DNSQuestion, DNSRecord

from .codec import (
    DnsFormatError,
    UpstreamResponse,
    labels_equal,
    parse_response,
)
from .errors import (
    BusinessError,
    E_UPSTREAM_MALFORMED_MESSAGE,
    E_UPSTREAM_QUESTION_MISMATCH,
    E_UPSTREAM_TIMEOUT,
    E_UPSTREAM_TRUNCATED,
)


class UpstreamTimeout(BusinessError):
    def __init__(self, detail: str = "upstream deadline expired") -> None:
        super().__init__(E_UPSTREAM_TIMEOUT, detail)


class UpstreamMalformed(BusinessError):
    def __init__(self, detail: str) -> None:
        super().__init__(E_UPSTREAM_MALFORMED_MESSAGE, detail)


class UpstreamClient:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 15353,
        *,
        timeout: float = 0.75,
        transport: str = "udp",
        max_udp_payload: int = 4096,
        require_loopback: bool = True,
        socket_factory: Callable[..., socket.socket] = socket.socket,
    ) -> None:
        if transport not in ("udp", "tcp"):
            raise ValueError("upstream transport must be 'udp' or 'tcp'")
        if require_loopback:
            self._assert_loopback(host)
        self.host = host
        self.port = int(port)
        self.timeout = float(timeout)
        self.transport = transport
        self.max_udp_payload = max(512, min(int(max_udp_payload), 65535))
        self._socket_factory = socket_factory
        self._lock = threading.Lock()
        self._active_sockets = 0
        self._sockets_opened = 0
        self._sockets_closed = 0
        self._queries = 0
        self._timeouts = 0

    @staticmethod
    def _assert_loopback(host: str) -> None:
        if host.lower() == "localhost":
            return
        try:
            address = ipaddress.ip_address(host)
        except ValueError as exc:
            raise ValueError("upstream host must be a loopback IP or localhost") from exc
        if not address.is_loopback:
            raise ValueError("upstream host must be loopback")

    def _family(self) -> int:
        if self.host.lower() == "localhost":
            return socket.AF_INET
        return socket.AF_INET6 if ipaddress.ip_address(self.host).version == 6 else socket.AF_INET

    def _build_query(self, labels: tuple[bytes, ...], qtype: int, qclass: int) -> tuple[int, bytes]:
        message_id = random.SystemRandom().randrange(0, 65536)
        question = DNSQuestion(
            DNSLabel(list(bytes(label).lower() for label in labels)), int(qtype), int(qclass)
        )
        record = DNSRecord(
            header=DNSHeader(id=message_id, qr=0, rd=1, opcode=0),
            questions=[question],
        )
        record.add_ar(EDNS0(udp_len=self.max_udp_payload, version=0))
        return message_id, record.pack()

    def query(self, labels: tuple[bytes, ...], qtype: int, qclass: int = int(CLASS.IN)) -> UpstreamResponse:
        with self._lock:
            self._queries += 1
        if self.transport == "tcp":
            return self._query_tcp(labels, qtype, qclass)
        response = self._query_udp(labels, qtype, qclass)
        if response.tc:
            return self._query_tcp(labels, qtype, qclass)
        return response

    def _socket(self, kind: int) -> socket.socket:
        sock = self._socket_factory(self._family(), kind)
        with self._lock:
            self._active_sockets += 1
            self._sockets_opened += 1
        return sock

    def _close(self, sock: socket.socket) -> None:
        try:
            sock.close()
        finally:
            with self._lock:
                self._active_sockets = max(0, self._active_sockets - 1)
                self._sockets_closed += 1

    def _timeout(self, detail: str) -> UpstreamTimeout:
        with self._lock:
            self._timeouts += 1
        return UpstreamTimeout(detail)

    def _query_udp(
        self, labels: tuple[bytes, ...], qtype: int, qclass: int
    ) -> UpstreamResponse:
        message_id, packet = self._build_query(labels, qtype, qclass)
        sock = self._socket(socket.SOCK_DGRAM)
        deadline = time.monotonic() + self.timeout
        try:
            sock.settimeout(self.timeout)
            sock.connect((self.host, self.port))
            sock.send(packet)
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise self._timeout("UDP upstream deadline expired")
                sock.settimeout(remaining)
                try:
                    data, _peer = sock.recvfrom(65535)
                except (socket.timeout, TimeoutError) as exc:
                    raise self._timeout("UDP upstream deadline expired") from exc
                if len(data) < 2 or struct.unpack_from("!H", data, 0)[0] != message_id:
                    continue
                try:
                    response = parse_response(data)
                except DnsFormatError as exc:
                    raise UpstreamMalformed(str(exc)) from exc
                self._check_question(response, labels, qtype, qclass)
                return response
        except UpstreamTimeout:
            raise
        except UpstreamMalformed:
            raise
        except (socket.timeout, TimeoutError) as exc:
            raise self._timeout("UDP upstream socket timeout") from exc
        except OSError as exc:
            raise BusinessError("E_UPSTREAM_IO", str(exc)) from exc
        finally:
            self._close(sock)

    @staticmethod
    def _check_question(
        response: UpstreamResponse,
        labels: tuple[bytes, ...],
        qtype: int,
        qclass: int,
    ) -> None:
        if len(response.questions) != 1:
            raise BusinessError(E_UPSTREAM_QUESTION_MISMATCH, "response question count is not 1")
        question = response.questions[0]
        if (
            not labels_equal(question.labels, labels)
            or question.qtype != qtype
            or question.qclass != qclass
        ):
            raise BusinessError(E_UPSTREAM_QUESTION_MISMATCH, "response question differs from query")

    def _query_tcp(
        self, labels: tuple[bytes, ...], qtype: int, qclass: int
    ) -> UpstreamResponse:
        message_id, packet = self._build_query(labels, qtype, qclass)
        sock = self._socket(socket.SOCK_STREAM)
        deadline = time.monotonic() + self.timeout
        try:
            sock.settimeout(self.timeout)
            sock.connect((self.host, self.port))
            sock.settimeout(max(0.001, deadline - time.monotonic()))
            sock.sendall(struct.pack("!H", len(packet)) + packet)
            prefix = self._recv_exact(sock, 2, deadline)
            length = struct.unpack("!H", prefix)[0]
            if length < 12:
                raise UpstreamMalformed("TCP upstream message shorter than DNS header")
            data = self._recv_exact(sock, length, deadline)
            try:
                response = parse_response(data)
            except DnsFormatError as exc:
                raise UpstreamMalformed(str(exc)) from exc
            self._check_question(response, labels, qtype, qclass)
            if response.tc:
                raise BusinessError(E_UPSTREAM_TRUNCATED, "TC set on TCP upstream response")
            return response
        except UpstreamTimeout:
            raise
        except UpstreamMalformed:
            raise
        except (socket.timeout, TimeoutError) as exc:
            raise self._timeout("TCP upstream deadline expired") from exc
        except OSError as exc:
            raise BusinessError("E_UPSTREAM_IO", str(exc)) from exc
        finally:
            self._close(sock)

    def _recv_exact(self, sock: socket.socket, length: int, deadline: float) -> bytes:
        chunks: list[bytes] = []
        remaining = length
        while remaining:
            timeout = deadline - time.monotonic()
            if timeout <= 0:
                raise self._timeout("TCP upstream deadline expired")
            sock.settimeout(timeout)
            try:
                chunk = sock.recv(remaining)
            except (socket.timeout, TimeoutError) as exc:
                raise self._timeout("TCP upstream deadline expired") from exc
            if not chunk:
                raise BusinessError("E_UPSTREAM_IO", "TCP connection closed before full response")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "queries": self._queries,
                "sockets_opened": self._sockets_opened,
                "sockets_closed": self._sockets_closed,
                "active_sockets": self._active_sockets,
                "timeouts": self._timeouts,
            }
