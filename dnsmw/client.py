"""Small DNS test client used by the demo and integration tests."""

from __future__ import annotations

import socket
import struct
import time
from dataclasses import dataclass

from dnslib import CLASS, EDNS0, QTYPE, DNSHeader, DNSLabel, DNSQuestion, DNSRecord

from .codec import business_code_from_response, canonical_name


@dataclass(frozen=True)
class ClientResult:
    response: DNSRecord
    wire: bytes
    transport: str
    elapsed: float
    initial_udp_truncated: bool = False
    udp_wire_size: int | None = None
    business_code: str | None = None


def _qtype(value: int | str) -> int:
    if isinstance(value, int):
        return value
    return int(getattr(QTYPE, value.upper()))


def make_query(
    name: str | None = None,
    qtype: int | str = QTYPE.A,
    *,
    labels: tuple[bytes, ...] | None = None,
    edns_payload: int | None = None,
    do: bool = False,
    qclass: int = int(CLASS.IN),
) -> DNSRecord:
    if labels is None:
        if name is None:
            raise ValueError("name or labels is required")
        qname = DNSLabel(name)
    else:
        qname = DNSLabel(list(labels))
    record = DNSRecord(
        header=DNSHeader(id=time.time_ns() & 0xFFFF, qr=0, rd=1),
        questions=[DNSQuestion(qname, _qtype(qtype), int(qclass))],
    )
    if edns_payload is not None:
        record.add_ar(
            EDNS0(udp_len=max(512, min(int(edns_payload), 65535)), flags="do" if do else "")
        )
    return record


def _recv_exact(sock: socket.socket, length: int) -> bytes:
    chunks: list[bytes] = []
    remaining = length
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError("DNS TCP peer closed early")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def query_udp(
    host: str,
    port: int,
    request: DNSRecord,
    *,
    timeout: float = 2.0,
) -> bytes:
    packet = request.pack()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)
    try:
        sock.sendto(packet, (host, int(port)))
        data, _ = sock.recvfrom(65535)
        return data
    finally:
        sock.close()


def query_tcp(
    host: str,
    port: int,
    request: DNSRecord,
    *,
    timeout: float = 2.0,
) -> bytes:
    packet = request.pack()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((host, int(port)))
        sock.sendall(struct.pack("!H", len(packet)) + packet)
        prefix = _recv_exact(sock, 2)
        length = struct.unpack("!H", prefix)[0]
        return _recv_exact(sock, length)
    finally:
        sock.close()


def ask(
    host: str,
    port: int,
    name: str | None = None,
    qtype: int | str = QTYPE.A,
    *,
    labels: tuple[bytes, ...] | None = None,
    edns_payload: int | None = None,
    tcp: bool = False,
    retry_tcp: bool = True,
    do: bool = False,
    timeout: float = 2.0,
) -> ClientResult:
    request = make_query(
        name, qtype, labels=labels, edns_payload=edns_payload, do=do
    )
    start = time.monotonic()
    if tcp:
        wire = query_tcp(host, port, request, timeout=timeout)
        response = DNSRecord.parse(wire)
        return ClientResult(
            response=response,
            wire=wire,
            transport="tcp",
            elapsed=time.monotonic() - start,
            business_code=business_code_from_response(wire),
        )
    udp_wire = query_udp(host, port, request, timeout=timeout)
    udp_response = DNSRecord.parse(udp_wire)
    if retry_tcp and udp_response.header.tc:
        wire = query_tcp(host, port, request, timeout=timeout)
        response = DNSRecord.parse(wire)
        return ClientResult(
            response=response,
            wire=wire,
            transport="tcp",
            elapsed=time.monotonic() - start,
            initial_udp_truncated=True,
            udp_wire_size=len(udp_wire),
            business_code=business_code_from_response(wire),
        )
    return ClientResult(
        response=udp_response,
        wire=udp_wire,
        transport="udp",
        elapsed=time.monotonic() - start,
        initial_udp_truncated=bool(udp_response.header.tc),
        udp_wire_size=len(udp_wire),
        business_code=business_code_from_response(udp_wire),
    )


def display_name(labels: tuple[bytes, ...]) -> str:
    return canonical_name(labels)
