"""Deterministic loopback authoritative DNS server used by tests and the demo."""

from __future__ import annotations

import argparse
import base64
import socket
import socketserver
import struct
import threading
import time
from collections import Counter
from dataclasses import dataclass
from typing import Callable

from dnslib import CLASS, EDNS0, QTYPE, DNSHeader, DNSLabel, DNSQuestion, DNSRecord, DNSError

from .codec import (
    RCODE_FORMERR,
    RCODE_NOERROR,
    RCODE_NXDOMAIN,
    extract_edns,
    labels_equal,
    labels_key,
    validate_wire,
)
from .model import Record, labels_from_json, labels_to_json


def L(*parts: bytes) -> tuple[bytes, ...]:
    return tuple(parts)


def rr(
    owner: tuple[bytes, ...], rtype: int, ttl: int, **data: object
) -> Record:
    return Record(owner, int(rtype), int(CLASS.IN), int(ttl), dict(data))


def txt(*strings: str) -> dict[str, object]:
    return {
        "strings": [base64.b64encode(item.encode("utf-8")).decode("ascii") for item in strings]
    }


ZONE = L(b"example", b"test")


def _base_zone() -> list[Record]:
    records = [
        rr(ZONE, QTYPE.SOA, 15, mname=labels_to_json(L(b"ns1", *ZONE)),
           rname=labels_to_json(L(b"hostmaster", *ZONE)),
           times=[2026112001, 3600, 600, 86400, 45]),
        rr(ZONE, QTYPE.NS, 300, target=labels_to_json(L(b"ns1", *ZONE))),
        rr(L(b"ns1", *ZONE), QTYPE.A, 300, address="192.0.2.53"),
        rr(L(b"www", *ZONE), QTYPE.A, 120, address="192.0.2.10"),
        rr(L(b"www", *ZONE), QTYPE.AAAA, 180, address="2001:db8::10"),
        rr(L(b"txt", *ZONE), QTYPE.TXT, 60, **txt("sample", "loopback")),
        rr(L(b"mail", *ZONE), QTYPE.MX, 90,
           preference=10, exchange=labels_to_json(L(b"mx1", *ZONE))),
        rr(L(b"mx1", *ZONE), QTYPE.A, 90, address="192.0.2.25"),
        rr(L(b"chain", *ZONE), QTYPE.CNAME, 300,
           target=labels_to_json(L(b"mid1", *ZONE))),
        rr(L(b"mid1", *ZONE), QTYPE.CNAME, 120,
           target=labels_to_json(L(b"mid2", *ZONE))),
        rr(L(b"mid2", *ZONE), QTYPE.A, 30, address="192.0.2.44"),
        rr(L(b"loop1", *ZONE), QTYPE.CNAME, 60,
           target=labels_to_json(L(b"loop2", *ZONE))),
        rr(L(b"loop2", *ZONE), QTYPE.CNAME, 60,
           target=labels_to_json(L(b"loop1", *ZONE))),
        rr(L(b"concurrent", *ZONE), QTYPE.A, 45, address="192.0.2.77"),
        rr(L(b"crash0", *ZONE), QTYPE.CNAME, 300,
           target=labels_to_json(L(b"crash1", *ZONE))),
        rr(L(b"crash1", *ZONE), QTYPE.CNAME, 120,
           target=labels_to_json(L(b"crash2", *ZONE))),
        rr(L(b"crash2", *ZONE), QTYPE.A, 30, address="192.0.2.99"),
        rr(L(b"timeout", *ZONE), QTYPE.A, 60, address="192.0.2.80"),
        rr(L(b"refused", *ZONE), QTYPE.A, 60, address="192.0.2.81"),
        rr(L(b"a.b", *ZONE), QTYPE.A, 60, address="192.0.2.82"),
    ]
    for index in range(7):
        owner = L(f"long{index}".encode(), *ZONE)
        if index < 6:
            records.append(
                rr(owner, QTYPE.CNAME, 90,
                   target=labels_to_json(L(f"long{index + 1}".encode(), *ZONE)))
            )
        else:
            records.append(rr(owner, QTYPE.A, 30, address="192.0.2.60"))
    for index in range(14):
        records.append(
            rr(
                L(b"big", *ZONE),
                QTYPE.TXT,
                300,
                **txt(f"sample-{index:02d}-" + "x" * 170),
            )
        )
    return records


@dataclass
class ZoneConfig:
    delays: dict[str, float] | None = None

    def __post_init__(self) -> None:
        if self.delays is None:
            self.delays = {
                "timeout.example.test.": 3.0,
                "concurrent.example.test.": 0.35,
                "crash2.example.test.": 6.0,
            }


class TestZone:
    def __init__(self, config: ZoneConfig | None = None) -> None:
        self.config = config or ZoneConfig()
        self.records = _base_zone()
        self._by_name: dict[tuple[bytes, ...], list[Record]] = {}
        for record in self.records:
            self._by_name.setdefault(labels_key(record.owner), []).append(record)
        self._counts: Counter[str] = Counter()
        self._lock = threading.Lock()

    def query_count(self, name: str) -> int:
        with self._lock:
            return self._counts[name.lower()]

    def total_queries(self) -> int:
        with self._lock:
            return sum(self._counts.values())

    def _count(self, labels: tuple[bytes, ...]) -> None:
        from .codec import canonical_name

        with self._lock:
            self._counts[canonical_name(labels)] += 1

    def handle(self, packet: bytes, transport: str = "udp") -> bytes:
        _, questions = validate_wire(packet)
        if len(questions) != 1:
            return self._error(packet, RCODE_FORMERR)
        question = questions[0]
        self._count(question.labels)
        delay = float(self.config.delays.get(_lower_name(question.labels), 0.0))
        if delay:
            time.sleep(delay)

        if _lower_name(question.labels) == "malformed.example.test.":
            return self._malformed_response(packet)
        try:
            request = DNSRecord.parse(packet)
        except DNSError:
            return self._error(packet, RCODE_FORMERR)
        try:
            edns, payload, do, _ = extract_edns(request)
        except Exception:
            edns, payload, do = False, 512, 0

        lower = labels_key(question.labels)
        records = self._by_name.get(lower, [])
        rcode = RCODE_NOERROR
        answers: list[Record] = []
        authority: list[Record] = []

        if lower == labels_key(L(b"refused", *ZONE)):
            rcode = 5
        elif not records:
            rcode = RCODE_NXDOMAIN
            authority = [record for record in self._by_name[labels_key(ZONE)] if record.rtype == QTYPE.SOA]
        else:
            if question.qtype == QTYPE.ANY:
                answers = list(records)
            else:
                answers = [record for record in records if record.rtype == question.qtype]
                cname = next((record for record in records if record.rtype == QTYPE.CNAME), None)
                if not answers and cname is not None and question.qtype != QTYPE.CNAME:
                    answers = [cname]
                if not answers and cname is None:
                    authority = [
                        record
                        for record in self._by_name[labels_key(ZONE)]
                        if record.rtype == QTYPE.SOA
                    ]

        response = DNSRecord(
            header=DNSHeader(
                id=int(request.header.id),
                qr=1,
                opcode=0,
                aa=1,
                rd=int(request.header.rd),
                ra=0,
                rcode=rcode,
            ),
            questions=[
                DNSQuestion(
                    DNSLabel(list(question.labels)), question.qtype, question.qclass
                )
            ],
        )
        if answers:
            response.add_answer(*(record.to_rr() for record in answers))
        if authority:
            response.add_auth(*(record.to_rr() for record in authority))
        if edns:
            response.add_ar(EDNS0(udp_len=payload, flags="do" if do else ""))
        packed = response.pack()
        limit = payload if edns else 512
        if transport == "udp" and len(packed) > limit:
            truncated = DNSRecord(
                header=DNSHeader(
                    id=int(request.header.id),
                    qr=1,
                    aa=1,
                    tc=1,
                    rd=int(request.header.rd),
                    ra=0,
                    rcode=rcode,
                ),
                questions=response.questions,
            )
            if edns:
                truncated.add_ar(EDNS0(udp_len=payload, flags="do" if do else ""))
            packed = truncated.pack()
        return packed

    @staticmethod
    def _error(packet: bytes, rcode: int) -> bytes:
        message_id = struct.unpack_from("!H", packet, 0)[0] if len(packet) >= 2 else 0
        return DNSRecord(header=DNSHeader(id=message_id, qr=1, ra=0, rcode=rcode)).pack()

    @staticmethod
    def _malformed_response(packet: bytes) -> bytes:
        message_id = struct.unpack_from("!H", packet, 0)[0]
        # A question name that points at itself is an illegal compression pointer.
        return struct.pack("!HHHHHH", message_id, 0x8180, 1, 0, 0, 0) + b"\xc0\x0c\x00\x01\x00\x01"


def _lower_name(labels: tuple[bytes, ...]) -> str:
    from .codec import canonical_name

    return canonical_name(labels)


class _ThreadingUDPServer(socketserver.ThreadingMixIn, socketserver.UDPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], zone: TestZone) -> None:
        self.zone = zone
        super().__init__(address, _UDPHandler)


class _UDPHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        data, sock = self.request
        try:
            response = self.server.zone.handle(bytes(data), "udp")  # type: ignore[attr-defined]
        except Exception:
            response = TestZone._error(bytes(data), RCODE_FORMERR)
        if response:
            try:
                sock.sendto(response, self.client_address)
            except OSError:
                return


class _ThreadingTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], zone: TestZone) -> None:
        self.zone = zone
        super().__init__(address, _TCPHandler)


class _TCPHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        sock: socket.socket = self.request
        sock.settimeout(5.0)
        try:
            prefix = _recv_exact(sock, 2)
            if prefix is None:
                return
            length = struct.unpack("!H", prefix)[0]
            data = _recv_exact(sock, length)
            if data is None:
                return
            response = self.server.zone.handle(data, "tcp")  # type: ignore[attr-defined]
            sock.sendall(struct.pack("!H", len(response)) + response)
        except (OSError, ValueError):
            return


def _recv_exact(sock: socket.socket, length: int) -> bytes | None:
    chunks: list[bytes] = []
    remaining = length
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            return None
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


class AuthoritativeTestServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 15353, zone: TestZone | None = None) -> None:
        self.host = host
        self.zone = zone or TestZone()
        self.udp = _ThreadingUDPServer((host, int(port)), self.zone)
        actual_port = int(self.udp.server_address[1])
        try:
            self.tcp = _ThreadingTCPServer((host, actual_port), self.zone)
        except OSError:
            self.udp.server_close()
            raise
        self.port = actual_port
        self._threads: list[threading.Thread] = []

    def start(self) -> "AuthoritativeTestServer":
        for server in (self.udp, self.tcp):
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self._threads.append(thread)
        return self

    def stop(self) -> None:
        for server in (self.udp, self.tcp):
            server.shutdown()
            server.server_close()
        for thread in self._threads:
            thread.join(timeout=2.0)

    def __enter__(self) -> "AuthoritativeTestServer":
        return self.start()

    def __exit__(self, *_: object) -> None:
        self.stop()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Loopback deterministic authoritative DNS test server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=15353)
    args = parser.parse_args(argv)
    server = AuthoritativeTestServer(args.host, args.port).start()
    print(f"authoritative test server listening on {args.host}:{server.port} UDP+TCP", flush=True)
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
