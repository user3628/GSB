from __future__ import annotations

import asyncio
import json
import struct
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .config import AuthorityConfig
from .errors import BusinessError
from .protocol import (
    QTYPE_A,
    QTYPE_AAAA,
    QTYPE_CNAME,
    QTYPE_MX,
    QTYPE_NS,
    QTYPE_SOA,
    QTYPE_TXT,
    RCODE_FORMERR,
    RCODE_NOERROR,
    RCODE_NXDOMAIN,
    RCODE_SERVFAIL,
    DnsMessage,
    DnsQuestion,
    ResourceRecord,
    build_response,
    parse_presentation_name,
    parse_query_packet,
)


@dataclass(slots=True)
class AuthorityStats:
    queries: int = 0
    responses: int = 0
    dropped: int = 0
    malformed: int = 0
    active: int = 0
    by_name: dict[str, int] = field(default_factory=dict)
    by_type: dict[str, int] = field(default_factory=dict)
    by_transport: dict[str, int] = field(default_factory=dict)

    def snapshot(self) -> dict[str, Any]:
        return {
            "queries": self.queries,
            "responses": self.responses,
            "dropped": self.dropped,
            "malformed": self.malformed,
            "active": self.active,
            "by_name": dict(self.by_name),
            "by_type": dict(self.by_type),
            "by_transport": dict(self.by_transport),
        }


@dataclass(slots=True)
class ZoneData:
    zone: str
    default_ttl_ms: int
    soa: ResourceRecord
    records: dict[str, list[ResourceRecord]]
    behaviors: dict[str, dict[str, Any]] = field(default_factory=dict)
    source_path: str | None = None

    @classmethod
    def load(cls, path: str | Path) -> "ZoneData":
        source = Path(path)
        with source.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        zone_name = parse_presentation_name(data.get("zone", "."))
        default_ttl_ms = int(data.get("default_ttl", 60)) * 1000
        soa_data = dict(data.get("soa", {}))
        soa_ttl_ms = int(soa_data.get("ttl", default_ttl_ms // 1000)) * 1000
        soa = ResourceRecord(
            name=zone_name,
            rtype=QTYPE_SOA,
            qclass=1,
            ttl_ms=soa_ttl_ms,
            rdata={
                "mname": parse_presentation_name(soa_data.get("mname", "ns1." + zone_name.canonical)),
                "rname": parse_presentation_name(soa_data.get("rname", "hostmaster." + zone_name.canonical)),
                "serial": int(soa_data.get("serial", 1)),
                "refresh": int(soa_data.get("refresh", 3600)),
                "retry": int(soa_data.get("retry", 600)),
                "expire": int(soa_data.get("expire", 86400)),
                "minimum": int(soa_data.get("minimum", 30)),
            },
        )
        records: dict[str, list[ResourceRecord]] = {}
        for owner, items in data.get("records", {}).items():
            dns_name = parse_presentation_name(owner)
            bucket = records.setdefault(dns_name.canonical, [])
            for item in items:
                type_name = str(item["type"]).upper()
                ttl_ms = int(item.get("ttl", default_ttl_ms // 1000)) * 1000
                rtype = {
                    "A": QTYPE_A,
                    "AAAA": QTYPE_AAAA,
                    "CNAME": QTYPE_CNAME,
                    "NS": QTYPE_NS,
                    "MX": QTYPE_MX,
                    "TXT": QTYPE_TXT,
                    "SOA": QTYPE_SOA,
                }[type_name]
                if rtype == QTYPE_A:
                    rdata = {"address": str(item["address"])}
                elif rtype == QTYPE_AAAA:
                    rdata = {"address": str(item["address"])}
                elif rtype in (QTYPE_CNAME, QTYPE_NS):
                    rdata = {"target": parse_presentation_name(str(item["target"]))}
                elif rtype == QTYPE_MX:
                    rdata = {
                        "preference": int(item.get("preference", 10)),
                        "target": parse_presentation_name(str(item["target"])),
                    }
                elif rtype == QTYPE_TXT:
                    texts = item.get("texts")
                    if texts is None:
                        texts = [str(item.get("text", ""))]
                    rdata = {"texts": [str(text) for text in texts]}
                else:
                    rdata = {
                        "mname": parse_presentation_name(str(item["mname"])),
                        "rname": parse_presentation_name(str(item["rname"])),
                        "serial": int(item.get("serial", 1)),
                        "refresh": int(item.get("refresh", 3600)),
                        "retry": int(item.get("retry", 600)),
                        "expire": int(item.get("expire", 86400)),
                        "minimum": int(item.get("minimum", 30)),
                    }
                bucket.append(ResourceRecord(dns_name, rtype, 1, ttl_ms, rdata))
        records.setdefault(zone_name.canonical, []).append(soa)
        return cls(
            zone=zone_name.canonical,
            default_ttl_ms=default_ttl_ms,
            soa=soa,
            records=records,
            behaviors=dict(data.get("behaviors", {})),
            source_path=str(source),
        )


class AuthorityServer:
    def __init__(self, config: AuthorityConfig, zone: ZoneData) -> None:
        self.config = config
        self.zone = zone
        self.stats = AuthorityStats()
        self._transport: asyncio.DatagramTransport | None = None
        self._tcp_server: asyncio.AbstractServer | None = None
        self._tasks: set[asyncio.Task[Any]] = set()
        self._stopping = False

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_datagram_endpoint(
            lambda: _AuthorityDatagramProtocol(self),
            local_addr=(self.config.host, self.config.port),
        )
        self._transport = transport
        try:
            self._tcp_server = await asyncio.start_server(
                self._handle_tcp_client,
                host=self.config.host,
                port=self.config.port,
                limit=65535,
            )
        except Exception:
            transport.close()
            self._transport = None
            raise

    async def stop(self) -> None:
        self._stopping = True
        if self._tcp_server is not None:
            self._tcp_server.close()
            await self._tcp_server.wait_closed()
            self._tcp_server = None
        if self._transport is not None:
            self._transport.close()
            self._transport = None
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    def _track(self, task: asyncio.Task[Any]) -> None:
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _record_query(self, question: DnsQuestion, transport: str) -> dict[str, Any]:
        self.stats.queries += 1
        self.stats.by_name[question.name.canonical] = self.stats.by_name.get(question.name.canonical, 0) + 1
        type_name = question.type_name
        self.stats.by_type[type_name] = self.stats.by_type.get(type_name, 0) + 1
        self.stats.by_transport[transport] = self.stats.by_transport.get(transport, 0) + 1
        return self.zone.behaviors.get(question.name.canonical, {})

    async def answer(self, packet: bytes, transport: str) -> bytes | None:
        try:
            query = parse_query_packet(packet)
        except BusinessError:
            self.stats.malformed += 1
            return _formerr(packet)
        self.stats.active += 1
        try:
            question = query.questions[0]
            behavior = self._record_query(question, transport)
            delay_ms = int(behavior.get("delay_ms", 0))
            if delay_ms > 0:
                await asyncio.sleep(delay_ms / 1000.0)
            if behavior.get("drop") or self._stopping:
                self.stats.dropped += 1
                if behavior.get("drop"):
                    await asyncio.sleep(30)
                return None

            type_name = question.type_name
            response = self._build_answer(query, question, force_tc=bool(behavior.get("tc") and transport == "udp"))
            self.stats.responses += 1
            if transport == "udp" and len(response) > 512:
                response = build_response(query, tc=True)
            if len(response) > 65535:
                self.stats.responses -= 1
                return build_response(query, rcode=RCODE_SERVFAIL)
            return response
        finally:
            self.stats.active = max(0, self.stats.active - 1)

    def _build_answer(self, query: DnsMessage, question: DnsQuestion, *, force_tc: bool = False) -> bytes:
        name = question.name.canonical
        qtype = question.qtype
        bucket = self.zone.records.get(name)
        if force_tc:
            return build_response(query, tc=True)
        if bucket is None:
            return build_response(query, authorities=[self.zone.soa], rcode=RCODE_NXDOMAIN)

        direct = [record for record in bucket if record.rtype == qtype and record.qclass == question.qclass]
        cname_records = [record for record in bucket if record.rtype == QTYPE_CNAME]
        if qtype == QTYPE_CNAME and cname_records:
            return build_response(query, answers=cname_records, aa=True)
        if cname_records and qtype != QTYPE_CNAME:
            return build_response(query, answers=cname_records, aa=True)
        if direct:
            return build_response(query, answers=direct, aa=True)
        return build_response(query, authorities=[self.zone.soa], aa=True, rcode=RCODE_NOERROR)

    async def _handle_tcp_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._track(task)
        try:
            header = await reader.readexactly(2)
            length = struct.unpack("!H", header)[0]
            if length < 12:
                return
            packet = await reader.readexactly(length)
            response = await self.answer(packet, "tcp")
            if response is not None:
                writer.write(struct.pack("!H", len(response)) + response)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError, asyncio.CancelledError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, asyncio.CancelledError):
                pass


class _AuthorityDatagramProtocol(asyncio.DatagramProtocol):
    def __init__(self, server: AuthorityServer) -> None:
        self.server = server
        self.transport: asyncio.DatagramTransport | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        if self.transport is None:
            return
        task = asyncio.create_task(self._handle(data, addr))
        self.server._track(task)

    async def _handle(self, data: bytes, addr: tuple[str, int]) -> None:
        response = await self.server.answer(data, "udp")
        if response is not None and self.transport is not None:
            try:
                self.transport.sendto(response, addr)
            except (RuntimeError, OSError):
                pass


def _formerr(packet: bytes) -> bytes | None:
    if len(packet) < 12:
        return None
    identifier, flags = struct.unpack_from("!HH", packet, 0)
    if flags & 0x8000:
        return None
    response_flags = 0x8000 | (flags & 0x7900) | RCODE_FORMERR
    return struct.pack("!HHHHHH", identifier, response_flags, 0, 0, 0, 0)

