from __future__ import annotations

import ipaddress
import secrets
import struct
from dataclasses import dataclass, field
from typing import Any, Iterable

from .errors import HTTP_STATUS_BY_CODE, BusinessError


QTYPE_A = 1
QTYPE_NS = 2
QTYPE_CNAME = 5
QTYPE_SOA = 6
QTYPE_MX = 15
QTYPE_TXT = 16
QTYPE_AAAA = 28
QTYPE_OPT = 41

QCLASS_IN = 1

RCODE_NOERROR = 0
RCODE_FORMERR = 1
RCODE_SERVFAIL = 2
RCODE_NXDOMAIN = 3
RCODE_NOTIMP = 4

TYPE_BY_NAME = {
    "A": QTYPE_A,
    "NS": QTYPE_NS,
    "CNAME": QTYPE_CNAME,
    "SOA": QTYPE_SOA,
    "MX": QTYPE_MX,
    "TXT": QTYPE_TXT,
    "AAAA": QTYPE_AAAA,
}
NAME_BY_TYPE = {value: key for key, value in TYPE_BY_NAME.items()}
SUPPORTED_TYPES = frozenset(TYPE_BY_NAME.values())


def message_error(reason: str, **context: Any) -> BusinessError:
    return BusinessError(
        code="E_MESSAGE_INVALID",
        message=f"invalid DNS message: {reason}",
        context={"reason": reason, **context},
        http_status=HTTP_STATUS_BY_CODE["E_MESSAGE_INVALID"],
        dns_rcode=RCODE_FORMERR,
    )


def _ascii_lower(data: bytes) -> bytes:
    return data.translate(bytes.maketrans(b"ABCDEFGHIJKLMNOPQRSTUVWXYZ", b"abcdefghijklmnopqrstuvwxyz"))


def _wire_length(labels: tuple[bytes, ...]) -> int:
    return sum(1 + len(label) for label in labels) + 1


@dataclass(frozen=True, slots=True)
class DnsName:
    labels: tuple[bytes, ...] = ()

    def __post_init__(self) -> None:
        for label in self.labels:
            if not label or len(label) > 63:
                raise message_error("label length must be between 1 and 63", label_length=len(label))
        if _wire_length(self.labels) > 255:
            raise message_error("expanded name exceeds 255 octets", wire_length=_wire_length(self.labels))

    @property
    def canonical(self) -> str:
        if not self.labels:
            return "."
        parts = []
        for label in self.labels:
            lowered = _ascii_lower(label)
            chars: list[str] = []
            for byte in lowered:
                if byte in (0x2E, 0x5C):
                    chars.append("\\" + chr(byte))
                elif 0x21 <= byte <= 0x7E:
                    chars.append(chr(byte))
                else:
                    chars.append(f"\\{byte:03d}")
            parts.append("".join(chars))
        return ".".join(parts) + "."

    @property
    def display(self) -> str:
        if not self.labels:
            return "."
        parts = []
        for label in self.labels:
            chars: list[str] = []
            for byte in label:
                if byte in (0x2E, 0x5C):
                    chars.append("\\" + chr(byte))
                elif 0x21 <= byte <= 0x7E:
                    chars.append(chr(byte))
                else:
                    chars.append(f"\\{byte:03d}")
            parts.append("".join(chars))
        return ".".join(parts) + "."

    def is_subdomain_of(self, parent: DnsName) -> bool:
        if len(parent.labels) > len(self.labels):
            return False
        if not parent.labels:
            return True
        return tuple(_ascii_lower(x) for x in self.labels[-len(parent.labels) :]) == tuple(
            _ascii_lower(x) for x in parent.labels
        )

    def __str__(self) -> str:
        return self.canonical


ROOT_NAME = DnsName(())


def parse_presentation_name(text: str) -> DnsName:
    if not isinstance(text, str) or text == "":
        raise message_error("domain name must be a non-empty string")
    if text == ".":
        return ROOT_NAME

    labels: list[bytes] = []
    current = bytearray()
    index = 0
    saw_separator = False
    while index < len(text):
        char = text[index]
        if char == ".":
            if not current:
                raise message_error("empty label in domain name", name=text, offset=index)
            labels.append(bytes(current))
            current.clear()
            saw_separator = True
            index += 1
            continue
        saw_separator = False
        if char == "\\":
            if index + 1 >= len(text):
                raise message_error("trailing escape in domain name", name=text, offset=index)
            if index + 3 < len(text) and text[index + 1 : index + 4].isdigit():
                value = int(text[index + 1 : index + 4])
                if value > 255:
                    raise message_error("decimal escape exceeds 255", name=text, offset=index)
                current.append(value)
                index += 4
                continue
            value = ord(text[index + 1])
            if value > 255:
                raise message_error("escape character is not a single octet", name=text, offset=index)
            current.append(value)
            index += 2
            continue
        value = ord(char)
        if value > 255:
            raise message_error("non-octet character in presentation name", name=text, offset=index)
        current.append(value)
        index += 1

    if current:
        labels.append(bytes(current))
    elif not saw_separator:
        raise message_error("invalid empty domain name", name=text)

    return DnsName(tuple(labels))


def normalize_name(text: str) -> str:
    return parse_presentation_name(text).canonical


def _decode_name(
    data: bytes,
    offset: int,
    label_offsets: set[int] | None = None,
) -> tuple[DnsName, int]:
    labels: list[bytes] = []
    cursor = offset
    next_offset: int | None = None
    visited: set[int] = set()
    jumps = 0
    expanded_length = 0

    while True:
        if cursor >= len(data):
            raise message_error("name extends past packet end", offset=cursor, packet_length=len(data))
        first = data[cursor]
        kind = first & 0xC0
        if first == 0:
            if label_offsets is not None:
                label_offsets.add(cursor)
            expanded_length += 1
            if expanded_length > 255:
                raise message_error("expanded name exceeds 255 octets", offset=offset)
            if next_offset is None:
                next_offset = cursor + 1
            break
        if kind == 0xC0:
            if cursor + 1 >= len(data):
                raise message_error("truncated compression pointer", offset=cursor)
            target = ((first & 0x3F) << 8) | data[cursor + 1]
            if target < 12 or target >= len(data):
                raise message_error("compression pointer outside message", offset=cursor, target=target)
            if target >= cursor:
                raise message_error("compression pointer is not backwards", offset=cursor, target=target)
            if label_offsets is not None and target not in label_offsets:
                raise message_error(
                    "compression pointer does not target a prior label boundary",
                    offset=cursor,
                    target=target,
                )
            if target in visited:
                raise message_error("compression pointer loop", offset=cursor, target=target)
            if next_offset is None:
                next_offset = cursor + 2
            visited.add(target)
            jumps += 1
            if jumps > 128:
                raise message_error("too many compression pointers", offset=offset)
            cursor = target
            continue
        if kind != 0:
            raise message_error("reserved label type", offset=cursor, label_type=kind >> 6)
        length = first
        if length > 63:
            raise message_error("label exceeds 63 octets", offset=cursor, label_length=length)
        if cursor + 1 + length > len(data):
            raise message_error("truncated label", offset=cursor, label_length=length)
        label = data[cursor + 1 : cursor + 1 + length]
        if not label:
            raise message_error("zero-length non-root label", offset=cursor)
        expanded_length += 1 + length
        if expanded_length > 255:
            raise message_error("expanded name exceeds 255 octets", offset=offset)
        labels.append(label)
        if label_offsets is not None:
            label_offsets.add(cursor)
        cursor += 1 + length
        if len(labels) > 127:
            raise message_error("too many labels", offset=offset)

    return DnsName(tuple(labels)), int(next_offset)


@dataclass(slots=True)
class DnsQuestion:
    name: DnsName
    qtype: int
    qclass: int = QCLASS_IN

    @property
    def type_name(self) -> str:
        return NAME_BY_TYPE.get(self.qtype, str(self.qtype))


@dataclass(slots=True)
class ResourceRecord:
    name: DnsName
    rtype: int
    qclass: int
    ttl_ms: int
    rdata: dict[str, Any] = field(default_factory=dict)

    @property
    def type_name(self) -> str:
        return NAME_BY_TYPE.get(self.rtype, str(self.rtype))

    def with_ttl_ms(self, ttl_ms: int) -> "ResourceRecord":
        return ResourceRecord(self.name, self.rtype, self.qclass, max(0, ttl_ms), dict(self.rdata))

    def to_dict(self) -> dict[str, Any]:
        def convert(value: Any) -> Any:
            if isinstance(value, DnsName):
                return value.canonical
            if isinstance(value, list):
                return [convert(item) for item in value]
            if isinstance(value, dict):
                return {key: convert(item) for key, item in value.items()}
            return value

        return {
            "name": self.name.canonical,
            "type": self.rtype,
            "type_name": self.type_name,
            "qclass": self.qclass,
            "ttl_ms": self.ttl_ms,
            "rdata": convert(self.rdata),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ResourceRecord":
        rdata = dict(data.get("rdata", {}))

        def convert(value: Any) -> Any:
            if isinstance(value, dict):
                return {key: convert(item) for key, item in value.items()}
            if isinstance(value, list):
                return [convert(item) for item in value]
            return value

        if "target" in rdata:
            rdata["target"] = parse_presentation_name(str(rdata["target"]))
        if "mname" in rdata:
            rdata["mname"] = parse_presentation_name(str(rdata["mname"]))
        if "rname" in rdata:
            rdata["rname"] = parse_presentation_name(str(rdata["rname"]))
        return cls(
            name=parse_presentation_name(str(data["name"])),
            rtype=int(data["type"]),
            qclass=int(data.get("qclass", QCLASS_IN)),
            ttl_ms=int(data.get("ttl_ms", 0)),
            rdata=convert(rdata),
        )


@dataclass(slots=True)
class DnsMessage:
    id: int = 0
    qr: bool = False
    opcode: int = 0
    aa: bool = False
    tc: bool = False
    rd: bool = True
    ra: bool = True
    rcode: int = RCODE_NOERROR
    questions: list[DnsQuestion] = field(default_factory=list)
    answers: list[ResourceRecord] = field(default_factory=list)
    authorities: list[ResourceRecord] = field(default_factory=list)
    additionals: list[ResourceRecord] = field(default_factory=list)


class _Reader:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.pos = 0
        self.label_offsets: set[int] = set()

    def require(self, size: int, what: str) -> None:
        if self.pos + size > len(self.data):
            raise message_error(f"truncated {what}", offset=self.pos, needed=size, available=len(self.data) - self.pos)

    def u8(self, what: str = "octet") -> int:
        self.require(1, what)
        value = self.data[self.pos]
        self.pos += 1
        return value

    def u16(self, what: str = "16-bit integer") -> int:
        self.require(2, what)
        value = struct.unpack_from("!H", self.data, self.pos)[0]
        self.pos += 2
        return value

    def u32(self, what: str = "32-bit integer") -> int:
        self.require(4, what)
        value = struct.unpack_from("!I", self.data, self.pos)[0]
        self.pos += 4
        return value

    def name(self) -> DnsName:
        name, next_pos = _decode_name(self.data, self.pos, self.label_offsets)
        self.pos = next_pos
        return name


def _parse_rdata(reader: _Reader, rtype: int, rclass: int, ttl_ms: int, end: int) -> dict[str, Any]:
    rdlength = end - reader.pos
    if rtype == QTYPE_A:
        if rdlength != 4:
            raise message_error("A record must contain 4 RDATA octets", rdlength=rdlength)
        raw = reader.data[reader.pos : end]
        reader.pos = end
        return {"address": str(ipaddress.IPv4Address(raw))}
    if rtype == QTYPE_AAAA:
        if rdlength != 16:
            raise message_error("AAAA record must contain 16 RDATA octets", rdlength=rdlength)
        raw = reader.data[reader.pos : end]
        reader.pos = end
        return {"address": str(ipaddress.IPv6Address(raw))}
    if rtype in (QTYPE_CNAME, QTYPE_NS):
        target = reader.name()
        if reader.pos != end:
            raise message_error("trailing bytes in name RDATA", rtype=rtype, rdata_end=end, offset=reader.pos)
        return {"target": target}
    if rtype == QTYPE_MX:
        preference = reader.u16("MX preference")
        target = reader.name()
        if reader.pos != end:
            raise message_error("trailing bytes in MX RDATA", offset=reader.pos, rdata_end=end)
        return {"preference": preference, "target": target}
    if rtype == QTYPE_TXT:
        texts: list[str] = []
        while reader.pos < end:
            length = reader.u8("TXT string length")
            if reader.pos + length > end:
                raise message_error("TXT string exceeds RDATA", offset=reader.pos, length=length)
            raw = reader.data[reader.pos : reader.pos + length]
            texts.append(raw.decode("utf-8", errors="backslashreplace"))
            reader.pos += length
        return {"texts": texts}
    if rtype == QTYPE_SOA:
        mname = reader.name()
        rname = reader.name()
        if reader.pos + 20 != end:
            raise message_error("SOA RDATA length mismatch", offset=reader.pos, rdata_end=end)
        serial, refresh, retry, expire, minimum = struct.unpack_from("!IIIII", reader.data, reader.pos)
        reader.pos += 20
        return {
            "mname": mname,
            "rname": rname,
            "serial": serial,
            "refresh": refresh,
            "retry": retry,
            "expire": expire,
            "minimum": minimum,
        }
    raw = reader.data[reader.pos : end]
    reader.pos = end
    return {"hex": raw.hex()}


def parse_message(data: bytes) -> DnsMessage:
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise message_error("packet is not bytes")
    packet = bytes(data)
    if len(packet) < 12:
        raise message_error("packet shorter than DNS header", packet_length=len(packet))
    if len(packet) > 65535:
        raise message_error("packet exceeds DNS maximum size", packet_length=len(packet))

    identifier, flags, qdcount, ancount, nscount, arcount = struct.unpack_from("!HHHHHH", packet, 0)
    if qdcount > 64:
        raise message_error("question count is unreasonable", qdcount=qdcount)
    total_records = ancount + nscount + arcount
    if total_records > 8192:
        raise message_error("record count is unreasonable", record_count=total_records)
    if 12 + qdcount * 5 + total_records * 11 > len(packet):
        raise message_error("declared counts exceed packet length", packet_length=len(packet))

    message = DnsMessage(
        id=identifier,
        qr=bool(flags & 0x8000),
        opcode=(flags >> 11) & 0xF,
        aa=bool(flags & 0x0400),
        tc=bool(flags & 0x0200),
        rd=bool(flags & 0x0100),
        ra=bool(flags & 0x0080),
        rcode=flags & 0xF,
    )
    reader = _Reader(packet)
    reader.pos = 12
    for _ in range(qdcount):
        name = reader.name()
        qtype = reader.u16("question type")
        qclass = reader.u16("question class")
        message.questions.append(DnsQuestion(name, qtype, qclass))

    for count, section in (
        (ancount, message.answers),
        (nscount, message.authorities),
        (arcount, message.additionals),
    ):
        for _ in range(count):
            name = reader.name()
            rtype = reader.u16("record type")
            rclass = reader.u16("record class")
            ttl = reader.u32("record TTL")
            rdlength = reader.u16("RDATA length")
            rdata_end = reader.pos + rdlength
            if rdata_end > len(packet):
                raise message_error("RDATA extends past packet end", offset=reader.pos, rdlength=rdlength)
            rdata = _parse_rdata(reader, rtype, rclass, ttl * 1000, rdata_end)
            if reader.pos != rdata_end:
                raise message_error("RDATA parser did not consume exactly RDLENGTH", offset=reader.pos, rdata_end=rdata_end)
            section.append(ResourceRecord(name, rtype, rclass, ttl * 1000, rdata))

    if reader.pos != len(packet):
        raise message_error("trailing bytes after last record", offset=reader.pos, packet_length=len(packet))
    return message


def parse_query_packet(data: bytes) -> DnsMessage:
    message = parse_message(data)
    if message.qr:
        raise message_error("query has QR bit set")
    if message.opcode != 0:
        raise message_error("unsupported opcode", opcode=message.opcode)
    if message.tc:
        raise message_error("query has TC bit set")
    if message.rcode != RCODE_NOERROR:
        raise message_error("query has non-zero RCODE", rcode=message.rcode)
    if len(message.questions) != 1:
        raise message_error("exactly one question is required", qdcount=len(message.questions))
    if len(message.answers) or len(message.authorities):
        raise message_error("query contains answer or authority records")
    return message


class MessageWriter:
    def __init__(self) -> None:
        self.buffer = bytearray(12)
        self.compression: dict[tuple[bytes, ...], int] = {}
        self.qdcount = 0
        self.ancount = 0
        self.nscount = 0
        self.arcount = 0

    @property
    def pos(self) -> int:
        return len(self.buffer)

    def _write_name(self, name: DnsName, *, compress: bool = True) -> None:
        labels = name.labels
        index = 0
        while index < len(labels):
            suffix = labels[index:]
            known = self.compression.get(suffix) if compress else None
            if known is not None and known < 0x4000:
                pointer = 0xC000 | known
                self.buffer.extend(struct.pack("!H", pointer))
                return
            if self.pos < 0x4000:
                self.compression[suffix] = self.pos
            label = labels[index]
            if len(label) > 63:
                raise ValueError("label too long")
            self.buffer.append(len(label))
            self.buffer.extend(label)
            index += 1
        self.buffer.append(0)

    def add_question(self, question: DnsQuestion) -> None:
        self._write_name(question.name)
        self.buffer.extend(struct.pack("!HH", question.qtype, question.qclass))
        self.qdcount += 1

    def add_record(self, record: ResourceRecord) -> None:
        self._write_name(record.name)
        self.buffer.extend(struct.pack("!HHI", record.rtype, record.qclass, _ttl_seconds(record.ttl_ms)))
        length_pos = self.pos
        self.buffer.extend(b"\x00\x00")
        start = self.pos
        self._write_rdata(record)
        length = self.pos - start
        if length > 65535:
            raise ValueError("RDATA too large")
        struct.pack_into("!H", self.buffer, length_pos, length)

    def _write_rdata(self, record: ResourceRecord) -> None:
        rdata = record.rdata
        if record.rtype == QTYPE_A:
            self.buffer.extend(ipaddress.IPv4Address(str(rdata["address"])).packed)
        elif record.rtype == QTYPE_AAAA:
            self.buffer.extend(ipaddress.IPv6Address(str(rdata["address"])).packed)
        elif record.rtype in (QTYPE_CNAME, QTYPE_NS):
            self._write_name(_as_dns_name(rdata["target"]))
        elif record.rtype == QTYPE_MX:
            self.buffer.extend(struct.pack("!H", int(rdata.get("preference", 0))))
            self._write_name(_as_dns_name(rdata["target"]))
        elif record.rtype == QTYPE_TXT:
            for text in rdata.get("texts", []):
                raw = str(text).encode("utf-8")
                if len(raw) > 255:
                    raise ValueError("TXT string exceeds 255 octets")
                self.buffer.append(len(raw))
                self.buffer.extend(raw)
        elif record.rtype == QTYPE_SOA:
            self._write_name(_as_dns_name(rdata["mname"]))
            self._write_name(_as_dns_name(rdata["rname"]))
            self.buffer.extend(
                struct.pack(
                    "!IIIII",
                    int(rdata.get("serial", 0)),
                    int(rdata.get("refresh", 0)),
                    int(rdata.get("retry", 0)),
                    int(rdata.get("expire", 0)),
                    int(rdata.get("minimum", 0)),
                )
            )
        else:
            self.buffer.extend(bytes.fromhex(str(rdata.get("hex", ""))))

    def finish(self, message: DnsMessage, *, truncate: bool = False) -> bytes:
        flags = 0
        if message.qr:
            flags |= 0x8000
        flags |= (message.opcode & 0xF) << 11
        if message.aa:
            flags |= 0x0400
        if message.tc or truncate:
            flags |= 0x0200
        if message.rd:
            flags |= 0x0100
        if message.ra:
            flags |= 0x0080
        flags |= message.rcode & 0xF
        struct.pack_into(
            "!HHHHHH",
            self.buffer,
            0,
            message.id & 0xFFFF,
            flags,
            self.qdcount,
            self.ancount,
            self.nscount,
            self.arcount,
        )
        return bytes(self.buffer)


def _ttl_seconds(ttl_ms: int) -> int:
    return min(0x7FFFFFFF, max(0, (int(ttl_ms) + 999) // 1000))


def _as_dns_name(value: Any) -> DnsName:
    if isinstance(value, DnsName):
        return value
    return parse_presentation_name(str(value))


def serialize_message(message: DnsMessage) -> bytes:
    writer = MessageWriter()
    for question in message.questions:
        writer.add_question(question)
    for section, attribute in (
        (message.answers, "ancount"),
        (message.authorities, "nscount"),
        (message.additionals, "arcount"),
    ):
        for record in section:
            writer.add_record(record)
            setattr(writer, attribute, getattr(writer, attribute) + 1)
    return writer.finish(message)


def build_query(
    name: str | DnsName,
    qtype: int | str,
    qclass: int = QCLASS_IN,
    *,
    identifier: int | None = None,
    rd: bool = True,
) -> bytes:
    dns_name = _as_dns_name(name)
    type_code = TYPE_BY_NAME[qtype.upper()] if isinstance(qtype, str) else int(qtype)
    message = DnsMessage(
        id=secrets.randbits(16) if identifier is None else identifier & 0xFFFF,
        qr=False,
        rd=rd,
        ra=False,
        questions=[DnsQuestion(dns_name, type_code, qclass)],
    )
    return serialize_message(message)


def build_response(
    query: DnsMessage,
    *,
    answers: Iterable[ResourceRecord] = (),
    authorities: Iterable[ResourceRecord] = (),
    additionals: Iterable[ResourceRecord] = (),
    rcode: int = RCODE_NOERROR,
    aa: bool = False,
    tc: bool = False,
) -> bytes:
    response = DnsMessage(
        id=query.id,
        qr=True,
        opcode=query.opcode,
        aa=aa,
        tc=tc,
        rd=query.rd,
        ra=True,
        rcode=rcode,
        questions=list(query.questions),
        answers=list(answers),
        authorities=list(authorities),
        additionals=list(additionals),
    )
    return serialize_message(response)


def build_udp_response(
    query: DnsMessage,
    *,
    answers: Iterable[ResourceRecord] = (),
    authorities: Iterable[ResourceRecord] = (),
    rcode: int = RCODE_NOERROR,
    max_bytes: int = 1232,
) -> bytes:
    full = build_response(query, answers=answers, authorities=authorities, rcode=rcode)
    if len(full) <= max_bytes:
        return full
    return build_response(query, rcode=rcode, tc=True)


def build_minimal_error(packet: bytes, rcode: int = RCODE_FORMERR) -> bytes | None:
    if len(packet) < 12:
        return None
    identifier, flags = struct.unpack_from("!HH", packet, 0)
    if flags & 0x8000:
        return None
    response_flags = 0x8000 | (flags & 0x7900) | (rcode & 0xF)
    return struct.pack("!HHHHHH", identifier, response_flags, 0, 0, 0, 0)


def validate_response(query: DnsMessage, response: DnsMessage) -> None:
    if not response.qr:
        raise message_error("upstream packet is not a response")
    if response.id != query.id:
        raise message_error("upstream transaction id mismatch", expected=query.id, actual=response.id)
    if len(response.questions) != 1 or len(query.questions) != 1:
        raise message_error("upstream response question count mismatch")
    expected = query.questions[0]
    actual = response.questions[0]
    if actual.name.canonical != expected.name.canonical or actual.qtype != expected.qtype or actual.qclass != expected.qclass:
        raise message_error(
            "upstream response question mismatch",
            expected={"name": expected.name.canonical, "type": expected.qtype, "class": expected.qclass},
            actual={"name": actual.name.canonical, "type": actual.qtype, "class": actual.qclass},
        )


def records_from_json(items: Iterable[dict[str, Any]]) -> list[ResourceRecord]:
    return [ResourceRecord.from_dict(item) for item in items]


def records_to_json(records: Iterable[ResourceRecord]) -> list[dict[str, Any]]:
    return [record.to_dict() for record in records]

