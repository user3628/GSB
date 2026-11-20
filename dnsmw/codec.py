"""Strict DNS wire validation plus semantic response re-encoding."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Iterable, Sequence

from dnslib import (
    CLASS,
    EDNS0,
    EDNSOption,
    QTYPE,
    DNSHeader,
    DNSLabel,
    DNSQuestion,
    DNSRecord,
    DNSError,
    RR,
)

from .errors import (
    DnsFormatError,
    E_BAD_EDNS_VERSION,
    E_UNSUPPORTED_OPCODE,
    E_UNSUPPORTED_QCLASS,
    E_UNSUPPORTED_QTYPE,
    BusinessError,
)
from .model import Record, Resolution, SUPPORTED_QTYPES, TYPE_NAMES


IN_CLASS = int(CLASS.IN)
OPT_TYPE = int(QTYPE.OPT)
MAX_DNS_MESSAGE = 65535
LEGACY_UDP_SIZE = 512
BUSINESS_CODE_OPTION = 65001
RCODE_NOERROR = 0
RCODE_FORMERR = 1
RCODE_SERVFAIL = 2
RCODE_NXDOMAIN = 3
RCODE_NOTIMP = 4
RCODE_BADVERS = 16


def labels_key(labels: Sequence[bytes]) -> tuple[bytes, ...]:
    return tuple(bytes(label).lower() for label in labels)


def labels_equal(left: Sequence[bytes], right: Sequence[bytes]) -> bool:
    return labels_key(left) == labels_key(right)


def _escape_label(label: bytes) -> str:
    parts: list[str] = []
    for byte in label:
        if byte == 0x2E:
            parts.append("\\.")
        elif byte == 0x5C:
            parts.append("\\\\")
        elif 0x21 <= byte <= 0x7E:
            parts.append(chr(byte))
        else:
            parts.append(f"\\{byte:03d}")
    return "".join(parts)


def canonical_name(labels: Sequence[bytes]) -> str:
    """Lowercase, fully qualify and preserve escaped labels for the cache key."""

    if not labels:
        return "."
    return ".".join(_escape_label(bytes(label).lower()) for label in labels) + "."


@dataclass(frozen=True)
class QueryKey:
    name: str
    qtype: int
    qclass: int

    @classmethod
    def from_labels(cls, labels: Sequence[bytes], qtype: int, qclass: int) -> "QueryKey":
        return cls(canonical_name(labels), int(qtype), int(qclass))

    def as_tuple(self) -> tuple[str, int, int]:
        return (self.name, self.qtype, self.qclass)


@dataclass(frozen=True)
class WireQuestion:
    labels: tuple[bytes, ...]
    qtype: int
    qclass: int


@dataclass(frozen=True)
class ParsedRequest:
    message_id: int
    labels: tuple[bytes, ...]
    qtype: int
    qclass: int
    opcode: int
    rd: int
    cd: int
    edns: bool
    udp_payload: int
    do: int
    packet: bytes

    @property
    def key(self) -> QueryKey:
        return QueryKey.from_labels(self.labels, self.qtype, self.qclass)


@dataclass(frozen=True)
class UpstreamResponse:
    message_id: int
    rcode: int
    aa: int
    tc: int
    questions: tuple[WireQuestion, ...]
    answers: tuple[Record, ...]
    authority: tuple[Record, ...]
    additional: tuple[Record, ...]
    raw: bytes


def decode_name(packet: bytes, offset: int) -> tuple[tuple[bytes, ...], int]:
    """Decode one DNS name, rejecting forward/self pointers, loops and bad labels."""

    if offset < 0 or offset >= len(packet):
        raise DnsFormatError(f"name offset {offset} outside packet")
    labels: list[bytes] = []
    cursor = offset
    next_offset: int | None = None
    visited: set[int] = set()
    expanded_length = 1  # root label
    jumps = 0

    while True:
        if cursor >= len(packet):
            raise DnsFormatError("truncated domain name")
        length = packet[cursor]
        tag = length & 0xC0
        if tag == 0xC0:
            if cursor + 1 >= len(packet):
                raise DnsFormatError("truncated compression pointer")
            pointer = ((length & 0x3F) << 8) | packet[cursor + 1]
            if pointer < 12 or pointer >= len(packet):
                raise DnsFormatError(f"illegal compression pointer {pointer}")
            if pointer >= cursor:
                raise DnsFormatError(f"forward/self compression pointer {pointer}")
            if pointer in visited:
                raise DnsFormatError("compression pointer loop")
            if next_offset is None:
                next_offset = cursor + 2
            visited.add(cursor)
            jumps += 1
            if jumps > 128:
                raise DnsFormatError("too many compression pointer jumps")
            cursor = pointer
            continue
        if tag:
            raise DnsFormatError(f"reserved label encoding 0x{length:02x}")
        cursor += 1
        if length == 0:
            if next_offset is None:
                next_offset = cursor
            return tuple(labels), next_offset
        if length > 63:
            raise DnsFormatError(f"oversized label {length}")
        if cursor + length > len(packet):
            raise DnsFormatError("truncated label")
        label = bytes(packet[cursor : cursor + length])
        labels.append(label)
        expanded_length += 1 + length
        if expanded_length > 255:
            raise DnsFormatError("expanded domain name exceeds 255 octets")
        cursor += length


def _validate_rdata(packet: bytes, start: int, end: int, rtype: int) -> None:
    length = end - start
    if rtype == QTYPE.A and length != 4:
        raise DnsFormatError("A RDATA length must be 4")
    if rtype == QTYPE.AAAA and length != 16:
        raise DnsFormatError("AAAA RDATA length must be 16")
    if rtype in (QTYPE.CNAME, QTYPE.NS):
        _, consumed = decode_name(packet, start)
        if consumed != end:
            raise DnsFormatError("trailing bytes after compressed DNS name")
        return
    if rtype == QTYPE.MX:
        if length < 3:
            raise DnsFormatError("MX RDATA too short")
        _, consumed = decode_name(packet, start + 2)
        if consumed != end:
            raise DnsFormatError("trailing bytes after MX exchange")
        return
    if rtype == QTYPE.SOA:
        _, cursor = decode_name(packet, start)
        _, cursor = decode_name(packet, cursor)
        if cursor + 20 != end:
            raise DnsFormatError("SOA RDATA length mismatch")
        return
    if rtype == QTYPE.TXT:
        cursor = start
        while cursor < end:
            text_length = packet[cursor]
            cursor += 1
            if cursor + text_length > end:
                raise DnsFormatError("TXT character-string exceeds RDATA")
            cursor += text_length
        if cursor != end:
            raise DnsFormatError("TXT RDATA length mismatch")
        return
    if rtype == OPT_TYPE:
        cursor = start
        while cursor < end:
            if cursor + 4 > end:
                raise DnsFormatError("truncated EDNS option header")
            option_length = struct.unpack_from("!H", packet, cursor + 2)[0]
            cursor += 4
            if cursor + option_length > end:
                raise DnsFormatError("truncated EDNS option data")
            cursor += option_length
        if cursor != end:
            raise DnsFormatError("EDNS option length mismatch")


def validate_wire(packet: bytes) -> tuple[tuple[int, int, int, int, int, int], tuple[WireQuestion, ...]]:
    """Validate every section before dnslib gets a chance to normalize errors."""

    if len(packet) < 12:
        raise DnsFormatError("DNS header shorter than 12 octets")
    if len(packet) > MAX_DNS_MESSAGE:
        raise DnsFormatError("DNS message exceeds 65535 octets")
    header = struct.unpack_from("!HHHHHH", packet, 0)
    _, _, qdcount, ancount, nscount, arcount = header
    offset = 12
    questions: list[WireQuestion] = []
    for _ in range(qdcount):
        labels, offset = decode_name(packet, offset)
        if offset + 4 > len(packet):
            raise DnsFormatError("truncated question")
        qtype, qclass = struct.unpack_from("!HH", packet, offset)
        offset += 4
        questions.append(WireQuestion(labels, qtype, qclass))

    opt_records = 0
    for _ in range(ancount + nscount + arcount):
        owner, offset = decode_name(packet, offset)
        if offset + 10 > len(packet):
            raise DnsFormatError("truncated resource-record header")
        rtype, _rclass, _ttl, rdlength = struct.unpack_from("!HHIH", packet, offset)
        offset += 10
        end = offset + rdlength
        if end > len(packet):
            raise DnsFormatError("resource record exceeds packet")
        if rtype == OPT_TYPE:
            opt_records += 1
            if owner:
                raise DnsFormatError("OPT owner name must be root")
        _validate_rdata(packet, offset, end, rtype)
        offset = end
    if opt_records > 1:
        raise DnsFormatError("multiple OPT pseudo-records")
    if offset != len(packet):
        raise DnsFormatError("trailing bytes after DNS message")
    return header, tuple(questions)


def extract_edns(record: DNSRecord) -> tuple[bool, int, int, int]:
    options = [rr for rr in record.ar if int(rr.rtype) == OPT_TYPE]
    if len(options) > 1:
        raise DnsFormatError("multiple OPT pseudo-records")
    if not options:
        return False, LEGACY_UDP_SIZE, 0, 0
    opt = options[0]
    if tuple(opt.rname.label):
        raise DnsFormatError("OPT owner name must be root")
    version = (int(opt.ttl) >> 16) & 0xFF
    if version != 0:
        raise BusinessError(E_BAD_EDNS_VERSION, f"EDNS version {version}")
    payload = max(LEGACY_UDP_SIZE, min(int(opt.rclass), MAX_DNS_MESSAGE))
    do = 1 if (int(opt.ttl) & 0x8000) else 0
    ext_rcode = (int(opt.ttl) >> 24) & 0xFF
    return True, payload, do, ext_rcode


def parse_request(packet: bytes) -> ParsedRequest:
    packet = bytes(packet)
    _, questions = validate_wire(packet)
    try:
        record = DNSRecord.parse(packet)
    except (DNSError, ValueError, IndexError) as exc:
        raise DnsFormatError(f"dnslib parse failed: {exc}") from exc
    if record.header.qr != 0:
        raise DnsFormatError("client packet has QR=1")
    if len(questions) != 1:
        raise DnsFormatError("exactly one question is required")
    question = questions[0]
    if record.header.opcode != 0:
        raise BusinessError(E_UNSUPPORTED_OPCODE, f"opcode {record.header.opcode}")
    if question.qclass != IN_CLASS:
        raise BusinessError(E_UNSUPPORTED_QCLASS, f"class {question.qclass}")
    if question.qtype not in SUPPORTED_QTYPES:
        type_name = TYPE_NAMES.get(question.qtype, str(question.qtype))
        raise BusinessError(E_UNSUPPORTED_QTYPE, type_name)
    edns, payload, do, _ = extract_edns(record)
    return ParsedRequest(
        message_id=int(record.header.id),
        labels=question.labels,
        qtype=question.qtype,
        qclass=question.qclass,
        opcode=int(record.header.opcode),
        rd=int(record.header.rd),
        cd=int(record.header.cd),
        edns=edns,
        udp_payload=payload,
        do=do,
        packet=packet,
    )


def _records_from_section(section: Iterable[RR]) -> tuple[Record, ...]:
    records: list[Record] = []
    for rr in section:
        if int(rr.rtype) == OPT_TYPE:
            continue
        if int(rr.rtype) not in TYPE_NAMES:
            continue
        records.append(Record.from_rr(rr))
    return tuple(records)


def parse_response(packet: bytes) -> UpstreamResponse:
    packet = bytes(packet)
    _, questions = validate_wire(packet)
    try:
        record = DNSRecord.parse(packet)
    except (DNSError, ValueError, IndexError) as exc:
        raise DnsFormatError(f"dnslib response parse failed: {exc}") from exc
    if record.header.qr != 1:
        raise DnsFormatError("upstream packet has QR=0")
    return UpstreamResponse(
        message_id=int(record.header.id),
        rcode=int(record.header.rcode),
        aa=int(record.header.aa),
        tc=int(record.header.tc),
        questions=questions,
        answers=_records_from_section(record.rr),
        authority=_records_from_section(record.auth),
        additional=_records_from_section(record.ar),
        raw=packet,
    )


def _business_opt(request: ParsedRequest, code: str | None) -> EDNS0 | None:
    if not request.edns:
        return None
    options = [EDNSOption(BUSINESS_CODE_OPTION, code.encode("ascii"))] if code else []
    return EDNS0(
        udp_len=request.udp_payload,
        version=0,
        flags="do" if request.do else "",
        opts=options,
    )


def _build_record(
    request: ParsedRequest,
    resolution: Resolution,
    *,
    tc: bool,
    ext_rcode: int,
) -> DNSRecord:
    header = DNSHeader(
        id=request.message_id,
        qr=1,
        opcode=request.opcode,
        aa=0,
        tc=1 if tc else 0,
        rd=request.rd,
        ra=1,
        rcode=(resolution.rcode & 0x0F),
    )
    reply = DNSRecord(
        header=header,
        questions=[DNSQuestion(DNSLabel(list(request.labels)), request.qtype, request.qclass)],
    )
    if not tc:
        if resolution.answers:
            reply.add_answer(*(record.to_rr() for record in resolution.answers))
        if resolution.authority:
            reply.add_auth(*(record.to_rr() for record in resolution.authority))
    opt = _business_opt(request, resolution.business_code)
    if opt is not None:
        if ext_rcode:
            opt.edns_rcode = ext_rcode
        reply.add_ar(opt)
    reply.set_header_qa()
    return reply


def render_response(
    request: ParsedRequest,
    resolution: Resolution,
    *,
    transport: str,
    udp_limit: int | None = None,
) -> tuple[bytes, bool]:
    """Re-encode a semantic result for this client and protocol.

    A truncated UDP answer deliberately carries no partial RRsets. The question
    and OPT are retained; the client retries over TCP and receives the full
    semantic result. Cached wire packets are never replayed.
    """

    limit = MAX_DNS_MESSAGE
    if transport == "udp":
        requested = request.udp_payload if request.edns else LEGACY_UDP_SIZE
        limit = max(LEGACY_UDP_SIZE, min(int(udp_limit or requested), MAX_DNS_MESSAGE))
    full = _build_record(request, resolution, tc=False, ext_rcode=0).pack()
    if transport == "udp" and len(full) > limit:
        truncated = _build_record(request, resolution, tc=True, ext_rcode=0).pack()
        if len(truncated) > limit:
            # The question plus OPT should always fit; retain OPT and drop the
            # question only for an extraordinary 512-byte edge case.
            minimal = DNSRecord(
                header=DNSHeader(
                    id=request.message_id,
                    qr=1,
                    aa=0,
                    tc=1,
                    rd=request.rd,
                    ra=1,
                    rcode=(resolution.rcode & 0x0F),
                )
            )
            opt = _business_opt(request, resolution.business_code)
            if opt is not None:
                minimal.add_ar(opt)
            truncated = minimal.pack()
        return truncated, True
    if len(full) > MAX_DNS_MESSAGE:
        raise BusinessError("E_RESPONSE_TOO_LARGE", f"encoded response is {len(full)} bytes")
    return full, False


def business_code_from_response(packet: bytes) -> str | None:
    try:
        record = DNSRecord.parse(packet)
    except Exception:
        return None
    for rr in record.ar:
        if int(rr.rtype) != OPT_TYPE:
            continue
        for option in rr.rdata:
            if int(option.code) == BUSINESS_CODE_OPTION:
                try:
                    return bytes(option.data).decode("ascii")
                except UnicodeDecodeError:
                    return None
    return None
