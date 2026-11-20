from __future__ import annotations

import struct

import pytest
from dnslib import A, AAAA, CNAME, MX, NS, QTYPE, SOA, TXT, DNSHeader, DNSLabel, DNSQuestion, DNSRecord, RR

from dnsmw.codec import (
    QueryKey,
    RCODE_NOERROR,
    business_code_from_response,
    canonical_name,
    decode_name,
    parse_request,
    parse_response,
    render_response,
)
from dnsmw.errors import DnsFormatError, E_MALFORMED_MESSAGE
from dnsmw.model import Record, Resolution


def _query_wire(qname_wire: bytes, qtype: int = 1, qclass: int = 1) -> bytes:
    return struct.pack("!HHHHHH", 0x1234, 0x0100, 1, 0, 0, 0) + qname_wire + struct.pack(
        "!HH", qtype, qclass
    )


def _assert_malformed(packet: bytes) -> None:
    with pytest.raises(DnsFormatError) as caught:
        parse_request(packet)
    assert caught.value.code == E_MALFORMED_MESSAGE


def test_canonical_cache_key_is_case_insensitive_and_preserves_escaped_labels():
    mixed = QueryKey.from_labels((b"MiXeD", b"ExAmPlE", b"TeSt"), QTYPE.A, 1)
    lower = QueryKey.from_labels((b"mixed", b"example", b"test"), QTYPE.A, 1)
    assert mixed == lower
    assert canonical_name((b"a.b", b"back\\slash", b"with space")) == (
        "a\\.b.back\\\\slash.with\\032space."
    )
    assert canonical_name(()) == "."


def test_compression_pointer_decoding():
    suffix = b"\x07example\x04test\x00"
    packet = bytearray(b"\x00" * 12)
    packet += b"\x03www" + suffix
    suffix_offset = 16
    packet += b"\xc0" + bytes([suffix_offset])
    labels, _ = decode_name(bytes(packet), 12)
    assert labels == (b"www", b"example", b"test")
    labels, next_offset = decode_name(bytes(packet), 30)
    assert labels == (b"example", b"test")
    assert next_offset == 32


@pytest.mark.parametrize(
    "qname_wire",
    [
        b"\xc0\x0c",  # self pointer
        b"\xc0\x0e",  # forward pointer
        b"\xc0\x00",  # pointer into header
        b"\x40" + b"x" * 64,  # reserved/oversized label encoding
        b"\x80" + b"x" * 10,  # reserved 10xx label encoding
        b"\x3f" + b"a" * 63 + b"\x3f" + b"b" * 63 + b"\x3f" + b"c" * 63 + b"\x3f" + b"d" * 63 + b"\x00",
    ],
)
def test_malformed_udp_names_are_rejected(qname_wire):
    _assert_malformed(_query_wire(qname_wire))


def test_truncated_and_trailing_packets_are_rejected():
    _assert_malformed(b"\x00\x01\x01\x00")
    valid = _query_wire(b"\x03www\x00")
    _assert_malformed(valid + b"\x00")


def test_record_roundtrip_for_all_required_types():
    values = [
        RR("a.example.", QTYPE.A, ttl=10, rdata=A("192.0.2.1")),
        RR("aaaa.example.", QTYPE.AAAA, ttl=20, rdata=AAAA("2001:db8::1")),
        RR("c.example.", QTYPE.CNAME, ttl=30, rdata=CNAME("target.example.")),
        RR("ns.example.", QTYPE.NS, ttl=40, rdata=NS("ns1.example.")),
        RR("mx.example.", QTYPE.MX, ttl=50, rdata=MX("mail.example.", 10)),
        RR("txt.example.", QTYPE.TXT, ttl=60, rdata=TXT([b"one", b"two"])),
        RR(
            "soa.example.",
            QTYPE.SOA,
            ttl=70,
            rdata=SOA("ns.example.", "hostmaster.example.", (1, 2, 3, 4, 5)),
        ),
    ]
    for rr in values:
        record = Record.from_rr(rr)
        rebuilt = record.to_rr()
        assert rebuilt.rtype == rr.rtype
        assert rebuilt.ttl == rr.ttl
        assert str(rebuilt.rname).lower() == str(rr.rname).lower()
        assert rebuilt.rdata == rr.rdata


def test_edns_extraction_and_opt_echo():
    request = DNSRecord.question("Mixed.Example.Test.", "A")
    from dnslib import EDNS0

    request.add_ar(EDNS0(udp_len=1232, flags="do"))
    parsed = parse_request(request.pack())
    assert parsed.edns is True
    assert parsed.udp_payload == 1232
    assert parsed.do == 1
    assert parsed.key.name == "mixed.example.test."

    response, truncated = render_response(
        parsed,
        Resolution(
            kind="positive",
            rcode=RCODE_NOERROR,
            answers=(Record.from_rr(RR("x.example.", QTYPE.A, rdata=A("192.0.2.1"), ttl=60)),),
        ),
        transport="udp",
    )
    assert not truncated
    decoded = DNSRecord.parse(response)
    assert decoded.header.aa == 0
    assert decoded.header.ra == 1
    assert len(decoded.ar) == 1
    assert decoded.ar[0].rtype == QTYPE.OPT
    assert decoded.ar[0].rclass == 1232
    assert decoded.ar[0].edns_do == 1


def _large_resolution() -> Resolution:
    records = tuple(
        Record.from_rr(RR("big.example.", QTYPE.TXT, ttl=300, rdata=TXT(("x" * 180).encode())))
        for _ in range(10)
    )
    return Resolution(kind="positive", rcode=RCODE_NOERROR, answers=records)


def test_udp_truncation_tcp_full_and_edns_payload_policy():
    legacy = parse_request(DNSRecord.question("big.example.", "TXT").pack())
    udp_wire, truncated = render_response(legacy, _large_resolution(), transport="udp")
    assert truncated is True
    udp = DNSRecord.parse(udp_wire)
    assert udp.header.tc == 1
    assert len(udp.rr) == 0
    assert len(udp_wire) <= 512
    assert len(udp.ar) == 0

    tcp_wire, truncated = render_response(legacy, _large_resolution(), transport="tcp")
    assert truncated is False
    tcp = DNSRecord.parse(tcp_wire)
    assert tcp.header.tc == 0
    assert len(tcp.rr) == 10

    edns_request = DNSRecord.question("big.example.", "TXT")
    from dnslib import EDNS0

    edns_request.add_ar(EDNS0(udp_len=4096))
    edns = parse_request(edns_request.pack())
    edns_wire, truncated = render_response(edns, _large_resolution(), transport="udp")
    assert truncated is False
    decoded = DNSRecord.parse(edns_wire)
    assert decoded.header.tc == 0
    assert len(decoded.rr) == 10
    assert decoded.ar[0].rclass == 4096


def test_business_code_is_encoded_in_custom_edns_option():
    request = DNSRecord.question("loop.example.", "A")
    from dnslib import EDNS0

    request.add_ar(EDNS0(udp_len=1232))
    parsed = parse_request(request.pack())
    wire, _ = render_response(
        parsed,
        Resolution(kind="error", rcode=2, business_code="E_CNAME_LOOP"),
        transport="udp",
    )
    assert business_code_from_response(wire) == "E_CNAME_LOOP"
    assert DNSRecord.parse(wire).header.rcode == 2


def test_malformed_compression_pointer_in_rdata_is_rejected():
    request = DNSRecord.question("c.example.", "CNAME")
    raw = bytearray(request.pack())
    raw[6:8] = struct.pack("!H", 1)  # ANCOUNT
    # owner pointer to question, CNAME type, ttl, rdlength=2, forward pointer
    rdata_offset = len(raw) + 12
    raw += struct.pack("!HHIH", 0xC00C, QTYPE.CNAME, 60, 2)
    raw += struct.pack("!H", 0xC000 | rdata_offset)
    with pytest.raises(DnsFormatError):
        parse_response(bytes(raw))
