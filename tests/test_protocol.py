from __future__ import annotations

import struct

import pytest

from dns_workbench.errors import BusinessError
from dns_workbench.protocol import (
    DnsName,
    QTYPE_A,
    build_query,
    build_response,
    parse_message,
    parse_presentation_name,
    parse_query_packet,
    ResourceRecord,
)


def test_name_normalization_preserves_escaped_labels_and_case_folds_ascii() -> None:
    mixed = parse_presentation_name("FoO\\.Bar.Example.")
    assert mixed.canonical == "foo\\.bar.example."
    assert parse_presentation_name("FOO\\.BAR.EXAMPLE.").canonical == mixed.canonical
    assert parse_presentation_name("foo\\046bar.example.").canonical == mixed.canonical
    assert parse_presentation_name(".").canonical == "."


def test_query_roundtrip_and_compressed_response() -> None:
    wire = build_query("A.Lab.Test.", "A", identifier=0x1234)
    message = parse_query_packet(wire)
    assert message.id == 0x1234
    assert message.questions[0].name.canonical == "a.lab.test."
    response = build_response(
        message,
        answers=[
            ResourceRecord(
                    message.questions[0].name,
                QTYPE_A,
                1,
                120_000,
                {"address": "192.0.2.1"},
            )
        ],
    )
    parsed = parse_message(response)
    assert parsed.qr
    assert parsed.answers[0].name.canonical == "a.lab.test."
    assert len(response) < len(wire) + 30
    assert b"\xc0\x0c" in response


def test_truncated_packet_is_rejected() -> None:
    wire = build_query("a.lab.test.", "A", identifier=1)
    with pytest.raises(BusinessError) as exc:
        parse_query_packet(wire[:-2])
    assert exc.value.code == "E_MESSAGE_INVALID"


def test_forward_compression_pointer_is_rejected() -> None:
    packet = struct.pack("!HHHHHH", 7, 0x0100, 1, 0, 0, 0) + b"\xc0\x0d" + struct.pack("!HH", 1, 1)
    with pytest.raises(BusinessError) as exc:
        parse_query_packet(packet)
    assert exc.value.code == "E_MESSAGE_INVALID"
    assert "backwards" in exc.value.context["reason"]


def test_compression_pointer_loop_is_rejected() -> None:
    packet = (
        struct.pack("!HHHHHH", 8, 0x0100, 1, 0, 0, 0)
        + b"\x01a\xc0\x0c"
        + struct.pack("!HH", 1, 1)
    )
    with pytest.raises(BusinessError) as exc:
        parse_query_packet(packet)
    assert exc.value.code == "E_MESSAGE_INVALID"
    assert "loop" in exc.value.context["reason"]


def test_compression_pointer_into_middle_of_label_is_rejected() -> None:
    packet = (
        struct.pack("!HHHHHH", 10, 0, 1, 1, 0, 0)
        + b"\x03abc\x00"
        + struct.pack("!HH", 1, 1)
        + b"\xc0\x0e"
        + struct.pack("!HHIH", 1, 1, 60, 4)
        + b"\xc0\x00\x02\x01"
    )
    with pytest.raises(BusinessError) as exc:
        parse_message(packet)
    assert exc.value.code == "E_MESSAGE_INVALID"
    assert "label boundary" in exc.value.context["reason"]


def test_oversized_label_is_rejected() -> None:
    label = b"a" * 64
    packet = (
        struct.pack("!HHHHHH", 9, 0x0100, 1, 0, 0, 0)
        + bytes([len(label)])
        + label
        + b"\x00"
        + struct.pack("!HH", 1, 1)
    )
    with pytest.raises(BusinessError) as exc:
        parse_query_packet(packet)
    assert exc.value.code == "E_MESSAGE_INVALID"


def test_trailing_bytes_are_rejected() -> None:
    wire = build_query("a.lab.test.", "A", identifier=2) + b"junk"
    with pytest.raises(BusinessError) as exc:
        parse_message(wire)
    assert exc.value.code == "E_MESSAGE_INVALID"


def test_question_count_and_tc_query_are_rejected() -> None:
    wire = bytearray(build_query("a.lab.test.", "A", identifier=3))
    wire[2:4] = struct.pack("!H", 0x0300)
    with pytest.raises(BusinessError) as exc:
        parse_query_packet(bytes(wire))
    assert exc.value.code == "E_MESSAGE_INVALID"

