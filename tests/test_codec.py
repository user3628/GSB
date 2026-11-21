from __future__ import annotations

import pytest

from vgw.codec import (
    decode_request,
    decode_response,
    encode_request,
    encode_response,
    parse_sequence,
)
from vgw.errors import GatewayError


def test_request_round_trip_and_fields() -> None:
    payload = b'{"op":"echo","body":"hello"}'
    raw = encode_request("alpha", 2, 17, payload)
    frame = decode_request(raw)
    assert frame.session_id == "alpha"
    assert frame.channel == 2
    assert frame.sequence == 17
    assert frame.payload == payload
    assert frame.checksum.startswith("sha256:")
    assert len(frame.checksum) == 71


def test_response_round_trip() -> None:
    raw = encode_response("alpha", 1, 9, b'{"ok":true}')
    response = decode_response(raw)
    assert response.status == 0
    assert response.session_id == "alpha"
    assert response.sequence == 9
    assert response.payload_json() == {"ok": True}


@pytest.mark.parametrize(
    "mutator, expected_fragment",
    [
        (lambda raw: raw[:-1] + bytes([raw[-1] ^ 0xFF]), "checksum mismatch"),
        (lambda raw: raw[:10], "shorter than"),
        (lambda raw: raw[:-3], "declared lengths"),
    ],
)
def test_invalid_frames_are_rejected(mutator, expected_fragment: str) -> None:
    raw = encode_request("alpha", 0, 1, b'{"x":1}')
    with pytest.raises(GatewayError) as caught:
        decode_request(mutator(raw))
    assert caught.value.code == "E_FRAME_INVALID"
    assert expected_fragment in caught.value.message


def test_payload_limit_is_rejected_before_encoding() -> None:
    with pytest.raises(GatewayError) as caught:
        encode_request("alpha", 0, 1, b"x" * 20, max_payload=10)
    assert caught.value.code == "E_FRAME_INVALID"
    assert caught.value.context["max_payload"] == 10


@pytest.mark.parametrize("value", ["abc", None, -1, 0, True])
def test_sequence_parse_failure(value) -> None:
    with pytest.raises(GatewayError) as caught:
        parse_sequence(value)
    assert caught.value.code == "E_FRAME_INVALID"


def test_sequence_accepts_numeric_string() -> None:
    assert parse_sequence("42") == 42

