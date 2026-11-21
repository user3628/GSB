from __future__ import annotations

import hashlib
import hmac
import json
import struct
from dataclasses import dataclass
from typing import Any

from .errors import GatewayError


REQUEST_MAGIC = b"VGW1"
RESPONSE_MAGIC = b"VGR1"
WIRE_VERSION = 1
REQUEST_HEADER = struct.Struct("!4sBBHIQHH")
RESPONSE_HEADER = struct.Struct("!4sBIQIHH")
MAX_UDP_DATAGRAM = 65507
STATUS_OK = 0
STATUS_INVALID_FRAME = 2


@dataclass(frozen=True, slots=True)
class DecodedFrame:
    session_id: str
    channel: int
    sequence: int
    payload: bytes
    checksum: str
    raw_size: int

    def payload_json(self) -> Any:
        try:
            return json.loads(self.payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None


@dataclass(frozen=True, slots=True)
class DecodedResponse:
    status: int
    session_id: str
    channel: int
    sequence: int
    payload: bytes
    checksum: str

    def payload_json(self) -> Any:
        try:
            return json.loads(self.payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None


def parse_sequence(value: Any) -> int:
    if isinstance(value, bool):
        raise GatewayError(
            "E_FRAME_INVALID",
            "sequence must be an integer",
            {"field": "sequence", "received_type": "bool"},
        )
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and value.strip().isdigit():
        parsed = int(value.strip(), 10)
    else:
        raise GatewayError(
            "E_FRAME_INVALID",
            "sequence parse failed",
            {"field": "sequence", "received": repr(value)},
        )
    if not (1 <= parsed <= (2**64 - 1)):
        raise GatewayError(
            "E_FRAME_INVALID",
            "sequence is outside the uint64 range",
            {"field": "sequence", "value": parsed},
        )
    return parsed


def encode_request(
    session_id: str,
    channel: int,
    sequence: int,
    payload: bytes,
    *,
    max_payload: int | None = None,
) -> bytes:
    session_bytes = _session_bytes(session_id)
    _validate_channel(channel)
    if not isinstance(sequence, int) or isinstance(sequence, bool) or not (
        1 <= sequence <= 2**64 - 1
    ):
        raise GatewayError(
            "E_FRAME_INVALID",
            "sequence must be a positive uint64",
            {"sequence": repr(sequence)},
        )
    if max_payload is not None and len(payload) > max_payload:
        raise GatewayError(
            "E_FRAME_INVALID",
            "payload exceeds the session limit",
            {"payload_bytes": len(payload), "max_payload": max_payload},
        )
    header = REQUEST_HEADER.pack(
        REQUEST_MAGIC,
        WIRE_VERSION,
        0,
        channel,
        sequence,
        len(payload),
        len(session_bytes),
        0,
    )
    body = header + session_bytes + payload
    raw = body + hashlib.sha256(body).digest()
    if len(raw) > MAX_UDP_DATAGRAM:
        raise GatewayError(
            "E_FRAME_INVALID",
            "encoded frame exceeds the UDP datagram limit",
            {"frame_bytes": len(raw), "max_datagram": MAX_UDP_DATAGRAM},
        )
    return raw


def decode_request(raw: bytes, *, max_payload: int = 60000) -> DecodedFrame:
    if len(raw) < REQUEST_HEADER.size + 32:
        raise _invalid("frame is shorter than the fixed header and digest", raw=raw)
    try:
        magic, version, _flags, channel, sequence, payload_len, session_len, _reserved = (
            REQUEST_HEADER.unpack_from(raw, 0)
        )
    except struct.error as exc:
        raise _invalid("frame header parse failed", detail=str(exc)) from exc
    if magic != REQUEST_MAGIC:
        raise _invalid("bad request magic", magic=magic.hex())
    if version != WIRE_VERSION:
        raise _invalid("unsupported wire version", wire_version=version)
    if payload_len > max_payload:
        raise _invalid(
            "payload exceeds configured maximum",
            payload_bytes=payload_len,
            max_payload=max_payload,
        )
    expected_size = REQUEST_HEADER.size + session_len + payload_len + 32
    if expected_size != len(raw):
        raise _invalid(
            "declared lengths do not match datagram size",
            expected_size=expected_size,
            actual_size=len(raw),
            payload_len=payload_len,
            session_len=session_len,
        )
    session_start = REQUEST_HEADER.size
    payload_start = session_start + session_len
    digest_start = payload_start + payload_len
    try:
        session_id = raw[session_start:payload_start].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise _invalid("session_id is not valid UTF-8") from exc
    if not session_id:
        raise _invalid("session_id must not be empty")
    payload = raw[payload_start:digest_start]
    actual_digest = hashlib.sha256(raw[:digest_start]).digest()
    declared_digest = raw[digest_start:digest_start + 32]
    if not hmac.compare_digest(actual_digest, declared_digest):
        raise _invalid(
            "frame checksum mismatch",
            declared_checksum="sha256:" + declared_digest.hex(),
            actual_checksum="sha256:" + actual_digest.hex(),
        )
    return DecodedFrame(
        session_id=session_id,
        channel=channel,
        sequence=sequence,
        payload=payload,
        checksum="sha256:" + actual_digest.hex(),
        raw_size=len(raw),
    )


def encode_response(
    session_id: str,
    channel: int,
    sequence: int,
    payload: bytes,
    *,
    status: int = STATUS_OK,
) -> bytes:
    session_bytes = _session_bytes(session_id, allow_empty=True)
    header = RESPONSE_HEADER.pack(
        RESPONSE_MAGIC,
        status,
        channel,
        sequence,
        len(payload),
        len(session_bytes),
        0,
    )
    body = header + session_bytes + payload
    return body + hashlib.sha256(body).digest()


def decode_response(raw: bytes, *, max_payload: int = 60000) -> DecodedResponse:
    if len(raw) < RESPONSE_HEADER.size + 32:
        raise _invalid("response is shorter than the fixed header and digest", raw=raw)
    try:
        (
            magic,
            status,
            channel,
            sequence,
            payload_len,
            session_len,
            _reserved,
        ) = RESPONSE_HEADER.unpack_from(raw, 0)
    except struct.error as exc:
        raise _invalid("response header parse failed", detail=str(exc)) from exc
    if magic != RESPONSE_MAGIC:
        raise _invalid("bad response magic", magic=magic.hex())
    if payload_len > max_payload:
        raise _invalid("response payload too large", payload_bytes=payload_len)
    expected_size = RESPONSE_HEADER.size + session_len + payload_len + 32
    if expected_size != len(raw):
        raise _invalid(
            "response lengths do not match datagram size",
            expected_size=expected_size,
            actual_size=len(raw),
        )
    session_start = RESPONSE_HEADER.size
    payload_start = session_start + session_len
    digest_start = payload_start + payload_len
    try:
        session_id = raw[session_start:payload_start].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise _invalid("response session_id is not valid UTF-8") from exc
    declared_digest = raw[digest_start:digest_start + 32]
    actual_digest = hashlib.sha256(raw[:digest_start]).digest()
    if not hmac.compare_digest(actual_digest, declared_digest):
        raise _invalid("response checksum mismatch")
    return DecodedResponse(
        status=status,
        session_id=session_id,
        channel=channel,
        sequence=sequence,
        payload=raw[payload_start:digest_start],
        checksum="sha256:" + actual_digest.hex(),
    )


def encode_error_response(raw_request: bytes, code: str, message: str) -> bytes:
    session_id = ""
    channel = 0
    sequence = 0
    if len(raw_request) >= REQUEST_HEADER.size:
        try:
            (
                _magic,
                _version,
                _flags,
                channel,
                sequence,
                payload_len,
                session_len,
                _reserved,
            ) = REQUEST_HEADER.unpack_from(raw_request, 0)
            start = REQUEST_HEADER.size
            end = start + session_len
            if end <= len(raw_request) and payload_len <= MAX_UDP_DATAGRAM:
                session_id = raw_request[start:end].decode("utf-8", errors="replace")
        except struct.error:
            pass
    payload = json.dumps(
        {"ok": False, "code": code, "message": message},
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return encode_response(
        session_id,
        channel,
        sequence,
        payload,
        status=STATUS_INVALID_FRAME,
    )


def _session_bytes(session_id: str, *, allow_empty: bool = False) -> bytes:
    if not isinstance(session_id, str):
        raise _invalid("session_id must be a string")
    data = session_id.encode("utf-8")
    if not data and not allow_empty:
        raise _invalid("session_id must not be empty")
    if len(data) > 65535:
        raise _invalid("session_id is too long", session_bytes=len(data))
    return data


def _validate_channel(channel: int) -> None:
    if not isinstance(channel, int) or isinstance(channel, bool) or not (0 <= channel <= 65535):
        raise _invalid("channel must be a uint16", channel=repr(channel))


def _invalid(message: str, **context: Any) -> GatewayError:
    return GatewayError("E_FRAME_INVALID", message, context)

