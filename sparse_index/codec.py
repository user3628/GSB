"""Difference encoding, LEB128 varints, and self-checking posting blocks."""

from __future__ import annotations

import struct
import zlib
from dataclasses import dataclass
from typing import Sequence

from .errors import error


BLOCK_MAGIC = b"PBLK"
BLOCK_VERSION = 1
BLOCK_HEADER = struct.Struct(">4sBII")  # magic, version, payload length, crc32
FILE_MAGIC = b"CSIDX1\0"


def encode_unsigned_varint(value: int) -> bytes:
    if value < 0:
        raise ValueError("unsigned varint cannot encode a negative value")
    output = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            output.append(byte | 0x80)
        else:
            output.append(byte)
            return bytes(output)


def decode_unsigned_varint(data: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0
    start = offset
    while offset < len(data):
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            return value, offset
        shift += 7
        if shift > 63:
            raise ValueError("varint is too long")
    raise ValueError(f"truncated varint at byte {start}")


def zigzag_encode(value: int) -> int:
    return value * 2 if value >= 0 else (-value * 2) - 1


def zigzag_decode(value: int) -> int:
    return value // 2 if value % 2 == 0 else -(value // 2) - 1


def encode_block(postings: Sequence[tuple[str, int]], doc_id_to_ordinal: dict[str, int]) -> bytes:
    """Encode a self-contained block; first doc id is absolute per block."""

    if not postings:
        raise ValueError("cannot encode an empty posting block")
    payload = bytearray()
    payload.extend(encode_unsigned_varint(len(postings)))
    previous_doc_ordinal = 0
    previous_weight = 0
    for position, (doc_id, quantized_weight) in enumerate(postings):
        ordinal = doc_id_to_ordinal[doc_id]
        if position == 0:
            payload.extend(encode_unsigned_varint(ordinal))
        else:
            delta = ordinal - previous_doc_ordinal
            if delta <= 0:
                raise ValueError("doc ids in a posting block must be strictly ascending")
            payload.extend(encode_unsigned_varint(delta))
        if position == 0:
            payload.extend(encode_unsigned_varint(zigzag_encode(quantized_weight)))
        else:
            payload.extend(
                encode_unsigned_varint(zigzag_encode(quantized_weight - previous_weight))
            )
        previous_doc_ordinal = ordinal
        previous_weight = quantized_weight
    payload_bytes = bytes(payload)
    checksum = zlib.crc32(payload_bytes) & 0xFFFFFFFF
    return BLOCK_HEADER.pack(BLOCK_MAGIC, BLOCK_VERSION, len(payload_bytes), checksum) + payload_bytes


def decode_block(
    block_bytes: bytes,
    ordinal_to_doc_id: Sequence[str],
    *,
    term_id: int | None = None,
    block_index: int | None = None,
) -> list[tuple[str, int]]:
    """Decode and validate one block, including its boundary metadata."""

    try:
        if len(block_bytes) < BLOCK_HEADER.size:
            raise ValueError("block header is truncated")
        magic, version, payload_length, checksum = BLOCK_HEADER.unpack(
            block_bytes[: BLOCK_HEADER.size]
        )
        if magic != BLOCK_MAGIC:
            raise ValueError("block magic mismatch")
        if version != BLOCK_VERSION:
            raise ValueError(f"unsupported block version {version}")
        if len(block_bytes) != BLOCK_HEADER.size + payload_length:
            raise ValueError(
                f"block length mismatch: expected {BLOCK_HEADER.size + payload_length}, "
                f"got {len(block_bytes)}"
            )
        payload = block_bytes[BLOCK_HEADER.size :]
        actual_checksum = zlib.crc32(payload) & 0xFFFFFFFF
        if actual_checksum != checksum:
            raise ValueError("block CRC32 mismatch")
        count, cursor = decode_unsigned_varint(payload, 0)
        if count <= 0:
            raise ValueError("block count must be positive")
        postings: list[tuple[str, int]] = []
        ordinal = 0
        weight = 0
        for position in range(count):
            if position == 0:
                ordinal, cursor = decode_unsigned_varint(payload, cursor)
            else:
                delta, cursor = decode_unsigned_varint(payload, cursor)
                if delta <= 0:
                    raise ValueError("doc id delta must be positive")
                ordinal += delta
            encoded_weight, cursor = decode_unsigned_varint(payload, cursor)
            delta_weight = zigzag_decode(encoded_weight)
            if position == 0:
                weight = delta_weight
            else:
                weight += delta_weight
            if ordinal < 0 or ordinal >= len(ordinal_to_doc_id):
                raise ValueError(f"doc ordinal {ordinal} is outside the document table")
            postings.append((ordinal_to_doc_id[ordinal], weight))
        if cursor != len(payload):
            raise ValueError("trailing bytes at block boundary")
        return postings
    except (ValueError, IndexError, struct.error) as exc:
        raise error(
            "E_BLOCK_CORRUPTION",
            "倒排分块解压失败或边界校验失败",
            term_id=term_id,
            block_index=block_index,
            reason=str(exc),
        )
