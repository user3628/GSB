from __future__ import annotations

import base64
import binascii
import struct
import zlib
from typing import Any, Callable

import numpy as np

from .errors import BusinessError, error
from .types import DTYPES, ENCODINGS, EncodedBlock


MAGIC = b"CSB1"
HEADER = struct.Struct("<4sBBIIII")
CONTAINER_MAGIC = b"CSBC"
CONTAINER_HEADER = struct.Struct("<4sBBIIIII")
CONTAINER_SEGMENT = struct.Struct("<IIII")
ENCODING_CODES = {name: index + 1 for index, name in enumerate(ENCODINGS)}
CODE_ENCODINGS = {value: key for key, value in ENCODING_CODES.items()}
DTYPE_CODES = {name: index + 1 for index, name in enumerate(DTYPES)}
CODE_DTYPES = {value: key for key, value in DTYPE_CODES.items()}
DTYPE_ITEMSIZE = {"int64": 8, "float64": 8, "bool": 1}


def _corrupt(block_id: str, reason: str, **details: Any) -> BusinessError:
    return error(
        "E_BLOCK_CORRUPT",
        f"compressed block is corrupt: {reason}",
        details={"block_id": block_id, "reason": reason, **details},
    )


def _scalar_bytes(dtype: str, value: Any) -> bytes:
    if dtype == "int64":
        return struct.pack("<q", int(value))
    if dtype == "float64":
        return struct.pack("<d", float(value))
    if dtype == "bool":
        return b"\x01" if bool(value) else b"\x00"
    if dtype == "string":
        return str(value).encode("utf-8")
    raise ValueError(f"unsupported dtype: {dtype}")


def _read_scalar(dtype: str, payload: bytes, offset: int) -> tuple[Any, int]:
    if dtype == "int64":
        end = offset + 8
        if end > len(payload):
            raise ValueError("truncated int64 value")
        return struct.unpack_from("<q", payload, offset)[0], end
    if dtype == "float64":
        end = offset + 8
        if end > len(payload):
            raise ValueError("truncated float64 value")
        return struct.unpack_from("<d", payload, offset)[0], end
    if dtype == "bool":
        end = offset + 1
        if end > len(payload):
            raise ValueError("truncated bool value")
        value = payload[offset]
        if value not in (0, 1):
            raise ValueError("invalid bool value")
        return bool(value), end
    if dtype == "string":
        end = offset + 4
        if end > len(payload):
            raise ValueError("truncated string length")
        length = struct.unpack_from("<I", payload, offset)[0]
        content_end = end + length
        if content_end > len(payload):
            raise ValueError("truncated string value")
        try:
            value = payload[end:content_end].decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("invalid UTF-8 string") from exc
        return value, content_end
    raise ValueError(f"unsupported dtype: {dtype}")


def _decode_scalar_raw(dtype: str, raw: bytes) -> Any:
    if dtype == "int64":
        if len(raw) != 8:
            raise ValueError("RLE/dictionary int64 value must be 8 bytes")
        return struct.unpack("<q", raw)[0]
    if dtype == "float64":
        if len(raw) != 8:
            raise ValueError("RLE/dictionary float64 value must be 8 bytes")
        return struct.unpack("<d", raw)[0]
    if dtype == "bool":
        if len(raw) != 1 or raw[0] not in (0, 1):
            raise ValueError("RLE/dictionary bool value must be 0 or 1")
        return bool(raw[0])
    if dtype == "string":
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("invalid UTF-8 string") from exc
    raise ValueError(f"unsupported dtype: {dtype}")


def _logical_raw_size(dtype: str, values: list[Any]) -> int:
    if dtype in DTYPE_ITEMSIZE:
        return len(values) * DTYPE_ITEMSIZE[dtype]
    return sum(4 + len(str(value).encode("utf-8")) for value in values)


def _encode_fixed(dtype: str, values: list[Any]) -> tuple[bytes, dict[str, int]]:
    if dtype == "int64":
        return struct.pack(f"<{len(values)}q", *(int(value) for value in values)), {"value_width": 8}
    if dtype == "float64":
        return struct.pack(f"<{len(values)}d", *(float(value) for value in values)), {"value_width": 8}
    if dtype == "bool":
        return bytes(1 if bool(value) else 0 for value in values), {"value_width": 1}
    encoded = [str(value).encode("utf-8") for value in values]
    width = max((len(value) for value in encoded), default=0)
    body = bytearray(struct.pack("<I", width))
    body.extend(struct.pack(f"<{len(encoded)}I", *(len(value) for value in encoded)))
    for value in encoded:
        body.extend(value)
        body.extend(b"\x00" * (width - len(value)))
    return bytes(body), {"value_width": width}


def _encode_rle(dtype: str, values: list[Any]) -> tuple[bytes, dict[str, int]]:
    runs: list[tuple[bytes, int]] = []
    for value in values:
        encoded = _scalar_bytes(dtype, value)
        if runs and runs[-1][0] == encoded:
            value_bytes, count = runs[-1]
            runs[-1] = (value_bytes, count + 1)
        else:
            runs.append((encoded, 1))
    body = bytearray(struct.pack("<I", len(runs)))
    for value_bytes, count in runs:
        body.extend(struct.pack("<II", len(value_bytes), count))
        body.extend(value_bytes)
    return bytes(body), {"run_count": len(runs)}


def _encode_dict(dtype: str, values: list[Any]) -> tuple[bytes, dict[str, int]]:
    encoded_values = [_scalar_bytes(dtype, value) for value in values]
    dictionary = sorted(set(encoded_values))
    positions = {value: index for index, value in enumerate(dictionary)}
    body = bytearray(struct.pack("<I", len(dictionary)))
    for value in dictionary:
        body.extend(struct.pack("<I", len(value)))
        body.extend(value)
    body.extend(struct.pack(f"<{len(values)}I", *(positions[value] for value in encoded_values)))
    return bytes(body), {"dict_size": len(dictionary)}


_ENCODERS: dict[str, Callable[[str, list[Any]], tuple[bytes, dict[str, int]]]] = {
    "fixed": _encode_fixed,
    "rle": _encode_rle,
    "dict": _encode_dict,
}


def encode_block(dtype: str, encoding: str, values: list[Any]) -> EncodedBlock:
    if dtype not in DTYPES:
        raise ValueError(f"unsupported dtype: {dtype}")
    if encoding not in ENCODINGS:
        raise ValueError(f"unsupported encoding: {encoding}")
    body, meta = _ENCODERS[encoding](dtype, values)
    raw_size = _logical_raw_size(dtype, values)
    checksum = zlib.crc32(body) & 0xFFFFFFFF
    header = HEADER.pack(
        MAGIC,
        ENCODING_CODES[encoding],
        DTYPE_CODES[dtype],
        len(values),
        raw_size,
        len(body),
        checksum,
    )
    payload = header + body
    ratio = round(raw_size / len(payload), 6) if payload else 0.0
    return EncodedBlock(
        payload_b64=base64.b64encode(payload).decode("ascii"),
        raw_size=raw_size,
        compressed_size=len(payload),
        compression_ratio=ratio,
        encoding_meta=meta,
    )


def encode_segmented_block(
    dtype: str,
    encoding: str,
    values: list[Any],
    *,
    segment_rows: int,
) -> EncodedBlock:
    if dtype not in DTYPES:
        raise ValueError(f"unsupported dtype: {dtype}")
    if encoding not in ENCODINGS:
        raise ValueError(f"unsupported encoding: {encoding}")
    if segment_rows < 1:
        raise ValueError("segment_rows must be positive")
    records: list[bytes] = []
    raw_size = 0
    for start in range(0, len(values), segment_rows):
        segment_values = values[start : start + segment_rows]
        encoded = encode_block(dtype, encoding, segment_values)
        segment_payload = base64.b64decode(encoded.payload_b64)
        records.append(
            CONTAINER_SEGMENT.pack(start, len(segment_values), len(segment_payload), encoded.raw_size) + segment_payload
        )
        raw_size += encoded.raw_size
    body = b"".join(records)
    checksum = zlib.crc32(body) & 0xFFFFFFFF
    header = CONTAINER_HEADER.pack(
        CONTAINER_MAGIC,
        ENCODING_CODES[encoding],
        DTYPE_CODES[dtype],
        len(values),
        raw_size,
        len(records),
        len(body),
        checksum,
    )
    payload = header + body
    ratio = round(raw_size / len(payload), 6) if payload else 0.0
    return EncodedBlock(
        payload_b64=base64.b64encode(payload).decode("ascii"),
        raw_size=raw_size,
        compressed_size=len(payload),
        compression_ratio=ratio,
        encoding_meta={"segment_rows": segment_rows, "segment_count": len(records)},
    )


def _decode_body(dtype: str, encoding: str, body: bytes, row_count: int) -> list[Any]:
    offset = 0
    values: list[Any] = []
    if encoding == "fixed":
        if dtype in DTYPE_ITEMSIZE:
            width = DTYPE_ITEMSIZE[dtype]
            expected = row_count * width
            if len(body) != expected:
                raise ValueError(f"fixed payload length {len(body)} != {expected}")
            if dtype == "int64":
                return list(struct.unpack(f"<{row_count}q", body))
            if dtype == "float64":
                return list(struct.unpack(f"<{row_count}d", body))
            return [bool(value) for value in body]
        if row_count == 0:
            if len(body) != 4:
                raise ValueError("invalid empty fixed string payload")
            return []
        if len(body) < 4:
            raise ValueError("truncated fixed string width")
        width = struct.unpack_from("<I", body, 0)[0]
        offset = 4
        lengths_end = offset + row_count * 4
        if lengths_end > len(body):
            raise ValueError("truncated fixed string lengths")
        lengths = list(struct.unpack_from(f"<{row_count}I", body, offset))
        offset = lengths_end
        expected = offset + row_count * width
        if len(body) != expected:
            raise ValueError(f"fixed string payload length {len(body)} != {expected}")
        for length in lengths:
            if length > width:
                raise ValueError("fixed string length exceeds declared width")
            raw = body[offset : offset + length]
            offset += width
            values.append(raw.decode("utf-8"))
        return values

    if encoding == "rle":
        if len(body) < 4:
            raise ValueError("truncated RLE run count")
        run_count = struct.unpack_from("<I", body, 0)[0]
        offset = 4
        for _ in range(run_count):
            if offset + 8 > len(body):
                raise ValueError("truncated RLE run header")
            length, repeat = struct.unpack_from("<II", body, offset)
            offset += 8
            if offset + length > len(body):
                raise ValueError("truncated RLE value")
            raw = body[offset : offset + length]
            offset += length
            if repeat == 0:
                raise ValueError("RLE run length must be positive")
            if len(values) + repeat > row_count:
                raise ValueError("RLE runs exceed row count")
            value = _decode_scalar_raw(dtype, raw)
            values.extend([value] * repeat)
        if offset != len(body):
            raise ValueError("RLE payload contains trailing bytes")
        if len(values) != row_count:
            raise ValueError("RLE runs do not match row count")
        return values

    if encoding == "dict":
        if len(body) < 4:
            raise ValueError("truncated dictionary size")
        dict_size = struct.unpack_from("<I", body, 0)[0]
        offset = 4
        dictionary: list[Any] = []
        for _ in range(dict_size):
            if offset + 4 > len(body):
                raise ValueError("truncated dictionary entry length")
            length = struct.unpack_from("<I", body, offset)[0]
            offset += 4
            if offset + length > len(body):
                raise ValueError("truncated dictionary entry")
            raw = body[offset : offset + length]
            offset += length
            value = _decode_scalar_raw(dtype, raw)
            dictionary.append(value)
        indices_end = offset + row_count * 4
        if indices_end > len(body):
            raise ValueError("truncated dictionary indices")
        indices = struct.unpack_from(f"<{row_count}I", body, offset)
        if indices_end != len(body):
            raise ValueError("dictionary payload contains trailing bytes")
        for index in indices:
            if index >= dict_size:
                raise ValueError("dictionary index out of range")
            values.append(dictionary[index])
        return values

    raise ValueError(f"unsupported encoding: {encoding}")


def _validate_flat_payload(
    payload: bytes,
    *,
    block_id: str,
    dtype: str,
    encoding: str,
    raw_size: int,
    row_count: int,
) -> bytes:
    if len(payload) < HEADER.size:
        raise _corrupt(block_id, "truncated block header", actual_size=len(payload))
    try:
        magic, encoding_code, dtype_code, header_rows, header_raw, body_len, checksum = HEADER.unpack_from(payload, 0)
    except struct.error as exc:
        raise _corrupt(block_id, "invalid block header") from exc
    if magic != MAGIC:
        raise _corrupt(block_id, "invalid block magic", actual_magic=magic.hex())
    actual_encoding = CODE_ENCODINGS.get(encoding_code)
    if actual_encoding != encoding:
        raise _corrupt(
            block_id,
            "encoding header mismatch",
            expected_encoding=encoding,
            actual_encoding=actual_encoding,
        )
    actual_dtype = CODE_DTYPES.get(dtype_code)
    if actual_dtype != dtype:
        raise _corrupt(block_id, "dtype header mismatch", expected_dtype=dtype, actual_dtype=actual_dtype)
    if header_rows != row_count:
        raise _corrupt(block_id, "row count header mismatch", expected_rows=row_count, actual_rows=header_rows)
    if header_raw != raw_size:
        raise _corrupt(block_id, "original size header mismatch", expected_raw_size=raw_size, actual_raw_size=header_raw)
    body = payload[HEADER.size :]
    if body_len != len(body):
        raise _corrupt(block_id, "payload length header mismatch", expected_body_size=body_len, actual_body_size=len(body))
    actual_checksum = zlib.crc32(body) & 0xFFFFFFFF
    if actual_checksum != checksum:
        raise _corrupt(block_id, "payload checksum mismatch", expected_crc32=checksum, actual_crc32=actual_checksum)
    return body


def _decode_container(
    payload: bytes,
    *,
    block_id: str,
    dtype: str,
    encoding: str,
    raw_size: int,
    row_count: int,
    row_range: tuple[int, int] | None,
) -> list[Any]:
    try:
        magic, encoding_code, dtype_code, total_rows, total_raw, segment_count, body_len, checksum = CONTAINER_HEADER.unpack_from(payload, 0)
    except struct.error as exc:
        raise _corrupt(block_id, "invalid segmented block header") from exc
    if magic != CONTAINER_MAGIC:
        raise _corrupt(block_id, "invalid segmented block magic")
    if CODE_ENCODINGS.get(encoding_code) != encoding:
        raise _corrupt(block_id, "segmented encoding header mismatch", expected_encoding=encoding)
    if CODE_DTYPES.get(dtype_code) != dtype:
        raise _corrupt(block_id, "segmented dtype header mismatch", expected_dtype=dtype)
    if total_rows != row_count:
        raise _corrupt(block_id, "segmented row count mismatch", expected_rows=row_count, actual_rows=total_rows)
    if total_raw != raw_size:
        raise _corrupt(
            block_id,
            "segmented original size mismatch",
            expected_raw_size=raw_size,
            actual_raw_size=total_raw,
        )
    body = payload[CONTAINER_HEADER.size :]
    if len(body) != body_len:
        raise _corrupt(block_id, "segmented body length mismatch", expected_body_size=body_len, actual_body_size=len(body))
    actual_checksum = zlib.crc32(body) & 0xFFFFFFFF
    if actual_checksum != checksum:
        raise _corrupt(block_id, "segmented payload checksum mismatch", expected_crc32=checksum, actual_crc32=actual_checksum)
    if row_range is None:
        requested_start, requested_end = 0, row_count
    else:
        requested_start, requested_end = row_range
        if requested_start < 0 or requested_end < requested_start or requested_end > row_count:
            raise _corrupt(block_id, "requested row range is outside the block", row_range=row_range)
    offset = 0
    expected_start = 0
    decoded_values: list[Any] = []
    selected_segments: list[tuple[int, int, list[Any]]] = []
    for _ in range(segment_count):
        if offset + CONTAINER_SEGMENT.size > len(body):
            raise _corrupt(block_id, "truncated segment table")
        start, count, segment_size, segment_raw = CONTAINER_SEGMENT.unpack_from(body, offset)
        offset += CONTAINER_SEGMENT.size
        if start != expected_start or count < 1:
            raise _corrupt(block_id, "segment ranges are not contiguous", expected_start=expected_start, actual_start=start)
        if offset + segment_size > len(body):
            raise _corrupt(block_id, "truncated segment payload")
        segment_payload = body[offset : offset + segment_size]
        offset += segment_size
        segment_body = _validate_flat_payload(
            segment_payload,
            block_id=block_id,
            dtype=dtype,
            encoding=encoding,
            raw_size=segment_raw,
            row_count=count,
        )
        expected_start += count
        overlaps = start < requested_end and start + count > requested_start
        if overlaps:
            try:
                values = _decode_body(dtype, encoding, segment_body, count)
            except (ValueError, struct.error, UnicodeDecodeError) as exc:
                raise _corrupt(block_id, str(exc)) from exc
            local_start = max(requested_start, start) - start
            local_end = min(requested_end, start + count) - start
            selected_segments.append((start + local_start, start + local_end, values[local_start:local_end]))
    if offset != len(body):
        raise _corrupt(block_id, "segmented payload contains trailing bytes")
    if expected_start != row_count:
        raise _corrupt(block_id, "segments do not cover the row block", expected_rows=row_count, actual_rows=expected_start)
    for _, _, values in sorted(selected_segments, key=lambda item: item[0]):
        decoded_values.extend(values)
    if len(decoded_values) != requested_end - requested_start:
        raise _corrupt(block_id, "decoded row range has an unexpected size")
    return decoded_values


def decode_block(
    payload_b64: str,
    *,
    block_id: str,
    dtype: str,
    encoding: str,
    raw_size: int,
    row_count: int,
    row_range: tuple[int, int] | None = None,
) -> np.ndarray:
    try:
        payload = base64.b64decode(payload_b64, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise _corrupt(block_id, "invalid base64 payload") from exc
    if payload.startswith(CONTAINER_MAGIC):
        if len(payload) < CONTAINER_HEADER.size:
            raise _corrupt(block_id, "truncated segmented block header", actual_size=len(payload))
        values = _decode_container(
            payload,
            block_id=block_id,
            dtype=dtype,
            encoding=encoding,
            raw_size=raw_size,
            row_count=row_count,
            row_range=row_range,
        )
    else:
        body = _validate_flat_payload(
            payload,
            block_id=block_id,
            dtype=dtype,
            encoding=encoding,
            raw_size=raw_size,
            row_count=row_count,
        )
        try:
            values = _decode_body(dtype, encoding, body, row_count)
        except (ValueError, struct.error, UnicodeDecodeError) as exc:
            raise _corrupt(block_id, str(exc)) from exc
        if row_range is not None:
            start, end = row_range
            if start < 0 or end < start or end > row_count:
                raise _corrupt(block_id, "requested row range is outside the block", row_range=row_range)
            values = values[start:end]
    if dtype == "int64":
        return np.asarray(values, dtype=np.int64)
    if dtype == "float64":
        return np.asarray(values, dtype=np.float64)
    if dtype == "bool":
        return np.asarray([1 if value else 0 for value in values], dtype=np.uint8)
    result = np.empty(len(values), dtype=object)
    result[:] = values
    return result

