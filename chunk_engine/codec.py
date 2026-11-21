from __future__ import annotations

import math
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

from .errors import BusinessError, E_BLOCK_CORRUPT
from .utils import atomic_write_bytes, canonical_json_bytes, sha256_hex, strict_json_loads


BLOCK_MAGIC = b"CHUNKBLK1\n"
BLOCK_FORMAT = "chunk-column-block-v1"
HEADER_LENGTH_WIDTH = 4
MAX_HEADER_BYTES = 1024 * 1024
SUPPORTED_ENCODINGS = frozenset({"fixed", "run", "dict"})
SUPPORTED_DTYPES = frozenset({"int64", "float64", "string"})
_WIDTH = {"int64": 8, "float64": 8}
_STRUCT_FORMAT = {"int64": "q", "float64": "d"}


@dataclass(frozen=True)
class DecodedSlice:
    block_no: int
    column: str
    dtype: str
    encoding: str
    key_min: int
    key_max: int
    row_offset: int
    values: list[Any]
    null_mask: list[bool]
    null_bitmap_bytes: bytes
    row_offsets: list[int]

    @property
    def row_count(self) -> int:
        return len(self.values)


def _business(code: str, message: str, context: dict[str, Any], status: int = 400) -> BusinessError:
    return BusinessError(code, message, context, status)


def _block_error(expected: Any, reason: str, **extra: Any) -> BusinessError:
    block_no = getattr(expected, "block_no", None)
    column = getattr(expected, "column", None)
    context = {"block_no": block_no, "column": column, "reason": reason}
    context.update(extra)
    return _business(E_BLOCK_CORRUPT, "block failed validation before decode", context)


def pack_null_bitmap(null_mask: Sequence[bool]) -> bytes:
    data = bytearray((len(null_mask) + 7) // 8)
    for index, is_null in enumerate(null_mask):
        if is_null:
            data[index >> 3] |= 1 << (index & 7)
    return bytes(data)


def unpack_null_bitmap(bitmap: bytes, row_count: int) -> list[bool]:
    if len(bitmap) != (row_count + 7) // 8:
        raise ValueError("null bitmap length does not match row count")
    return [bool(bitmap[index >> 3] & (1 << (index & 7))) for index in range(row_count)]


def _validate_value(dtype: str, value: Any) -> None:
    if dtype == "int64":
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("int64 value must be an integer")
        if value < -(2**63) or value > 2**63 - 1:
            raise ValueError("int64 value is out of range")
    elif dtype == "float64":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("float64 value must be numeric")
        if not math.isfinite(float(value)):
            raise ValueError("float64 value must be finite")
    elif dtype == "string":
        if not isinstance(value, str):
            raise ValueError("string value must be a string")
        if len(value.encode("utf-8")) > 65535:
            raise ValueError("string value exceeds 65535 UTF-8 bytes")
    else:
        raise ValueError(f"unsupported dtype: {dtype}")


def _value_equal(dtype: str, left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is None and right is None
    if dtype == "float64":
        return float(left).hex() == float(right).hex()
    return left == right


def _append_value(buffer: bytearray, dtype: str, value: Any) -> None:
    if dtype in ("int64", "float64"):
        buffer.extend(struct.pack("<" + _STRUCT_FORMAT[dtype], value))
    elif dtype == "string":
        encoded = value.encode("utf-8")
        buffer.extend(struct.pack("<H", len(encoded)))
        buffer.extend(encoded)
    else:
        raise ValueError(f"unsupported dtype: {dtype}")


def _encode_fixed(values: Sequence[Any], dtype: str) -> bytes:
    if dtype not in _WIDTH:
        raise ValueError("fixed encoding supports int64 and float64 only")
    output = bytearray()
    for value in values:
        if value is None:
            value = 0 if dtype == "int64" else 0.0
        _validate_value(dtype, value)
        output.extend(struct.pack("<" + _STRUCT_FORMAT[dtype], value))
    return bytes(output)


def _encode_run(values: Sequence[Any], dtype: str) -> bytes:
    runs: list[tuple[int, bool, Any]] = []
    for value in values:
        if value is not None:
            _validate_value(dtype, value)
        is_null = value is None
        if runs and runs[-1][1] == is_null and _value_equal(dtype, runs[-1][2], value):
            run_length, _, run_value = runs[-1]
            runs[-1] = (run_length + 1, is_null, run_value)
        else:
            runs.append((1, is_null, value))
    output = bytearray(struct.pack("<I", len(runs)))
    for run_length, is_null, value in runs:
        output.extend(struct.pack("<IB", run_length, 1 if is_null else 0))
        if not is_null:
            _append_value(output, dtype, value)
    return bytes(output)


def _encode_dict(values: Sequence[Any], dtype: str) -> bytes:
    dictionary: list[Any] = []
    positions: dict[Any, int] = {}
    indices: list[int] = []
    for value in values:
        if value is None:
            indices.append(0)
            continue
        _validate_value(dtype, value)
        key = float(value).hex() if dtype == "float64" else value
        if key not in positions:
            positions[key] = len(dictionary)
            dictionary.append(value)
        indices.append(positions[key])
    if not dictionary:
        dictionary.append(0 if dtype == "int64" else 0.0 if dtype == "float64" else "")
    output = bytearray(struct.pack("<I", len(dictionary)))
    for value in dictionary:
        _append_value(output, dtype, value)
    for index in indices:
        output.extend(struct.pack("<I", index))
    return bytes(output)


def estimate_decompressed_bytes(dtype: str, values: Sequence[Any]) -> int:
    """Conservative estimate of the in-memory decoded representation."""
    row_count = len(values)
    null_bytes = (row_count + 7) // 8
    if dtype in _WIDTH:
        row_bytes = row_count * _WIDTH[dtype]
    else:
        row_bytes = 0
        for value in values:
            row_bytes += 16 if value is None else 16 + len(value.encode("utf-8"))
    pointer_bytes = row_count * 8
    mask_bytes = row_count
    return max(1, row_bytes + null_bytes + pointer_bytes + mask_bytes)


def write_block_file(
    path: Path,
    *,
    block_no: int,
    column: str,
    dtype: str,
    encoding: str,
    row_offset: int,
    key_min: int,
    key_max: int,
    values: Sequence[Any],
    null_mask: Sequence[bool] | None = None,
) -> dict[str, Any]:
    row_count = len(values)
    if row_count <= 0:
        raise ValueError("a block must contain at least one row")
    if encoding not in SUPPORTED_ENCODINGS:
        raise ValueError(f"unsupported encoding: {encoding}")
    if dtype not in SUPPORTED_DTYPES:
        raise ValueError(f"unsupported dtype: {dtype}")
    if null_mask is not None and row_count != len(null_mask):
        raise ValueError("null mask length must match values")
    actual_null_mask = list(null_mask) if null_mask is not None else [value is None for value in values]
    for value, is_null in zip(values, actual_null_mask):
        if (value is None) != bool(is_null):
            raise ValueError("null mask and values disagree")
    for value, is_null in zip(values, actual_null_mask):
        if value is not None and is_null:
            raise ValueError("null mask marks a non-null value as null")

    null_bitmap = pack_null_bitmap(actual_null_mask)
    non_null_values: list[Any] = []
    for value, is_null in zip(values, actual_null_mask):
        non_null_values.append(None if is_null else value)
    if encoding == "fixed":
        payload = _encode_fixed(non_null_values, dtype)
    elif encoding == "run":
        payload = _encode_run(non_null_values, dtype)
    else:
        payload = _encode_dict(non_null_values, dtype)
    body = null_bitmap + payload
    header = {
        "format": BLOCK_FORMAT,
        "block_no": block_no,
        "column": column,
        "dtype": dtype,
        "encoding": encoding,
        "row_offset": row_offset,
        "row_count": row_count,
        "original_rows": row_count,
        "key_min": key_min,
        "key_max": key_max,
        "null_bitmap_len": len(null_bitmap),
        "null_count": sum(actual_null_mask),
        "body_len": len(body),
        "checksum": sha256_hex(body),
    }
    header_bytes = canonical_json_bytes(header)
    raw = BLOCK_MAGIC + struct.pack(">I", len(header_bytes)) + header_bytes + body
    atomic_write_bytes(path, raw)
    return {
        **header,
        "file": path.name,
        "decompressed_bytes": estimate_decompressed_bytes(dtype, values),
        "encoded_bytes": len(body),
    }


def read_header(path: Path, expected: Any | None = None) -> dict[str, Any]:
    try:
        with path.open("rb") as handle:
            magic = handle.read(len(BLOCK_MAGIC))
            if magic != BLOCK_MAGIC:
                raise _block_error(expected, "bad block magic", path=str(path))
            length_bytes = handle.read(HEADER_LENGTH_WIDTH)
            if len(length_bytes) != HEADER_LENGTH_WIDTH:
                raise _block_error(expected, "truncated header length", path=str(path))
            header_length = struct.unpack(">I", length_bytes)[0]
            if header_length <= 0 or header_length > MAX_HEADER_BYTES:
                raise _block_error(expected, "invalid header length", path=str(path))
            header_bytes = handle.read(header_length)
            if len(header_bytes) != header_length:
                raise _block_error(expected, "truncated header", path=str(path))
    except FileNotFoundError as exc:
        raise _block_error(expected, "block file is missing", path=str(path)) from exc
    except OSError as exc:
        raise _block_error(expected, "block file cannot be read", path=str(path), errno=exc.errno) from exc
    try:
        header = strict_json_loads(header_bytes)
    except Exception as exc:
        raise _block_error(expected, "header is not valid JSON", path=str(path)) from exc
    if not isinstance(header, dict):
        raise _block_error(expected, "header must be an object", path=str(path))
    return header


def _verify_header(header: dict[str, Any], expected: Any) -> None:
    fields = (
        "block_no",
        "column",
        "dtype",
        "encoding",
        "row_offset",
        "row_count",
        "original_rows",
        "key_min",
        "key_max",
        "null_bitmap_len",
        "body_len",
        "checksum",
    )
    if header.get("format") != BLOCK_FORMAT:
        raise _block_error(expected, "unsupported block format", actual_format=header.get("format"))
    for field in fields:
        expected_value = getattr(expected, field, None)
        if field in ("null_bitmap_len", "body_len", "checksum"):
            expected_value = getattr(expected, field)
        if header.get(field) != expected_value:
            raise _block_error(
                expected,
                f"header field mismatch: {field}",
                expected=expected_value,
                actual=header.get(field),
            )
    if header.get("dtype") not in SUPPORTED_DTYPES:
        raise _block_error(expected, "unsupported dtype in header", actual=header.get("dtype"))
    if header.get("encoding") not in SUPPORTED_ENCODINGS:
        raise _block_error(expected, "unsupported encoding in header", actual=header.get("encoding"))
    row_count = header.get("row_count")
    if not isinstance(row_count, int) or row_count <= 0:
        raise _block_error(expected, "invalid row count in header")
    if header.get("original_rows") != row_count:
        raise _block_error(expected, "original row count mismatch")
    if header.get("null_bitmap_len") != (row_count + 7) // 8:
        raise _block_error(expected, "invalid null bitmap length")
    if header.get("key_min") is None or header.get("key_max") is None:
        raise _block_error(expected, "missing primary-key range")
    if int(header["key_min"]) > int(header["key_max"]):
        raise _block_error(expected, "inverted primary-key range")


def load_verified_block(path: Path, expected: Any) -> tuple[dict[str, Any], bytes]:
    header = read_header(path, expected)
    _verify_header(header, expected)
    try:
        with path.open("rb") as handle:
            handle.read(len(BLOCK_MAGIC))
            header_length = struct.unpack(">I", handle.read(HEADER_LENGTH_WIDTH))[0]
            handle.read(header_length)
            body = handle.read()
    except OSError as exc:
        raise _block_error(expected, "block body cannot be read", path=str(path)) from exc
    if len(body) != header["body_len"]:
        raise _block_error(
            expected,
            "body length mismatch",
            expected=header["body_len"],
            actual=len(body),
        )
    if sha256_hex(body) != header["checksum"]:
        raise _block_error(expected, "body checksum mismatch")
    return header, body


def _slice_bounds(row_count: int, start: int, end: int | None) -> tuple[int, int]:
    actual_end = row_count if end is None else end
    if start < 0 or actual_end < start or actual_end > row_count:
        raise ValueError("decoded row range is outside the block")
    return start, actual_end


def _decode_fixed(
    expected: Any,
    body: bytes,
    null_bitmap_len: int,
    start: int,
    end: int,
) -> list[Any]:
    width = _WIDTH[expected.dtype]
    payload = body[null_bitmap_len:]
    row_count = expected.row_count
    if len(payload) != row_count * width:
        raise _block_error(expected, "fixed payload length mismatch")
    values: list[Any] = []
    fmt = "<" + _STRUCT_FORMAT[expected.dtype]
    for index in range(start, end):
        value = struct.unpack_from(fmt, payload, index * width)[0]
        if expected.dtype == "float64" and not math.isfinite(float(value)):
            raise _block_error(expected, "non-finite float in block", row_index=index)
        values.append(value)
    return values


def _decode_run(
    expected: Any,
    body: bytes,
    null_bitmap_len: int,
    start: int,
    end: int,
) -> tuple[list[Any], list[bool]]:
    cursor = null_bitmap_len
    if cursor + 4 > len(body):
        raise _block_error(expected, "truncated run header")
    run_count = struct.unpack_from("<I", body, cursor)[0]
    cursor += 4
    values: list[Any] = []
    run_null_flags: list[bool] = []
    row_cursor = 0
    for _ in range(run_count):
        if cursor + 5 > len(body):
            raise _block_error(expected, "truncated run record")
        run_length, is_null = struct.unpack_from("<IB", body, cursor)
        cursor += 5
        if run_length <= 0 or is_null not in (0, 1):
            raise _block_error(expected, "invalid run record")
        if row_cursor + run_length > expected.row_count:
            raise _block_error(expected, "run lengths exceed row count")
        value = None
        if not is_null:
            if expected.dtype in _WIDTH:
                width = _WIDTH[expected.dtype]
                if cursor + width > len(body):
                    raise _block_error(expected, "truncated run value")
                value = struct.unpack_from("<" + _STRUCT_FORMAT[expected.dtype], body, cursor)[0]
                cursor += width
            elif expected.dtype == "string":
                if cursor + 2 > len(body):
                    raise _block_error(expected, "truncated run string length")
                length = struct.unpack_from("<H", body, cursor)[0]
                cursor += 2
                if cursor + length > len(body):
                    raise _block_error(expected, "truncated run string")
                value = body[cursor : cursor + length].decode("utf-8")
                cursor += length
            else:
                raise _block_error(expected, "unsupported run dtype")
        overlap_start = max(start, row_cursor)
        overlap_end = min(end, row_cursor + run_length)
        if overlap_start < overlap_end:
            values.extend([value] * (overlap_end - overlap_start))
            run_null_flags.extend([bool(is_null)] * (overlap_end - overlap_start))
        row_cursor += run_length
    if row_cursor != expected.row_count:
        raise _block_error(expected, "run lengths do not cover the block")
    if cursor != len(body):
        raise _block_error(expected, "trailing bytes after run payload")
    return values, run_null_flags


def _decode_dict(
    expected: Any,
    body: bytes,
    null_bitmap_len: int,
    start: int,
    end: int,
) -> list[Any]:
    cursor = null_bitmap_len
    if cursor + 4 > len(body):
        raise _block_error(expected, "truncated dictionary header")
    dictionary_count = struct.unpack_from("<I", body, cursor)[0]
    cursor += 4
    dictionary: list[Any] = []
    for _ in range(dictionary_count):
        if expected.dtype in _WIDTH:
            width = _WIDTH[expected.dtype]
            if cursor + width > len(body):
                raise _block_error(expected, "truncated dictionary value")
            dictionary.append(struct.unpack_from("<" + _STRUCT_FORMAT[expected.dtype], body, cursor)[0])
            cursor += width
        elif expected.dtype == "string":
            if cursor + 2 > len(body):
                raise _block_error(expected, "truncated dictionary string length")
            length = struct.unpack_from("<H", body, cursor)[0]
            cursor += 2
            if cursor + length > len(body):
                raise _block_error(expected, "truncated dictionary string")
            dictionary.append(body[cursor : cursor + length].decode("utf-8"))
            cursor += length
        else:
            raise _block_error(expected, "unsupported dictionary dtype")
    index_offset = cursor
    expected_payload = expected.row_count * 4
    if len(body) - index_offset != expected_payload:
        raise _block_error(expected, "dictionary index payload length mismatch")
    values: list[Any] = []
    for index in range(start, end):
        dict_index = struct.unpack_from("<I", body, index_offset + index * 4)[0]
        if dict_index >= dictionary_count:
            raise _block_error(expected, "dictionary index is out of range", row_index=index)
        values.append(dictionary[dict_index])
    return values


def decode_block_slice(
    block_path: Path,
    expected: Any,
    start: int = 0,
    end: int | None = None,
) -> DecodedSlice:
    """Validate the full block checksum and decode only the requested row range."""
    header, body = load_verified_block(block_path, expected)
    start, end = _slice_bounds(expected.row_count, start, end)
    null_bitmap_len = header["null_bitmap_len"]
    null_bitmap = body[:null_bitmap_len]
    null_mask = unpack_null_bitmap(null_bitmap, expected.row_count)[start:end]
    fixed_payload_null_flags: list[bool] | None = None
    if expected.encoding == "fixed":
        values = _decode_fixed(expected, body, null_bitmap_len, start, end)
    elif expected.encoding == "run":
        values, fixed_payload_null_flags = _decode_run(expected, body, null_bitmap_len, start, end)
    elif expected.encoding == "dict":
        values = _decode_dict(expected, body, null_bitmap_len, start, end)
    else:
        raise _block_error(expected, "unsupported encoding")

    for relative_index, is_null in enumerate(null_mask):
        absolute_index = start + relative_index
        if is_null:
            values[relative_index] = None
        elif values[relative_index] is None:
            raise _block_error(expected, "null bitmap and payload disagree", row_index=absolute_index)
        if fixed_payload_null_flags is not None and fixed_payload_null_flags[relative_index] != is_null:
            raise _block_error(expected, "run null flag and null bitmap disagree", row_index=absolute_index)
    if expected.dtype == "int64":
        for value in values:
            if value is not None and (isinstance(value, bool) or not isinstance(value, int)):
                raise _block_error(expected, "decoded value has wrong type")
    elif expected.dtype == "float64":
        for value in values:
            if value is not None and (not isinstance(value, float) or not math.isfinite(value)):
                raise _block_error(expected, "decoded value has wrong type")
    elif expected.dtype == "string":
        for value in values:
            if value is not None and not isinstance(value, str):
                raise _block_error(expected, "decoded value has wrong type")
    return DecodedSlice(
        block_no=expected.block_no,
        column=expected.column,
        dtype=expected.dtype,
        encoding=expected.encoding,
        key_min=expected.key_min,
        key_max=expected.key_max,
        row_offset=expected.row_offset + start,
        values=list(values),
        null_mask=list(null_mask),
        null_bitmap_bytes=null_bitmap,
        row_offsets=list(range(expected.row_offset + start, expected.row_offset + end)),
    )


def decode_full_block(block_path: Path, expected: Any) -> DecodedSlice:
    return decode_block_slice(block_path, expected, 0, expected.row_count)


def iter_bits_as_bools(data: bytes, count: int) -> Iterable[bool]:
    for index in range(count):
        yield bool(data[index >> 3] & (1 << (index & 7)))
