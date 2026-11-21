"""Compact binary persistence format (ETRK).

File layout
-----------
::

    +--------------------------------------------------------------+
    | magic (4B = b"ETRK") | format_version (1B) | endian (1B)      |
    +--------------------------------------------------------------+
    | record 0: length_prefix(uint32) | payload | crc32(uint32)     |
    | record 1: length_prefix(uint32) | payload | crc32(uint32)     |
    | ...                                                           |
    | terminator: length_prefix == 0 (uint32)                       |
    +--------------------------------------------------------------+

Every multi-byte integer follows the header endian marker
(``0x01`` little-endian, ``0x02`` big-endian). Each record payload carries an
optional-field presence bitmap, so absent optional fields cost zero bytes.

Format versions
~~~~~~~~~~~~~~~
* ``BIN_V1 = 1``: metric timestamps stored as integer milliseconds (lower
  precision, sub-ms data is rounded); no downgrade-log field.
* ``BIN_V2 = 2`` (current): metric timestamps stored as integer microseconds;
  adds an optional downgrade-log field.
"""

from __future__ import annotations

import json
import struct
import zlib
from typing import Dict, Iterator, List, Tuple

from .errors import ErrorCode, TrackerError
from .models import Artifact, MetricPoint, Run
from .timeutil import ms_to_us, us_to_ms_grid, ROUND

MAGIC = b"ETRK"
ENDIAN_LE = 0x01
ENDIAN_BE = 0x02

BIN_V1 = 1
BIN_V2 = 2
BIN_CURRENT = BIN_V2
SUPPORTED_BIN_VERSIONS = (BIN_V1, BIN_V2)

# Optional-field presence bitmap bits.
BIT_PARAMS = 1 << 0
BIT_ARTIFACTS = 1 << 1
BIT_BASE_TIME = 1 << 2
BIT_RETENTION = 1 << 3
BIT_EXPIRES = 1 << 4
BIT_DOWNGRADES = 1 << 5

# Parameter value type tags.
TAG_NULL = 0
TAG_BOOL_TRUE = 1
TAG_BOOL_FALSE = 2
TAG_INT = 3
TAG_FLOAT = 4
TAG_STR = 5


class _Writer:
    def __init__(self, endian: str):
        self.endian = endian
        self.char = "<" if endian == "little" else ">"

    def u16(self, value: int) -> bytes:
        return struct.pack(self.char + "H", value)

    def u32(self, value: int) -> bytes:
        return struct.pack(self.char + "I", value)

    def i64(self, value: int) -> bytes:
        return struct.pack(self.char + "q", value)

    def u64(self, value: int) -> bytes:
        return struct.pack(self.char + "Q", value)

    def f64(self, value: float) -> bytes:
        return struct.pack(self.char + "d", float(value))

    def string(self, text: str) -> bytes:
        raw = text.encode("utf-8")
        return self.u16(len(raw)) + raw


class _Reader:
    def __init__(self, data: bytes, offset: int, endian: str):
        self.data = data
        self.pos = offset
        self.char = "<" if endian == "little" else ">"

    def take(self, size: int, what: str, record_index: int) -> bytes:
        if self.pos + size > len(self.data):
            raise TrackerError(
                ErrorCode.RECORD_CORRUPT,
                f"truncated payload while reading {what}",
                {"record_index": record_index, "field": what},
            )
        chunk = self.data[self.pos:self.pos + size]
        self.pos += size
        return chunk

    def u16(self, record_index: int) -> int:
        return struct.unpack(self.char + "H", self.take(2, "u16", record_index))[0]

    def u32(self, record_index: int) -> int:
        return struct.unpack(self.char + "I", self.take(4, "u32", record_index))[0]

    def i64(self, record_index: int) -> int:
        return struct.unpack(self.char + "q", self.take(8, "i64", record_index))[0]

    def u64(self, record_index: int) -> int:
        return struct.unpack(self.char + "Q", self.take(8, "u64", record_index))[0]

    def f64(self, record_index: int) -> float:
        return struct.unpack(self.char + "d", self.take(8, "f64", record_index))[0]

    def string(self, record_index: int) -> str:
        length = self.u16(record_index)
        return self.take(length, "string", record_index).decode("utf-8")


def _encode_value(writer: _Writer, value) -> bytes:
    if value is None:
        return bytes([TAG_NULL])
    if isinstance(value, bool):
        return bytes([TAG_BOOL_TRUE if value else TAG_BOOL_FALSE])
    if isinstance(value, int):
        return bytes([TAG_INT]) + writer.i64(value)
    if isinstance(value, float):
        return bytes([TAG_FLOAT]) + writer.f64(value)
    if isinstance(value, str):
        return bytes([TAG_STR]) + writer.string(value)
    raise TrackerError(
        ErrorCode.INVALID_REQUEST,
        "binary format cannot encode parameter value of unsupported type",
        {"type": type(value).__name__},
    )


def _decode_value(reader: _Reader, record_index: int):
    tag = reader.take(1, "value_tag", record_index)[0]
    if tag == TAG_NULL:
        return None
    if tag == TAG_BOOL_TRUE:
        return True
    if tag == TAG_BOOL_FALSE:
        return False
    if tag == TAG_INT:
        return reader.i64(record_index)
    if tag == TAG_FLOAT:
        return reader.f64(record_index)
    if tag == TAG_STR:
        return reader.string(record_index)
    raise TrackerError(
        ErrorCode.RECORD_CORRUPT,
        "unknown parameter value type tag",
        {"record_index": record_index, "tag": tag},
    )


def _encode_payload(run: Run, version: int, endian: str) -> bytes:
    writer = _Writer(endian)
    bitmap = 0
    if run.params:
        bitmap |= BIT_PARAMS
    if run.artifacts:
        bitmap |= BIT_ARTIFACTS
    if run.base_time_us is not None:
        bitmap |= BIT_BASE_TIME
    if run.retention_seconds is not None:
        bitmap |= BIT_RETENTION
    if run.expires_at_us is not None:
        bitmap |= BIT_EXPIRES
    if version >= BIN_V2 and run.downgrades:
        bitmap |= BIT_DOWNGRADES

    parts = [writer.u16(bitmap)]
    parts.append(writer.string(run.run_id))
    parts.append(writer.string(run.experiment))
    parts.append(writer.string(run.toolchain_version))
    parts.append(writer.u64(run.last_write_us))

    parts.append(writer.u32(len(run.metrics)))
    for point in run.metrics:
        parts.append(writer.i64(point.step))
        parts.append(writer.f64(point.value))
        if version >= BIN_V2:
            parts.append(writer.i64(point.time_us))
        else:
            parts.append(writer.i64(us_to_ms_grid(point.time_us, ROUND)))

    if bitmap & BIT_PARAMS:
        parts.append(writer.u32(len(run.params)))
        for key in sorted(run.params):
            parts.append(writer.string(str(key)))
            parts.append(_encode_value(writer, run.params[key]))

    if bitmap & BIT_ARTIFACTS:
        parts.append(writer.u32(len(run.artifacts)))
        for artifact in run.artifacts:
            parts.append(writer.string(artifact.name))
            parts.append(writer.u64(artifact.size_bytes))
            parts.append(writer.string(artifact.digest))

    if bitmap & BIT_BASE_TIME:
        parts.append(writer.i64(run.base_time_us))
    if bitmap & BIT_RETENTION:
        parts.append(writer.f64(run.retention_seconds))
    if bitmap & BIT_EXPIRES:
        parts.append(writer.i64(run.expires_at_us))
    if bitmap & BIT_DOWNGRADES:
        raw = json.dumps(run.downgrades, sort_keys=True).encode("utf-8")
        parts.append(writer.u32(len(raw)))
        parts.append(raw)
    return b"".join(parts)


def _decode_payload(payload: bytes, version: int, endian: str, record_index: int) -> Run:
    reader = _Reader(payload, 0, endian)
    bitmap = reader.u16(record_index)
    run_id = reader.string(record_index)
    experiment = reader.string(record_index)
    toolchain_version = reader.string(record_index)
    last_write_us = reader.u64(record_index)

    metric_count = reader.u32(record_index)
    metrics: List[MetricPoint] = []
    for _ in range(metric_count):
        step = reader.i64(record_index)
        value = reader.f64(record_index)
        time_field = reader.i64(record_index)
        time_us = time_field if version >= BIN_V2 else ms_to_us(time_field)
        metrics.append(MetricPoint(step=step, value=value, time_us=time_us))

    params: Dict[str, object] = {}
    artifacts: List[Artifact] = []
    base_time_us = None
    retention_seconds = None
    expires_at_us = None
    downgrades: List[dict] = []

    if bitmap & BIT_PARAMS:
        param_count = reader.u32(record_index)
        for _ in range(param_count):
            key = reader.string(record_index)
            params[key] = _decode_value(reader, record_index)

    if bitmap & BIT_ARTIFACTS:
        artifact_count = reader.u32(record_index)
        for _ in range(artifact_count):
            name = reader.string(record_index)
            size_bytes = reader.u64(record_index)
            digest = reader.string(record_index)
            artifacts.append(Artifact(name=name, size_bytes=size_bytes, digest=digest))

    if bitmap & BIT_BASE_TIME:
        base_time_us = reader.i64(record_index)
    if bitmap & BIT_RETENTION:
        retention_seconds = reader.f64(record_index)
    if bitmap & BIT_EXPIRES:
        expires_at_us = reader.i64(record_index)
    if bitmap & BIT_DOWNGRADES:
        if version < BIN_V2:
            raise TrackerError(
                ErrorCode.RECORD_CORRUPT,
                "downgrade bitmap set in v1 payload",
                {"record_index": record_index},
            )
        raw_len = reader.u32(record_index)
        raw = reader.take(raw_len, "downgrades", record_index)
        downgrades = json.loads(raw.decode("utf-8"))

    return Run(
        run_id=run_id,
        experiment=experiment,
        params=params,
        metrics=metrics,
        toolchain_version=toolchain_version,
        last_write_us=last_write_us,
        base_time_us=base_time_us,
        retention_seconds=retention_seconds,
        expires_at_us=expires_at_us,
        artifacts=artifacts,
        downgrades=downgrades,
    )


def encode_file(runs: List[Run], path: str, version: int = BIN_CURRENT,
                endian: str = "little") -> None:
    if version not in SUPPORTED_BIN_VERSIONS:
        raise TrackerError(
            ErrorCode.FORMAT_UNSUPPORTED,
            "cannot encode unsupported binary format version",
            {"version": version, "supported": list(SUPPORTED_BIN_VERSIONS)},
        )
    if endian not in ("little", "big"):
        raise TrackerError(ErrorCode.INVALID_REQUEST, "endian must be little or big", {})
    marker = ENDIAN_LE if endian == "little" else ENDIAN_BE
    writer = _Writer(endian)
    with open(path, "wb") as handle:
        handle.write(MAGIC + bytes([version, marker]))
        for index, run in enumerate(runs):
            payload = _encode_payload(run, version, endian)
            crc = zlib.crc32(payload) & 0xFFFFFFFF
            handle.write(writer.u32(len(payload)))
            handle.write(payload)
            handle.write(writer.u32(crc))
        handle.write(writer.u32(0))


def inspect_file(path: str) -> Dict[str, object]:
    """Read only the header; never mutates committed state."""
    with open(path, "rb") as handle:
        header = handle.read(6)
    if len(header) < 6 or header[:4] != MAGIC:
        raise TrackerError(
            ErrorCode.FORMAT_UNSUPPORTED,
            "not an ETRK binary file (bad magic)",
            {"path": path},
            http_status=415,
        )
    version = header[4]
    marker = header[5]
    if marker not in (ENDIAN_LE, ENDIAN_BE):
        raise TrackerError(
            ErrorCode.RECORD_CORRUPT,
            "invalid endian marker",
            {"marker": marker},
        )
    return {
        "kind": "binary",
        "format_version": version,
        "endian": "little" if marker == ENDIAN_LE else "big",
    }


def decode_file(path: str) -> Iterator[Tuple[int, Run]]:
    """Yield ``(record_index, run)``; raise E_RECORD_CORRUPT on any damage."""
    info = inspect_file(path)
    version = info["format_version"]
    endian = info["endian"]
    if version not in SUPPORTED_BIN_VERSIONS:
        raise TrackerError(
            ErrorCode.FORMAT_UNSUPPORTED,
            "binary format version is outside the compatibility matrix",
            {"version": version, "supported": list(SUPPORTED_BIN_VERSIONS)},
            http_status=415,
        )
    char = "<" if endian == "little" else ">"
    with open(path, "rb") as handle:
        handle.seek(6)
        blob = handle.read()

    index = 0
    pos = 0
    while pos < len(blob):
        if pos + 4 > len(blob):
            raise TrackerError(
                ErrorCode.RECORD_CORRUPT,
                "dangling length prefix at end of file",
                {"record_index": index},
            )
        length = struct.unpack(char + "I", blob[pos:pos + 4])[0]
        pos += 4
        if length == 0:
            return
        if pos + length + 4 > len(blob):
            raise TrackerError(
                ErrorCode.RECORD_CORRUPT,
                "record length prefix exceeds remaining file bytes",
                {"record_index": index, "declared_length": length},
            )
        payload = blob[pos:pos + length]
        pos += length
        stored_crc = struct.unpack(char + "I", blob[pos:pos + 4])[0]
        pos += 4
        actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
        if stored_crc != actual_crc:
            raise TrackerError(
                ErrorCode.RECORD_CORRUPT,
                "CRC32 mismatch",
                {
                    "record_index": index,
                    "expected_crc": actual_crc,
                    "stored_crc": stored_crc,
                },
            )
        yield index, _decode_payload(payload, version, endian, index)
        index += 1
