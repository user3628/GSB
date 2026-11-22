from __future__ import annotations

import json
import struct
import sys
import zlib
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from joincost.model import canonical_json, finalize_snapshot, normalize_relation


def sample_bytes(name: str) -> bytes:
    return (ROOT / "samples" / name).read_bytes()


def predicates() -> list[dict[str, Any]]:
    return json.loads((ROOT / "samples" / "predicates.json").read_text(encoding="utf-8"))


def make_snapshot(
    *,
    version: int = 1,
    relations: list[tuple[str, int, Any, Any]] | None = None,
    stale: tuple[str, ...] = (),
):
    relation_specs = relations or [
        ("a", 1_000, 0.25, 1.0),
        ("b", 2_000, 0.50, 1.0),
        ("c", 500, 0.40, 1.0),
    ]
    normalized = [
        normalize_relation(
            name=name,
            row_count=rows,
            ndv_ratio_value=ratio,
            correlation_value=correlation,
            stale_keys=stale if index == 0 else (),
            source_kind="permille_integer",
            source_precision_places=3,
        )
        for index, (name, rows, ratio, correlation) in enumerate(relation_specs)
    ]
    return finalize_snapshot(
        version=version,
        relations=normalized,
        format_name="binary",
        format_version=2,
        decision="ACCEPT",
        degradations=[],
    )


def reseal_binary(data: bytes, body: bytes) -> bytes:
    order = "<" if data[5] == 0x4C else ">"
    (
        magic,
        format_version,
        endian_byte,
        header_size,
        flags,
        version,
        record_count,
        _old_body_length,
        _old_body_crc,
        _old_header_crc,
    ) = struct.unpack(f"{order}4sBBHIQIQII", data[:40])
    partial = struct.pack(
        f"{order}4sBBHIQIQ",
        magic,
        format_version,
        endian_byte,
        header_size,
        flags,
        version,
        record_count,
        len(body),
    )
    body_crc = zlib.crc32(body) & 0xFFFFFFFF
    partial += struct.pack(f"{order}I", body_crc)
    header_crc = zlib.crc32(partial) & 0xFFFFFFFF
    return partial + struct.pack(f"{order}I", header_crc) + body


def corrupt_first_record_length(data: bytes) -> bytes:
    body = bytearray(data[40:])
    body[:4] = struct.pack("<I", 0)
    return reseal_binary(data, bytes(body))


def mutate_jsonl(
    data: bytes,
    *,
    record_mutator=None,
    manifest_mutator=None,
) -> bytes:
    lines = [line for line in data.decode("utf-8").splitlines() if line.strip()]
    manifest = json.loads(lines[0])
    records = [json.loads(line) for line in lines[1:]]
    if record_mutator is not None:
        for record in records:
            record_mutator(record)
    repaired_records = []
    for record in records:
        payload = {key: value for key, value in record.items() if key != "record_crc32"}
        record["record_crc32"] = f"{zlib.crc32(canonical_json(payload).encode()) & 0xFFFFFFFF:08x}"
        repaired_records.append(canonical_json(record))
    records_blob = "\n".join(repaired_records) + "\n"
    manifest["records_crc32"] = f"{zlib.crc32(records_blob.encode()) & 0xFFFFFFFF:08x}"
    if manifest_mutator is not None:
        manifest_mutator(manifest)
    return (canonical_json(manifest) + "\n" + records_blob).encode("utf-8")

