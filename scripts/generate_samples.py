#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kvstore.formats import encode_binary, jsonl_record_object  # noqa: E402
from kvstore.model import Record  # noqa: E402
from kvstore.snapshot import build_snapshot_document, snapshot_bytes  # noqa: E402


BASE_MS = 1_900_000_000_123


def fixed_records() -> list[Record]:
    # All values and timestamps are fixed; running this script twice produces
    # byte-identical samples. One record is already expired and one has no TTL.
    return [
        Record("alpha", b"domain-alpha-v1", 1, BASE_MS, BASE_MS + 86_400_000),
        Record("beta", b"domain-beta-v2", 2, BASE_MS + 1_000, None),
        Record("gamma", b"domain-gamma-expired", 1, BASE_MS + 2_000, BASE_MS - 1_000),
        Record("delta", b"domain-delta-ttl", 3, BASE_MS + 3_000, BASE_MS + 3_603_000),
        Record("epsilon", b"domain-epsilon-short", 1, BASE_MS + 4_000, BASE_MS + 5_000),
    ]


def jsonl_bytes(records: list[Record]) -> bytes:
    # Add deterministic sub-millisecond digits to demonstrate the declared
    # truncation rule. The binary fixture stores the already-truncated ms value.
    remainders = [456, 789, 123, 900, 1]
    lines: list[dict[str, object]] = []
    for index, record in enumerate(records):
        obj = jsonl_record_object(record, shard_count=2)
        write_us = record.write_ts_ms * 1000 + remainders[index]
        obj.pop("crc32", None)
        obj["write_timestamp_us"] = write_us
        obj["write_timestamp"] = write_us
        obj["write_timestamp_unit"] = "us"
        if record.key == "delta":
            # Relative TTL is based on the record-provided base timestamp.
            obj.pop("expires_at_us", None)
            obj.pop("expires_at", None)
            obj["base_time_us"] = write_us
            obj["ttl_ms"] = 3_600_000
        elif record.expires_at_ms is None:
            obj["expires_at_us"] = None
            obj["expires_at"] = None
        else:
            exp_us = record.expires_at_ms * 1000
            obj["expires_at_us"] = exp_us
            obj["expires_at"] = exp_us
        lines.append(obj)
    return (
        "\n".join(
            json.dumps(line, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            for line in lines
        )
        + "\n"
    ).encode("utf-8")


def main() -> None:
    sample_dir = ROOT / "samples"
    sample_dir.mkdir(parents=True, exist_ok=True)
    records = fixed_records()
    binary = encode_binary(records, endian="little")
    jsonl = jsonl_bytes(records)
    corrupt = bytearray(binary)
    corrupt[-1] ^= 0xFF  # last CRC byte of the final record
    snapshot_doc = build_snapshot_document(
        records=records[:2],
        tombstones=[],
        snapshot_seq=7,
        generated_at_ms=BASE_MS + 10_000,
        state_version=7,
        base_snapshot_seq=0,
    )

    files = {
        "sample.kvs.bin": binary,
        "sample.jsonl": jsonl,
        "sample_corrupt.kvs.bin": bytes(corrupt),
        "sample_snapshot.json": snapshot_bytes(snapshot_doc),
    }
    manifest: dict[str, object] = {
        "generated_by": "scripts/generate_samples.py",
        "deterministic": True,
        "base_time_ms": BASE_MS,
        "records": [
            {
                "key": record.key,
                "value": record.value.decode("utf-8"),
                "version": record.version,
                "write_timestamp_ms": record.write_ts_ms,
                "expires_at_ms": record.expires_at_ms,
            }
            for record in records
        ],
        "files": {},
    }
    for name, payload in files.items():
        path = sample_dir / name
        path.write_bytes(payload)
        manifest["files"][name] = {
            "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
        }
    (sample_dir / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"ok": True, "sample_dir": str(sample_dir), "files": manifest["files"]}, indent=2))


if __name__ == "__main__":
    main()

