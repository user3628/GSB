#!/usr/bin/env python3
"""Generate deterministic binary/JSONL diagnostic samples and corrupt variants."""

from __future__ import annotations

import hashlib
import json
import struct
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from diaglab.formats import encode_binary, encode_jsonl  # noqa: E402
from diaglab.models import DiagnosticRecord  # noqa: E402


SAMPLES = ROOT / "samples"
BASE_MS = 1_704_067_200_000  # 2024-01-01T00:00:00Z


def rec(
    ordinal: int,
    path: str,
    probe: str,
    *,
    timestamp_ms: int,
    rtt_us: int,
    success: bool = True,
    failure: str = "none",
    payload: int = 64,
    rtt_missing: bool = False,
    payload_missing: bool = False,
) -> DiagnosticRecord:
    return DiagnosticRecord(
        timestamp_ms=timestamp_ms,
        probe_id=probe,
        path_node=path,
        protocol="tcp",
        success=success,
        rtt_us=rtt_us,
        payload_bytes=payload,
        failure_type=failure,
        ordinal=ordinal,
        source_format="sample",
        source_version=2,
        rtt_missing=rtt_missing,
        payload_missing=payload_missing,
    )


def build_full_records() -> list[DiagnosticRecord]:
    records: list[DiagnosticRecord] = []
    a_rtts = [1200, 1260, 1310, 1250, 4500, None, 1380, 1500]
    for index, rtt in enumerate(a_rtts):
        failed = rtt is None
        records.append(
            rec(
                len(records),
                "local-edge-a",
                f"probe-a-{index + 1:03d}",
                timestamp_ms=BASE_MS + index * 1_000,
                rtt_us=0 if failed else int(rtt),
                success=not failed,
                failure="timeout" if failed else "none",
                payload=64,
            )
        )
    b_rtts = [2100, None, 2200, 2050, 2300, None, 2150]
    for index, rtt in enumerate(b_rtts):
        failed = rtt is None
        records.append(
            rec(
                len(records),
                "local-edge-b",
                f"probe-b-{index + 1:03d}",
                timestamp_ms=BASE_MS + 10_000 + index * 1_000,
                rtt_us=0 if failed else int(rtt),
                success=not failed,
                failure="refused" if failed else "none",
                payload=64,
            )
        )
    for index, rtt in enumerate([900, 940, 920, 960, 930, 950]):
        records.append(
            rec(
                len(records),
                "local-edge-c",
                f"probe-c-{index + 1:03d}",
                timestamp_ms=BASE_MS + 20_000 + index * 1_000,
                rtt_us=rtt,
                payload=64,
            )
        )
    return records


def build_v1_records() -> list[DiagnosticRecord]:
    return [
        rec(
            0,
            "legacy-binary",
            "legacy-001",
            timestamp_ms=BASE_MS + 30_000,
            rtt_us=0,
            payload=128,
            rtt_missing=True,
        ),
        rec(
            1,
            "legacy-binary",
            "legacy-002",
            timestamp_ms=BASE_MS + 31_000,
            rtt_us=1800,
            payload=0,
            payload_missing=True,
        ),
        rec(
            2,
            "legacy-binary",
            "legacy-003",
            timestamp_ms=BASE_MS + 32_000,
            rtt_us=0,
            payload=0,
            rtt_missing=True,
            payload_missing=True,
        ),
        rec(
            3,
            "legacy-binary",
            "legacy-004",
            timestamp_ms=BASE_MS + 33_000,
            rtt_us=1900,
            payload=128,
        ),
    ]


def write_jsonl(path: Path, items: list[dict]) -> None:
    path.write_text(
        "\n".join(json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":")) for item in items)
        + "\n",
        encoding="utf-8",
    )


def build_json_v2_items() -> list[dict]:
    rtts_ms = [2, 3, 2, 4, 3, 2]
    items: list[dict] = []
    for index, rtt_ms in enumerate(rtts_ms):
        timestamp_us = (BASE_MS + 40_000 + index * 1_000) * 1_000 + 200 + index * 37
        items.append(
            {
                "format": "jsonl",
                "version": 2,
                "timestamp_us": timestamp_us,
                "probe_id": f"json-probe-{index + 1:03d}",
                "path_node": "local-edge-d",
                "protocol": "tcp",
                "success": True,
                "failure_type": "none",
                "rtt_value": rtt_ms,
                "rtt_unit": "ms",
                "payload_value": 1,
                "payload_unit": "KiB",
            }
        )
    return items


def build_json_v1_items() -> list[dict]:
    return [
        {
            "format": "jsonl",
            "version": 1,
            "timestamp_us": (BASE_MS + 60_000) * 1_000 + 750,
            "probe_id": "legacy-json-001",
            "path_node": "legacy-json",
            "protocol": "tcp",
            "success": True,
            "failure_type": "none",
            "payload_value": 2,
            "payload_unit": "KiB",
        },
        {
            "format": "jsonl",
            "version": 1,
            "timestamp_us": (BASE_MS + 61_000) * 1_000 + 100,
            "probe_id": "legacy-json-002",
            "path_node": "legacy-json",
            "protocol": "tcp",
            "success": False,
            "failure_type": "timeout",
        },
        {
            "format": "jsonl",
            "version": 1,
            "timestamp_us": (BASE_MS + 62_000) * 1_000 + 999,
            "probe_id": "legacy-json-003",
            "path_node": "legacy-json",
            "protocol": "tcp",
            "success": True,
            "failure_type": "none",
            "rtt_value": 1700,
            "rtt_unit": "us",
        },
    ]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    SAMPLES.mkdir(parents=True, exist_ok=True)
    full = build_full_records()

    binary_v2 = SAMPLES / "binary_v2.bin"
    binary_v2.write_bytes(encode_binary(full, version=2, endian="little"))

    binary_big = SAMPLES / "binary_big_endian_v2.bin"
    binary_big.write_bytes(encode_binary(full, version=2, endian="big"))

    binary_v1 = SAMPLES / "binary_v1_degraded.bin"
    binary_v1.write_bytes(encode_binary(build_v1_records(), version=1, endian="little"))

    bad_crc = SAMPLES / "binary_bad_crc.bin"
    corrupted = bytearray(binary_v2.read_bytes())
    corrupted[-1] ^= 0x5A
    bad_crc.write_bytes(bytes(corrupted))

    bad_length = SAMPLES / "binary_bad_length.bin"
    corrupted_length = bytearray(binary_v2.read_bytes())
    struct.pack_into("<I", corrupted_length, 16, 0x7FFFFFFF)
    bad_length.write_bytes(bytes(corrupted_length))

    binary_v3 = SAMPLES / "binary_v3_unsupported.bin"
    unsupported = bytearray(binary_v2.read_bytes())
    unsupported[4] = 3
    binary_v3.write_bytes(bytes(unsupported))

    json_v2 = SAMPLES / "jsonl_v2.jsonl"
    write_jsonl(json_v2, build_json_v2_items())

    json_v1 = SAMPLES / "jsonl_v1_degraded.jsonl"
    write_jsonl(json_v1, build_json_v1_items())

    json_bad_unit = SAMPLES / "jsonl_unit_invalid.jsonl"
    invalid_items = build_json_v2_items()[:1]
    invalid_items[0]["payload_value"] = -1
    write_jsonl(json_bad_unit, invalid_items)

    json_v3 = SAMPLES / "jsonl_v3_unsupported.jsonl"
    unknown_items = build_json_v2_items()[:1]
    unknown_items[0]["version"] = 3
    unknown_items[0]["required_features"] = ["future_transport_metadata"]
    write_jsonl(json_v3, unknown_items)

    json_required = SAMPLES / "jsonl_unknown_required.jsonl"
    required_items = build_json_v2_items()[:1]
    required_items[0]["required_features"] = ["tls_handshake_metadata"]
    write_jsonl(json_required, required_items)

    manifest = {
        "generated_by": "scripts/make_samples.py",
        "deterministic_base_timestamp_ms": BASE_MS,
        "files": {},
    }
    for path in sorted(SAMPLES.glob("binary_*")) + sorted(SAMPLES.glob("jsonl_*")):
        manifest["files"][path.name] = {
            "bytes": path.stat().st_size,
            "sha256": sha256(path),
        }
    (SAMPLES / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

