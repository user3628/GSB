"""Generate deterministic local samples under samples/.

Run:  python scripts/generate_samples.py
"""
from __future__ import annotations

import struct
import sys
import zlib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.codecs import binary
from app.models import DecodedPayload, FormatName, RawRecord, SnapshotHeader

ROOT = Path(__file__).resolve().parent.parent
SAMPLES = ROOT / "samples"
GENERATED_AT = "2026-11-22T00:00:00Z"


def main() -> None:
    SAMPLES.mkdir(parents=True, exist_ok=True)

    # 1) Binary snapshot: native permille fixed-point weights, full bitmap set.
    binary_records = [
        RawRecord(
            resource_id="svc-gateway",
            parents=[],
            budget=3,
            weight_permille=900,
            expected_version=2,
            present_fields=["budget", "weight", "expected_version"],
        ),
        RawRecord(
            resource_id="svc-auth",
            parents=["svc-gateway"],
            budget=2,
            weight_permille=750,
            expected_version=2,
            present_fields=["budget", "weight", "expected_version", "parents"],
        ),
        RawRecord(
            resource_id="svc-orders",
            parents=["svc-auth", "svc-gateway"],
            budget=4,
            weight_permille=600,
            expected_version=2,
            present_fields=["budget", "weight", "expected_version", "parents"],
        ),
        RawRecord(
            resource_id="svc-billing",
            parents=["svc-orders"],
            budget=2,
            weight_permille=450,
            expected_version=2,
            present_fields=["budget", "weight", "expected_version", "parents"],
        ),
        RawRecord(
            resource_id="svc-notify",
            parents=["svc-orders"],
            budget=1,
            weight_permille=300,
            expected_version=2,
            present_fields=["budget", "weight", "expected_version", "parents"],
        ),
    ]
    binary_payload = DecodedPayload(
        header=SnapshotHeader(
            format=FormatName.BINARY,
            format_version=1,
            snapshot_seq=1,
            generated_at=GENERATED_AT,
        ),
        records=binary_records,
    )
    (SAMPLES / "snapshot_v1.bin").write_bytes(binary.encode(binary_payload))

    # 2) JSON-lines snapshot: float weights and records with missing fields.
    jsonl_lines = [
        '{"snapshot": true, "format": "jsonl", "format_version": 1, '
        f'"snapshot_seq": 2, "generated_at": "{GENERATED_AT}"}}',
        '{"resource_id": "svc-gateway", "parents": [], "budget": 3, "weight": 0.9005}',
        '{"resource_id": "svc-auth", "parents": ["svc-gateway"], "budget": 2, "weight": 0.7505}',
        # Missing weight and expected_version on this record.
        '{"resource_id": "svc-orders", "parents": ["svc-auth", "svc-gateway"], "budget": 4}',
        # Missing budget on this record.
        '{"resource_id": "svc-billing", "parents": ["svc-orders"], "weight": 0.4504}',
        '{"resource_id": "svc-notify", "parents": ["svc-orders"], "budget": 1, "weight": 0.3004}',
    ]
    (SAMPLES / "snapshot_v2.jsonl").write_text("\n".join(jsonl_lines) + "\n", encoding="utf-8")

    # 3) Legacy YAML subset: id + parents only, no weight anywhere.
    yaml_lines = [
        "# @format yaml",
        "# @format_version 1",
        "# @snapshot_seq 3",
        f"# @generated_at {GENERATED_AT}",
        "svc-gateway: []",
        "svc-auth: [svc-gateway]",
        "svc-orders: [svc-auth, svc-gateway]",
        "svc-billing: [svc-orders]",
        "svc-notify: [svc-orders]",
    ]
    (SAMPLES / "snapshot_v3.yaml").write_text("\n".join(yaml_lines) + "\n", encoding="utf-8")

    # 4) Cyclic graph sample (jsonl for easy inspection): orders <-> billing.
    cycle_lines = [
        '{"snapshot": true, "format": "jsonl", "format_version": 1, '
        f'"snapshot_seq": 4, "generated_at": "{GENERATED_AT}"}}',
        '{"resource_id": "svc-gateway", "parents": [], "budget": 3, "weight": 0.9}',
        '{"resource_id": "svc-orders", "parents": ["svc-gateway", "svc-billing"], "budget": 4, "weight": 0.6}',
        '{"resource_id": "svc-billing", "parents": ["svc-orders"], "budget": 2, "weight": 0.45}',
    ]
    (SAMPLES / "snapshot_cycle.jsonl").write_text("\n".join(cycle_lines) + "\n", encoding="utf-8")

    # 5) High-version binary carrying the unknown-required marker -> reject.
    high = bytearray(binary.encode(binary_payload))
    high[4] = 2  # format version byte
    high[5] |= 0x01  # flags: unknown required field present
    # Recompute header/body CRC after mutating version + flags bytes.
    body = bytes(high[binary.HEADER_SIZE :])
    head_for_crc = bytes(high[:binary.CRC_OFFSET])
    crc = zlib.crc32(head_for_crc + body) & 0xFFFFFFFF
    high[binary.CRC_OFFSET:binary.HEADER_SIZE] = struct.pack("!I", crc)
    (SAMPLES / "snapshot_v2_unsupported.bin").write_bytes(bytes(high))

    # 6) Corrupted binary (flipped payload byte) -> E_FORMAT_INVALID.
    corrupted = bytearray((SAMPLES / "snapshot_v1.bin").read_bytes())
    corrupted[-5] ^= 0xFF
    (SAMPLES / "snapshot_corrupt.bin").write_bytes(bytes(corrupted))

    print(f"wrote {len(list(SAMPLES.iterdir()))} samples to {SAMPLES}")


if __name__ == "__main__":
    main()
