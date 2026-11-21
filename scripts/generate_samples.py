#!/usr/bin/env python3
from __future__ import annotations

import json
import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qos_workbench.codecs import decode_jsonl, encode_binary, encode_jsonl  # noqa: E402
from qos_workbench.models import Policy, PolicySnapshot  # noqa: E402


SAMPLES = ROOT / "samples"


def kbps(value: int) -> int:
    return value * 1000


def kb(value: int) -> int:
    return value * 1024


def build_snapshot() -> PolicySnapshot:
    timestamp_us = 1_700_000_000_123_456
    policies = (
        Policy(
            queue_id="wan",
            cir_bps=kbps(100_000),
            pir_bps=kbps(200_000),
            cbs_bytes=kb(256),
            pbs_bytes=kb(512),
            priority=0,
            match_rule="all traffic",
            parent_queue_id=None,
            timestamp_us=timestamp_us,
        ),
        Policy(
            queue_id="voice",
            cir_bps=kbps(2_000),
            pir_bps=kbps(4_000),
            cbs_bytes=kb(4),
            pbs_bytes=kb(8),
            priority=1,
            match_rule="udp and dst port 5060",
            parent_queue_id="wan",
            timestamp_us=timestamp_us,
        ),
        Policy(
            queue_id="video",
            cir_bps=kbps(10_000),
            pir_bps=kbps(20_000),
            cbs_bytes=kb(32),
            pbs_bytes=kb(64),
            priority=2,
            match_rule="udp and dst port 5004",
            parent_queue_id="wan",
            timestamp_us=timestamp_us,
        ),
        Policy(
            queue_id="bulk",
            cir_bps=kbps(20_000),
            pir_bps=kbps(40_000),
            cbs_bytes=kb(64),
            pbs_bytes=kb(128),
            priority=7,
            match_rule="tcp and dst port 443",
            parent_queue_id="wan",
            timestamp_us=timestamp_us,
        ),
    )
    return PolicySnapshot(
        format_version=3,
        snapshot_no=1,
        generated_at_us=1_700_000_000_123_456,
        policies=tuple(sorted(policies, key=lambda policy: policy.queue_id)),
    )


def build_v1_jsonl() -> bytes:
    policies = [
        {
            "format_version": 1,
            "snapshot_no": 1,
            "generated_at_ms": "1700000000123.456",
            "queue_id": "wan",
            "cir_kbps": 100000,
            "cbs_kb": 256,
            "priority": 0,
            "match_rule": "all traffic",
            "parent_queue_id": None,
            "timestamp_ms": "1700000000123.456",
        },
        {
            "format_version": 1,
            "snapshot_no": 1,
            "generated_at_ms": "1700000000123.456",
            "queue_id": "voice",
            "cir_kbps": 2000,
            "cbs_kb": 4,
            "priority": 1,
            "match_rule": "udp and dst port 5060",
            "parent_queue_id": "wan",
            "timestamp_ms": "1700000000123.456",
        },
        {
            "format_version": 1,
            "snapshot_no": 1,
            "generated_at_ms": "1700000000123.456",
            "queue_id": "video",
            "cir_kbps": 10000,
            "cbs_kb": 32,
            "priority": 2,
            "match_rule": "udp and dst port 5004",
            "parent_queue_id": "wan",
            "timestamp_ms": "1700000000123.456",
        },
        {
            "format_version": 1,
            "snapshot_no": 1,
            "generated_at_ms": "1700000000123.456",
            "queue_id": "bulk",
            "cir_kbps": 20000,
            "cbs_kb": 64,
            "priority": 7,
            "match_rule": "tcp and dst port 443",
            "parent_queue_id": "wan",
            "timestamp_ms": "1700000000123.456",
        },
    ]
    lines = []
    for item in policies:
        # Decimal-looking strings are deliberately kept as JSON numbers.
        text = json.dumps(item, ensure_ascii=False, separators=(",", ":"))
        text = text.replace('"1700000000123.456"', "1700000000123.456")
        lines.append(text)
    return ("\n".join(lines) + "\n").encode("utf-8")


def build_traffic() -> list[dict]:
    rows: list[dict] = []
    def add(queue_id: str, count: int, packet_bytes: int, timestamp: int) -> None:
        for index in range(count):
            if index % 11 == 10:
                color = "red"
            elif index % 7 == 6:
                color = "yellow"
            else:
                color = "green"
            rows.append(
                {
                    "timestamp_ms": timestamp,
                    "queue_id": queue_id,
                    "packet_bytes": packet_bytes,
                    "color": color,
                }
            )

    add("voice", 18, 1000, 0)
    add("voice", 12, 1000, 100)
    add("video", 60, 1500, 0)
    add("video", 20, 1500, 25)
    add("bulk", 120, 1500, 0)
    add("bulk", 25, 1500, 50)
    return rows


def main() -> None:
    SAMPLES.mkdir(parents=True, exist_ok=True)
    snapshot = build_snapshot()
    (SAMPLES / "policies_v3.jsonl").write_bytes(encode_jsonl(snapshot, version=3))
    (SAMPLES / "policies_v1.jsonl").write_bytes(build_v1_jsonl())
    (SAMPLES / "policies_v3_little_endian.bin").write_bytes(
        encode_binary(snapshot, version=3, endianness="little")
    )
    (SAMPLES / "policies_v3_big_endian.bin").write_bytes(
        encode_binary(snapshot, version=3, endianness="big")
    )
    (SAMPLES / "policies_v1_little_endian.bin").write_bytes(
        encode_binary(snapshot, version=1, endianness="little")
    )
    traffic_text = (
        "".join(
            json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
            for row in build_traffic()
        )
    )
    (SAMPLES / "traffic.jsonl").write_text(traffic_text, encoding="utf-8")
    (SAMPLES / "manifest.json").write_text(
        json.dumps(
            {
                "snapshot_no": snapshot.snapshot_no,
                "policy_snapshot_checksum": snapshot.checksum,
                "policy_count": len(snapshot.policies),
                "traffic_packet_count": len(build_traffic()),
                "traffic_sha256": hashlib.sha256(traffic_text.encode("utf-8")).hexdigest(),
                "deterministic": True,
                "policy_files": [
                    "policies_v3.jsonl",
                    "policies_v1.jsonl",
                    "policies_v3_little_endian.bin",
                    "policies_v3_big_endian.bin",
                    "policies_v1_little_endian.bin",
                ],
                "traffic_file": "traffic.jsonl",
                "units": {
                    "v3_jsonl": {"rate": "kbps", "burst": "KB", "timestamp": "ms"},
                    "binary": {"rate": "bps", "burst": "bytes", "timestamp": "us"},
                },
            },
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    # Verify the generated contradictory-unit formats normalize identically.
    from qos_workbench.codecs import decode_binary

    json_snapshot = decode_jsonl((SAMPLES / "policies_v3.jsonl").read_bytes()).snapshot
    little = decode_binary((SAMPLES / "policies_v3_little_endian.bin").read_bytes()).snapshot
    big = decode_binary((SAMPLES / "policies_v3_big_endian.bin").read_bytes()).snapshot
    assert json_snapshot == snapshot
    assert little == snapshot
    assert big == snapshot
    print(json.dumps({"status": "ok", "snapshot_checksum": snapshot.checksum}, indent=2))


if __name__ == "__main__":
    main()

