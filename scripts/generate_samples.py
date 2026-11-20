from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kvstore.codec_binary import encode_binary
from kvstore.model import Record


SAMPLES = ROOT / "samples"


def main() -> int:
    SAMPLES.mkdir(parents=True, exist_ok=True)
    binary_records = [
        Record(
            key="config:region",
            value="cn-east",
            version=2,
            written_at_us=4_100_000_001_000_000,
            expires_at_us=4_200_000_000_000_000,
        ),
        Record(
            key="device:mode",
            value="eco",
            version=1,
            written_at_us=4_100_000_000_000_000,
            expires_at_us=None,
        ),
        Record(
            key="device:threshold",
            value=10,
            version=2,
            written_at_us=4_100_000_000_400_000,
            expires_at_us=None,
        ),
        Record(
            key="sensor:temp",
            value=21.5,
            version=1,
            written_at_us=4_100_000_000_000_000,
            expires_at_us=4_200_000_000_000_000,
        ),
    ]
    (SAMPLES / "binary_sample.kvsb").write_bytes(encode_binary(binary_records, endian="little"))

    jsonl_records = [
        {
            "key": "device:threshold",
            "value": 20,
            "version": 2,
            "written_at_us": 4_100_000_000_500_000,
            "ttl_us": 600_000_000,
        },
        {
            "key": "sensor:temp",
            "value": 22.5,
            "version": 2,
            "written_at_us": 4_100_000_002_000_000,
        },
        {
            "key": "config:region",
            "value": "eu-west",
            "version": 1,
            "written_at_us": 4_100_000_003_000_000,
        },
        {
            "key": "feature:beta",
            "value": True,
            "version": 1,
            "written_at_us": 4_100_000_004_000_000,
            "ttl_ms": 900_000,
            "source": "external-tool",
        },
        {
            "key": "dup:key",
            "value": "first",
            "version": 3,
            "written_at_us": 4_100_000_005_000_000,
        },
        {
            "key": "dup:key",
            "value": "second",
            "version": 3,
            "written_at_us": 4_100_000_005_000_000,
        },
        {
            "key": "device:mode",
            "value": "eco",
            "version": 1,
            "written_at_us": 4_100_000_000_000_000,
        },
    ]
    lines = [json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":")) for item in jsonl_records]
    (SAMPLES / "jsonl_sample.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")

    expected = {
        "precondition": "Import binary_sample.kvsb before jsonl_sample.jsonl into an otherwise empty store.",
        "jsonl_record_outcomes": [
            {
                "record": 1,
                "key": "device:threshold",
                "outcome": "overwrite",
                "reason": "newer_write_time",
                "note": "same version=2 and different value, incoming written_at_us is newer",
            },
            {
                "record": 2,
                "key": "sensor:temp",
                "outcome": "overwrite",
                "reason": "higher_version",
                "filled_fields": ["expires_at_us"],
            },
            {
                "record": 3,
                "key": "config:region",
                "outcome": "skip",
                "reason": "lower_version",
            },
            {
                "record": 4,
                "key": "feature:beta",
                "outcome": "overwrite",
                "reason": "new_key",
                "unknown_fields": ["source"],
            },
            {
                "record": 5,
                "key": "dup:key",
                "outcome": "reject",
                "reason": "unresolved_same_version_and_write_time",
            },
            {
                "record": 6,
                "key": "dup:key",
                "outcome": "reject",
                "reason": "unresolved_same_version_and_write_time",
            },
            {
                "record": 7,
                "key": "device:mode",
                "outcome": "skip",
                "reason": "duplicate_record",
                "filled_fields": ["expires_at_us"],
            },
        ],
        "expected_summary": {
            "input_records": 7,
            "overwrite": 3,
            "skip": 2,
            "reject": 2,
            "committed_records": 3,
            "rejected_keys": ["dup:key"],
        },
        "time_mapping": {
            "binary": "milliseconds -> internal microseconds",
            "json_lines": "microseconds kept; ttl_us/ttl_ms converted using each record's written_at_us",
        },
    }
    (SAMPLES / "expected_conflicts.json").write_text(
        json.dumps(expected, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps({"binary_bytes": (SAMPLES / "binary_sample.kvsb").stat().st_size, "jsonl_records": len(jsonl_records)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
