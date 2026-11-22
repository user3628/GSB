#!/usr/bin/env python3
"""Generate every deterministic local sample used by tests and the demo."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path
from typing import Any

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from slo_budget import codec  # noqa: E402
from slo_budget.binary_format import (  # noqa: E402
    HEADER_SIZE,
    write_binary_snapshot,
)
from slo_budget.calculator import calculate  # noqa: E402
from slo_budget.events import aggregate_events_to_windows  # noqa: E402
from slo_budget.jsonl_format import write_jsonl_snapshot  # noqa: E402
from slo_budget.models import (  # noqa: E402
    RequestEvent,
    SnapshotManifest,
    WindowRecord,
    dumps_json,
    sorted_records,
)


SEED = 20261123
BUCKET_MS = 5 * 60 * 1000
BASE_MS = 1_767_225_600_000  # 2026-01-01T00:00:00Z
AS_OF_MS = BASE_MS + 60 * 60 * 1000
RECORD_COUNT = 48
CURRENT_SEQUENCE = 2026112301
LEGACY_SEQUENCE = 2026112201
FUTURE_SEQUENCE = 2026112302


SERIES: list[dict[str, Any]] = [
    {
        "service": "checkout-api",
        "sli_type": "availability",
        "errors": [24, 21, 27, 22, 26, 23, 25, 24, 28, 21, 23, 26],
        "lateth": [2, 1, 3, 4, 2, 1, 0, 3, 2, 4, 1, 2],
    },
    {
        "service": "checkout-api",
        "sli_type": "latency",
        "errors": [3, 4, 3, 4, 5, 3, 4, 3, 3, 4, 3, 5],
        "lateth": [1, 2, 0, 3, 1, 2, 1, 0, 2, 1, 3, 1],
    },
    {
        "service": "search-api",
        "sli_type": "availability",
        "errors": [1, 0, 2, 1, 0, 1, 1, 0, 1, 0, 1, 0],
        "lateth": [0, 1, 0, 2, 1, 0, 1, 0, 2, 1, 0, 1],
    },
    {
        "service": "search-api",
        "sli_type": "latency",
        "errors": [2, 1, 2, 3, 1, 2, 1, 2, 1, 2, 1, 2],
        "lateth": [1, 0, 2, 1, 0, 1, 2, 1, 0, 2, 1, 0],
    },
]


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(text, encoding="utf-8", newline="\n")
    tmp.replace(path)


def build_events(rng: np.random.Generator) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for bucket_index in range(12):
        bucket_start = BASE_MS + bucket_index * BUCKET_MS
        for series in SERIES:
            total_events = 300
            error_count = int(series["errors"][bucket_index])
            late_count = int(series["lateth"][bucket_index])
            if not (0 <= error_count <= total_events):
                raise AssertionError("error count outside bucket")
            if not (0 <= late_count <= total_events):
                raise AssertionError("late count outside bucket")
            offsets = rng.choice(BUCKET_MS, size=total_events, replace=False)
            for event_index, offset in enumerate(offsets):
                events.append(
                    {
                        "timestamp_ms": bucket_start + int(offset),
                        "service": series["service"],
                        "sli_type": series["sli_type"],
                        "good": event_index >= error_count,
                        "late": event_index < late_count,
                    }
                )
    events.sort(
        key=lambda item: (
            item["timestamp_ms"],
            item["service"],
            item["sli_type"],
            item["good"],
            item["late"],
        )
    )
    return events


def write_events(path: Path, events: list[dict[str, Any]]) -> None:
    lines = [
        json.dumps(item, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        for item in events
    ]
    write_text(path, "\n".join(lines) + "\n")


def assert_counts(records: tuple[WindowRecord, ...]) -> None:
    expected = {
        (series["service"], series["sli_type"]): series for series in SERIES
    }
    for record in records:
        series = expected[(record.service, record.sli_type)]
        bucket_index = (record.window_start_ms - BASE_MS) // BUCKET_MS
        if record.total_events != 300:
            raise AssertionError("aggregated total mismatch")
        if record.good_events != 300 - int(series["errors"][bucket_index]):
            raise AssertionError("aggregated good mismatch")
        if record.late_count != int(series["lateth"][bucket_index]):
            raise AssertionError("aggregated late mismatch")


def query_expectations(records: tuple[WindowRecord, ...]) -> dict[str, Any]:
    expected: dict[str, Any] = {}
    for series in SERIES:
        result = calculate(
            records,
            service=series["service"],
            sli_type=series["sli_type"],
            as_of_ms=AS_OF_MS,
        )
        expected.setdefault(series["service"], {})[series["sli_type"]] = {
            "alert_grade": result["alert"]["grade"],
            "alert_code": result["alert"]["business_code"],
            "windows": {
                str(window["window_ms"]): {
                    "sli": window["sli"],
                    "burn_rate": window["error_budget"]["burn_rate"],
                    "good_events": window["counts"]["good_events"],
                    "total_events": window["counts"]["total_events"],
                    "late_count": window["counts"]["late_count"],
                    "sample_status": window["sample_gate"]["status"],
                }
                for window in result["windows"]
            },
        }
    return expected


def file_record(name: str, path: Path, base: Path) -> dict[str, Any]:
    return {
        "path": path.relative_to(base).as_posix(),
        "name": name,
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def generate(out_dir: Path) -> dict[str, Any]:
    windows_dir = out_dir / "windows"
    events_dir = out_dir / "events"
    corrupt_dir = out_dir / "corrupt"
    invalid_dir = out_dir / "invalid"
    conflicts_dir = out_dir / "conflicts"
    for directory in (windows_dir, events_dir, corrupt_dir, invalid_dir, conflicts_dir):
        directory.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(SEED)
    events = build_events(rng)
    events_path = events_dir / "requests.jsonl"
    write_events(events_path, events)

    requests = [
        RequestEvent(
            timestamp_ms=item["timestamp_ms"],
            service=item["service"],
            sli_type=item["sli_type"],
            good=item["good"],
            late=item["late"],
        )
        for item in events
    ]
    records = sorted_records(
        aggregate_events_to_windows(
            requests,
            bucket_ms=BUCKET_MS,
            generated_at_ms=AS_OF_MS,
        )
    )
    if len(records) != RECORD_COUNT:
        raise AssertionError(f"expected {RECORD_COUNT} records, got {len(records)}")
    assert_counts(records)

    current_manifest = SnapshotManifest(
        file_format="binary",
        format_version=2,
        snapshot_sequence=CURRENT_SEQUENCE,
        generated_at_ms=AS_OF_MS,
        record_count=len(records),
    )
    current_bin = windows_dir / "current-v2.bin"
    write_binary_snapshot(current_bin, records, current_manifest, endian="little")

    json_remainders = {
        record.key: ((index * 137 + 11) % 999) + 1
        for index, record in enumerate(records)
    }
    current_jsonl = windows_dir / "current-v2.jsonl"
    write_jsonl_snapshot(
        current_jsonl,
        records,
        SnapshotManifest(
            file_format="jsonl",
            format_version=2,
            snapshot_sequence=CURRENT_SEQUENCE,
            generated_at_ms=AS_OF_MS,
            record_count=len(records),
        ),
        timestamp_remainders_us=json_remainders,
    )

    legacy_manifest = SnapshotManifest(
        file_format="binary",
        format_version=1,
        snapshot_sequence=LEGACY_SEQUENCE,
        generated_at_ms=AS_OF_MS,
        record_count=len(records),
    )
    legacy_bin = windows_dir / "legacy-v1.bin"
    write_binary_snapshot(legacy_bin, records, legacy_manifest, version=1)
    relative_remainders = {
        record.key: (
            ((index * 53 + 7) % 999) + 1
            if record.window_end_ms < AS_OF_MS
            else 0
        )
        for index, record in enumerate(records)
    }
    legacy_jsonl = windows_dir / "legacy-v1.jsonl"
    write_jsonl_snapshot(
        legacy_jsonl,
        records,
        SnapshotManifest(
            file_format="jsonl",
            format_version=1,
            snapshot_sequence=LEGACY_SEQUENCE,
            generated_at_ms=AS_OF_MS,
            record_count=len(records),
        ),
        version=1,
        relative_remainders_us=relative_remainders,
    )

    future_bin = windows_dir / "future-v3-required.bin"
    write_binary_snapshot(
        future_bin,
        records,
        SnapshotManifest(
            file_format="binary",
            format_version=3,
            snapshot_sequence=FUTURE_SEQUENCE,
            generated_at_ms=AS_OF_MS,
            record_count=len(records),
        ),
        version=3,
    )
    future_jsonl = windows_dir / "future-v3-required.jsonl"
    write_jsonl_snapshot(
        future_jsonl,
        records,
        SnapshotManifest(
            file_format="jsonl",
            format_version=3,
            snapshot_sequence=FUTURE_SEQUENCE,
            generated_at_ms=AS_OF_MS,
            record_count=len(records),
        ),
        version=3,
    )

    current_bytes = current_bin.read_bytes()
    truncated_bin = corrupt_dir / "current-v2-truncated.bin"
    write_bytes(truncated_bin, current_bytes[: HEADER_SIZE + 19])
    bad_crc_bytes = bytearray(current_bytes)
    bad_crc_offset = HEADER_SIZE + 18
    bad_crc_bytes[bad_crc_offset] ^= 0x5A
    bad_crc = corrupt_dir / "current-v2-bad-crc.bin"
    write_bytes(bad_crc, bytes(bad_crc_bytes))
    current_jsonl_bytes = current_jsonl.read_bytes()
    truncated_jsonl = corrupt_dir / "current-v2-truncated.jsonl"
    write_bytes(truncated_jsonl, current_jsonl_bytes[:-31])

    invalid_record = WindowRecord(
        service="checkout-api",
        sli_type="availability",
        window_start_ms=BASE_MS + 11 * BUCKET_MS,
        window_length_ms=BUCKET_MS,
        good_events=301,
        total_events=300,
        late_count=0,
        last_write_ms=AS_OF_MS,
        format_version=2,
    )
    invalid_jsonl = invalid_dir / "count-invalid.jsonl"
    write_jsonl_snapshot(
        invalid_jsonl,
        [invalid_record],
        SnapshotManifest(
            file_format="jsonl",
            format_version=2,
            snapshot_sequence=CURRENT_SEQUENCE,
            generated_at_ms=AS_OF_MS,
            record_count=1,
        ),
    )

    overwrite_source = next(
        record
        for record in records
        if record.service == "checkout-api"
        and record.sli_type == "availability"
        and record.window_start_ms == BASE_MS + 11 * BUCKET_MS
    )
    overwrite_record = WindowRecord(
        **{
            **overwrite_source.internal_dict(),
            "late_count": overwrite_source.late_count + 1,
            "last_write_ms": overwrite_source.last_write_ms,
            "format_version": 2,
        }
    )
    merge_overwrite = conflicts_dir / "merge-overwrite.jsonl"
    write_jsonl_snapshot(
        merge_overwrite,
        [overwrite_record, records[0]],
        SnapshotManifest(
            file_format="jsonl",
            format_version=2,
            snapshot_sequence=CURRENT_SEQUENCE,
            generated_at_ms=AS_OF_MS,
            record_count=2,
        ),
    )
    reject_record = WindowRecord(
        **{
            **overwrite_source.internal_dict(),
            "good_events": overwrite_source.good_events - 1,
            "last_write_ms": overwrite_source.last_write_ms,
            "format_version": 2,
        }
    )
    merge_reject = conflicts_dir / "merge-reject.jsonl"
    write_jsonl_snapshot(
        merge_reject,
        [reject_record, records[1]],
        SnapshotManifest(
            file_format="jsonl",
            format_version=2,
            snapshot_sequence=CURRENT_SEQUENCE,
            generated_at_ms=AS_OF_MS,
            record_count=2,
        ),
    )

    decoded_current_bin = codec.decode(current_bin)
    decoded_current_jsonl = codec.decode(current_jsonl)
    decoded_legacy_bin = codec.decode(legacy_bin)
    decoded_legacy_jsonl = codec.decode(legacy_jsonl)
    binary_internal = [record.internal_dict() for record in decoded_current_bin.records]
    jsonl_internal = [record.internal_dict() for record in decoded_current_jsonl.records]
    if binary_internal != jsonl_internal:
        raise AssertionError("current binary and JSONL internal records differ")
    if any(record.late_count != 0 for record in decoded_legacy_bin.records):
        raise AssertionError("legacy binary late_count should degrade to zero")
    if any(record.late_count != 0 for record in decoded_legacy_jsonl.records):
        raise AssertionError("legacy JSONL late_count should degrade to zero")
    if [record.window_start_ms for record in decoded_legacy_bin.records] != [
        record.window_start_ms for record in records
    ]:
        raise AssertionError("legacy binary relative-time conversion mismatch")
    if [record.window_start_ms for record in decoded_legacy_jsonl.records] != [
        record.window_start_ms for record in records
    ]:
        raise AssertionError("legacy JSONL relative-time conversion mismatch")

    sample_files = [
        events_path,
        current_bin,
        current_jsonl,
        legacy_bin,
        legacy_jsonl,
        future_bin,
        future_jsonl,
        truncated_bin,
        bad_crc,
        truncated_jsonl,
        invalid_jsonl,
        merge_overwrite,
        merge_reject,
    ]
    hashes = {
        path.relative_to(out_dir).as_posix(): {
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
        for path in sample_files
    }

    def expected_code(path: Path) -> tuple[str | None, str | None, str]:
        try:
            negotiation = codec.preflight(path)
            return negotiation.verdict, negotiation.code, "preflight"
        except Exception as error:  # noqa: BLE001 - fixture summary captures business code
            return None, getattr(error, "code", type(error).__name__), "exception"

    expectations = {}
    for path in sample_files:
        verdict, code, source = expected_code(path)
        expectations[path.relative_to(out_dir).as_posix()] = {
            "verdict": verdict,
            "business_code": code,
            "result_source": source,
        }
    expectations[current_jsonl.relative_to(out_dir).as_posix()] = {
        "verdict": "ACCEPT",
        "business_code": None,
        "result_source": "preflight",
    }
    expectations[legacy_jsonl.relative_to(out_dir).as_posix()] = {
        "verdict": "DEGRADE",
        "business_code": None,
        "result_source": "preflight",
    }
    expectations[invalid_jsonl.relative_to(out_dir).as_posix()] = {
        "verdict": "ACCEPT",
        "business_code": "E_COUNT_INVALID",
        "result_source": "decode",
    }
    for corrupt_path in (truncated_bin, bad_crc, truncated_jsonl):
        expectations[corrupt_path.relative_to(out_dir).as_posix()] = {
            "verdict": None,
            "business_code": "E_RECORD_CORRUPT",
            "result_source": "decode",
        }
    expectations[merge_overwrite.relative_to(out_dir).as_posix()] = {
        "verdict": "ACCEPT",
        "business_code": None,
        "result_source": "import",
        "import_outcome": {"overwritten": 1, "skipped": 1, "rejected": 0},
    }
    expectations[merge_reject.relative_to(out_dir).as_posix()] = {
        "verdict": "ACCEPT",
        "business_code": "E_IMPORT_CONFLICT",
        "result_source": "import",
        "import_outcome": {"overwritten": 0, "skipped": 1, "rejected": 1, "status": "ROLLED_BACK"},
    }

    summary = {
        "schema_version": 1,
        "generated_by": "scripts/generate_samples.py",
        "seed": SEED,
        "time": {
            "base_ms": BASE_MS,
            "as_of_ms": AS_OF_MS,
            "bucket_ms": BUCKET_MS,
            "buckets": 12,
            "window_end_boundary": "exclusive",
            "window_start_boundary": "inclusive",
            "late_tolerance_ms": 60 * 60 * 1000,
            "late_tolerance_boundary": "inclusive",
        },
        "versions": {
            "current": 2,
            "legacy": 1,
            "future": 3,
            "current_snapshot_sequence": CURRENT_SEQUENCE,
            "legacy_snapshot_sequence": LEGACY_SEQUENCE,
            "future_snapshot_sequence": FUTURE_SEQUENCE,
        },
        "counts": {
            "events": len(events),
            "records_per_file": len(records),
            "series": len(SERIES),
            "records_by_series": {
                f"{series['service']}/{series['sli_type']}": 12 for series in SERIES
            },
        },
        "files": hashes,
        "expectations": expectations,
        "expected_query": query_expectations(records),
        "pinned_business_codes": {
            "legacy": "DEGRADE",
            "current": "ACCEPT",
            "future": "E_FORMAT_UNSUPPORTED",
            "truncated_binary": "E_RECORD_CORRUPT",
            "bad_crc_binary": "E_RECORD_CORRUPT",
            "truncated_jsonl": "E_RECORD_CORRUPT",
            "invalid_count": "E_COUNT_INVALID",
            "conflict_reject": "E_IMPORT_CONFLICT",
            "insufficient": "E_INSUFFICIENT_SAMPLES",
            "snapshot_regression": "E_SNAPSHOT_INVALID",
        },
        "notes": [
            "JSONL window_start_us contains sub-millisecond remainders; floor normalization must recover binary window_start_ms.",
            "Version 1 omits late_count and uses relative end offsets; late_count becomes 0.",
            "Version 3 declares a required extension and must be rejected before any record import.",
            "All expected numeric values are fixed-decimal strings using ROUND_HALF_UP.",
        ],
    }
    write_text(out_dir / "samples.json", dumps_json(summary, pretty=True) + "\n")

    sha_lines = [
        f"{details['sha256']}  {relative_path}"
        for relative_path, details in sorted(hashes.items())
    ]
    write_text(out_dir / "SHA256SUMS", "\n".join(sha_lines) + "\n")
    return summary


def write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_bytes(payload)
    tmp.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=str(ROOT / "samples"))
    parser.add_argument("--clean", action="store_true")
    args = parser.parse_args()
    out_dir = Path(args.out).resolve()
    if args.clean and out_dir.exists():
        shutil.rmtree(out_dir)
    summary = generate(out_dir)
    print(json.dumps({
        "ok": True,
        "out": str(out_dir),
        "events": summary["counts"]["events"],
        "records": summary["counts"]["records_per_file"],
        "files": len(summary["files"]),
    }, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
