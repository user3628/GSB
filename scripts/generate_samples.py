#!/usr/bin/env python3
"""Generate fixed offline samples, policy snapshots and naive population facts."""

from __future__ import annotations

import hashlib
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from obs_sampler.policy import policy_digest  # noqa: E402

SAMPLES = ROOT / "samples"
DEMO_BATCH_ID = "fixed-sample-batch-1"

SERVICES = {
    "checkout": ["INFO", "WARN", "ERROR"],
    "payments": ["INFO", "ERROR"],
    "search": ["INFO", "WARN", "ERROR"],
}

POLICY_V1 = {
    "schema_version": 1,
    "version": 1,
    "generated_at": "2026-11-20T00:00:00.000Z",
    "seed": 20261120,
    "dedup_window_ms": 500,
    "dedup_scope": "trace_per_kind",
    "min_samples": 12,
    "error_bound": 0.40,
    "bias_correction": True,
    "strata": {
        "checkout": {"INFO": 0.65, "WARN": 0.80, "ERROR": 1.0},
        "payments": {"INFO": 0.45, "ERROR": 1.0},
        "search": {"INFO": 0.55, "WARN": 0.75, "ERROR": 1.0},
    },
    "tail_sampling": {
        "latency_threshold_us": 250000,
        "base_rate": 0.35,
        "error_multiplier": 2.0,
    },
}

POLICY_V2 = {
    "schema_version": 1,
    "version": 2,
    "generated_at": "2026-11-20T01:00:00.000Z",
    "seed": 20261120,
    "dedup_window_ms": 350,
    "dedup_scope": "trace_per_kind",
    "min_samples": 12,
    "error_bound": 0.40,
    "bias_correction": True,
    "strata": {
        "checkout": {"INFO": 0.50, "WARN": 0.70, "ERROR": 0.90},
        "payments": {"INFO": 0.60, "ERROR": 1.0},
        "search": {"INFO": 0.80, "WARN": 0.60, "ERROR": 1.0},
    },
    "tail_sampling": {
        "latency_threshold_us": 300000,
        "base_rate": 0.45,
        "error_multiplier": 2.5,
    },
}


def add_digest(document: dict[str, Any]) -> dict[str, Any]:
    result = json.loads(json.dumps(document))
    result["digest"] = policy_digest(result)
    return result


def sample_weight(index: int) -> float:
    if index % 13 == 0:
        return 2.0
    if index % 17 == 0:
        return 1.5
    return 1.0


def span_shape(service: str, level: str, index: int) -> tuple[int, bool]:
    if level == "ERROR":
        return 120000 + (index % 8) * 50000, True
    if level == "WARN" and index % 5 == 0:
        return 350000, False
    if level == "INFO" and index % 7 == 0:
        return 275000, False
    if index % 29 == 0:
        return 450000, True
    return 30000 + (index % 13) * 7000, False


def generate_records() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    logs: list[dict[str, Any]] = []
    spans: list[dict[str, Any]] = []
    base_timestamp = 1735689600000
    service_offset = 0

    for service, levels in SERVICES.items():
        for level_index, level in enumerate(levels):
            stratum_offset = service_offset + level_index * 20_000_000
            for index in range(80):
                trace_id = f"trace-{service}-{level.lower()}-{index:04d}"
                root_span_id = f"span-root-{service}-{level.lower()}-{index:04d}"
                log_span_id = f"span-log-{service}-{level.lower()}-{index:04d}"
                span_span_id = f"span-child-{service}-{level.lower()}-{index:04d}"
                timestamp = base_timestamp + stratum_offset + index * 100

                logs.append(
                    {
                        "timestamp_ms": timestamp,
                        "service": service,
                        "level": level,
                        "message_fingerprint": f"fp-{service}-{level.lower()}-{index % 23:02d}",
                        "trace_id": trace_id,
                        "span_id": log_span_id,
                        "parent_span_id": root_span_id,
                        "sample_weight": 1.0,
                    }
                )

                latency_us, is_error = span_shape(service, level, index)
                spans.append(
                    {
                        "timestamp_ms": timestamp + 7,
                        "service": service,
                        "level": level,
                        "message_fingerprint": f"span-op-{index % 11:02d}",
                        "trace_id": trace_id,
                        "span_id": span_span_id,
                        "parent_span_id": root_span_id,
                        "latency_us": latency_us,
                        "error": is_error,
                        "sample_weight": sample_weight(index),
                    }
                )

                # A repeated signal occurrence inside the inclusive window. A new
                # span_id is used so batch identity validation still succeeds.
                if index > 0 and index % 9 == 0:
                    previous_trace = f"trace-{service}-{level.lower()}-{index - 1:04d}"
                    logs.append(
                        {
                            "timestamp_ms": timestamp + 30,
                            "service": service,
                            "level": level,
                            "message_fingerprint": f"fp-{service}-{level.lower()}-dup",
                            "trace_id": previous_trace,
                            "span_id": f"{log_span_id}-dup",
                            "parent_span_id": root_span_id,
                            "sample_weight": 1.0,
                        }
                    )
                    spans.append(
                        {
                            "timestamp_ms": timestamp + 37,
                            "service": service,
                            "level": level,
                            "message_fingerprint": f"span-op-dup-{index % 11:02d}",
                            "trace_id": previous_trace,
                            "span_id": f"{span_span_id}-dup",
                            "parent_span_id": root_span_id,
                            "latency_us": latency_us,
                            "error": is_error,
                            "sample_weight": sample_weight(index),
                        }
                    )

                # Same trace just outside the v1 window: it becomes a new leader
                # and exercises the inclusive/exclusive boundary separately.
                if index > 2 and index % 41 == 0:
                    old_trace = f"trace-{service}-{level.lower()}-{index - 2:04d}"
                    logs.append(
                        {
                            "timestamp_ms": timestamp + 650,
                            "service": service,
                            "level": level,
                            "message_fingerprint": f"fp-{service}-{level.lower()}-outside",
                            "trace_id": old_trace,
                            "span_id": f"{log_span_id}-outside",
                            "parent_span_id": root_span_id,
                            "sample_weight": 1.0,
                        }
                    )

            service_offset += 100_000_000

    logs.sort(key=lambda row: (row["timestamp_ms"], row["trace_id"], row["span_id"]))
    spans.sort(key=lambda row: (row["timestamp_ms"], row["trace_id"], row["span_id"]))
    return logs, spans


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
            handle.write("\n")


def write_json(path: Path, value: Any) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")


def expected_population(
    logs: list[dict[str, Any]], spans: list[dict[str, Any]], policy: dict[str, Any]
) -> dict[str, Any]:
    all_rows: list[tuple[int, int, str, int, dict[str, Any]]] = []
    for source_index, row in enumerate(logs):
        all_rows.append((row["timestamp_ms"], 1, "log", source_index, row))
    for source_index, row in enumerate(spans):
        all_rows.append((row["timestamp_ms"], 0, "span", source_index, row))
    all_rows.sort(key=lambda item: (item[0], item[1], item[3]))

    active: dict[tuple[str, str], tuple[int, str, int]] = {}
    strata: dict[tuple[str, str], dict[str, float | int]] = defaultdict(
        lambda: {"population_count": 0, "population_weight": 0.0}
    )
    head_strata: dict[tuple[str, str], dict[str, float | int]] = defaultdict(
        lambda: {"sampled_leader_count": 0, "sampled_upstream_weight": 0.0}
    )
    sampled_uids: list[str] = []
    leaders = 0
    duplicates = 0
    total_weight = 0.0
    for timestamp, _kind_order, kind, source_index, row in all_rows:
        key = (kind, row["trace_id"])
        current = active.get(key)
        if current is not None and 0 <= timestamp - current[0] <= policy["dedup_window_ms"]:
            duplicates += 1
            continue
        active[key] = (timestamp, row["span_id"], 0)
        leaders += 1
        stratum = strata[(row["service"], row["level"])]
        stratum["population_count"] += 1
        stratum["population_weight"] += row.get("sample_weight", 1.0)
        total_weight += row.get("sample_weight", 1.0)

        record_uid = f"{DEMO_BATCH_ID}:{kind}:{source_index:08d}"
        head_material = (
            f"head|{policy['version']}|{policy['seed']}|{DEMO_BATCH_ID}|{kind}|{source_index}"
        ).encode("utf-8")
        head_unit = int.from_bytes(
            hashlib.blake2b(head_material, digest_size=8).digest(), "big"
        ) / float(1 << 64)
        head_rate = policy["strata"].get(row["service"], {}).get(row["level"], 0.0)
        if head_unit < head_rate:
            sampled_uids.append(record_uid)
            head_stratum = head_strata[(row["service"], row["level"])]
            head_stratum["sampled_leader_count"] += 1
            head_stratum["sampled_upstream_weight"] += row.get("sample_weight", 1.0)

    head_digest = hashlib.sha256()
    for record_uid in sorted(sampled_uids):
        head_digest.update(record_uid.encode("utf-8"))
        head_digest.update(b"\n")

    return {
        "policy_version": policy["version"],
        "policy_digest": policy["digest"],
        "method": "exact local population after per-signal inclusive-window dedup",
        "record_count": len(logs) + len(spans),
        "leader_count": leaders,
        "duplicate_count": duplicates,
        "total_population_weight": total_weight,
        "strata": {
            f"{service}/{level}": {
                "service": service,
                "level": level,
                **values,
            }
            for (service, level), values in sorted(strata.items())
        },
        "head_only_sample": {
            "batch_id": DEMO_BATCH_ID,
            "method": "deterministic head selection only, after the same inclusive-window dedup",
            "sampled_leader_count": len(sampled_uids),
            "sampled_upstream_weight": sum(
                value["sampled_upstream_weight"] for value in head_strata.values()
            ),
            "sampled_uids_sha256": head_digest.hexdigest(),
            "strata": {
                f"{service}/{level}": {
                    "service": service,
                    "level": level,
                    **values,
                }
                for (service, level), values in sorted(head_strata.items())
            },
        },
    }


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    SAMPLES.mkdir(parents=True, exist_ok=True)
    logs, spans = generate_records()
    policy_v1 = add_digest(POLICY_V1)
    policy_v2 = add_digest(POLICY_V2)

    write_jsonl(SAMPLES / "logs.jsonl", logs)
    write_jsonl(SAMPLES / "spans.jsonl", spans)
    write_json(SAMPLES / "policy_v1.json", policy_v1)
    write_json(SAMPLES / "policy_v2.json", policy_v2)

    expected = {
        "description": (
            "Naive local enumeration before sampling. Demo compares Kahan-weighted "
            "Horvitz-Thompson estimates against these exact population facts."
        ),
        "versions": {
            "1": expected_population(logs, spans, policy_v1),
            "2": expected_population(logs, spans, policy_v2),
        },
    }
    write_json(SAMPLES / "naive_expected.json", expected)

    manifest = {
        "files": {
            name: file_sha256(SAMPLES / name)
            for name in (
                "logs.jsonl",
                "spans.jsonl",
                "policy_v1.json",
                "policy_v2.json",
                "naive_expected.json",
            )
        }
    }
    write_json(SAMPLES / "manifest.json", manifest)
    print(json.dumps({"logs": len(logs), "spans": len(spans), "manifest": manifest}, indent=2))


if __name__ == "__main__":
    main()
