"""Result validation and query tool.

Rebuilds global statistics exclusively from the persisted shard records and
cross-checks every record against the manifest fingerprints, so stale results
from an older sample/config version can never be silently presented as current.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .checkpoint import CheckpointStore
from .errors import E_VERSION_MISMATCH, PipeError
from .stats import reduce_global
from .versioning import canonical_bytes, short_fp


def load_run(workspace: str | Path) -> tuple[dict[str, Any], list[dict[str, Any]], CheckpointStore]:
    store = CheckpointStore(workspace)
    manifest = store.read_manifest()
    records = store.read_all_records()
    return manifest, records, store


def validate_run(workspace: str | Path) -> dict[str, Any]:
    manifest, records, store = load_run(workspace)
    expected_total = len(manifest["shards"])
    invalid: list[dict[str, Any]] = []
    unprocessed: list[dict[str, Any]] = []
    valid_records: list[dict[str, Any]] = []
    chain_ok = True

    indices = sorted(record["shard_index"] for record in records)
    missing = [i for i in range(expected_total) if i not in indices]
    if indices != list(range(len(indices))):
        chain_ok = False

    by_index = {record["shard_index"]: record for record in records}
    for index in range(expected_total):
        record = by_index.get(index)
        if record is None:
            continue
        try:
            store.validate_record_versions(record, manifest)
        except PipeError as exc:
            chain_ok = False
            invalid.append(
                {
                    "shard_index": index,
                    "code": exc.code,
                    "field": exc.field,
                    "message": exc.message,
                    "details": exc.details,
                }
            )
            continue
        if record["status"] == "unprocessed":
            unprocessed.append(
                {
                    "shard_index": index,
                    "code": (record.get("reason") or {}).get("code"),
                    "reason": (record.get("reason") or {}).get("reason"),
                    "detail": (record.get("reason") or {}).get("detail"),
                }
            )
        else:
            valid_records.append(record)

    image_summaries: list[dict[str, Any]] = []
    for record in valid_records:
        image_summaries.extend(record["image_summaries"])

    global_stats = reduce_global(image_summaries)
    evaluated = sum(
        record["candidate_count"] for record in valid_records
    )
    global_stats["totals"]["candidates_evaluated_in_processed_shards"] = evaluated
    global_stats["totals"]["unprocessed_shards"] = len(unprocessed)
    global_stats["totals"]["invalid_shards"] = len(invalid)
    global_stats["totals"]["missing_shards"] = len(missing)

    return {
        "ok": chain_ok and not invalid and not missing,
        "workspace": str(workspace),
        "manifest_fingerprint": short_fp(manifest),
        "threshold_version": short_fp(manifest["threshold_version"]),
        "budget_version": short_fp(manifest["budget_version"]),
        "dataset_fingerprint": short_fp(manifest["dataset_fingerprint"]),
        "shards_total": expected_total,
        "shards_present": len(records),
        "chain_contiguous": indices == list(range(len(indices))),
        "missing_shard_indices": missing,
        "invalid_shards": invalid,
        "unprocessed_shards": unprocessed,
        "config": manifest["config"],
        "global_statistics": global_stats,
    }


def report_bytes(report: dict[str, Any]) -> bytes:
    """Canonical byte representation used for byte-level equality checks."""
    return canonical_bytes(report["global_statistics"])


def write_report(report: dict[str, Any], path: str | Path) -> None:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(report, fh, sort_keys=True, indent=2, ensure_ascii=False)
        fh.write("\n")


def image_summaries(workspace: str | Path) -> list[dict[str, Any]]:
    _, records, store = load_run(workspace)
    manifest = store.read_manifest()
    summaries: list[dict[str, Any]] = []
    for record in sorted(records, key=lambda item: item["shard_index"]):
        store.validate_record_versions(record, manifest)
        if record["status"] == "processed":
            summaries.extend(record["image_summaries"])
    return sorted(summaries, key=lambda item: item["image_id"])


def assert_no_stale_results(workspace: str | Path) -> None:
    report = validate_run(workspace)
    if report["invalid_shards"]:
        raise PipeError(
            E_VERSION_MISMATCH,
            "stale shard records detected for the current manifest",
            details={"invalid_shards": report["invalid_shards"]},
        )
