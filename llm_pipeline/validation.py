"""Final consistency checks and deterministic validation report."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable

from .config import VALIDATE_ALGORITHM
from .errors import E_VALIDATION_FAILED, PipelineError
from .util import sha256_bytes
from .workspace import (
    allocation_product_bytes,
    mixed_product_bytes,
    record_keep_product_bytes,
)


def _fail(message: str, details: dict[str, Any]) -> PipelineError:
    return PipelineError(E_VALIDATION_FAILED, message, details, http_status=409)


def validate(
    normalized_payload: dict[str, Any],
    allocation_payload: dict[str, Any],
    assembly_payload: dict[str, Any],
    config: dict[str, Any],
    *,
    normalized_artifact_digest: str,
    allocation_artifact_digest: str,
    assembly_artifact_digest: str,
    check_stop: Callable[[], None] | None = None,
) -> dict[str, Any]:
    keep_bytes = record_keep_product_bytes(normalized_payload)
    allocation_bytes = allocation_product_bytes(allocation_payload)
    mixed_bytes = mixed_product_bytes(assembly_payload)
    digests = {
        "keep_list": sha256_bytes(keep_bytes),
        "allocation_table": sha256_bytes(allocation_bytes),
        "mixed_set": sha256_bytes(mixed_bytes),
        "normalized_artifact": normalized_artifact_digest,
        "allocation_artifact": allocation_artifact_digest,
        "assembly_artifact": assembly_artifact_digest,
    }

    kept = {record["id"]: record for record in normalized_payload["kept"]}
    selected = assembly_payload["rows"]
    checks: list[dict[str, Any]] = []

    ok = normalized_payload.get("keep_list_digest") == digests["keep_list"]
    checks.append(
        {
            "name": "normalize_keep_list_digest",
            "ok": ok,
            "expected": normalized_payload.get("keep_list_digest"),
            "actual": digests["keep_list"],
        }
    )
    if not ok:
        raise _fail("normalized keep-list digest is inconsistent", checks[-1])

    ok = allocation_payload.get("allocation_digest") == digests["allocation_table"]
    checks.append(
        {
            "name": "allocation_table_digest",
            "ok": ok,
            "expected": allocation_payload.get("allocation_digest"),
            "actual": digests["allocation_table"],
        }
    )
    if not ok:
        raise _fail("allocation table digest is inconsistent", checks[-1])

    ok = assembly_payload.get("mixed_digest") == digests["mixed_set"]
    checks.append(
        {
            "name": "mixed_set_digest",
            "ok": ok,
            "expected": assembly_payload.get("mixed_digest"),
            "actual": digests["mixed_set"],
        }
    )
    if not ok:
        raise _fail("mixed-set digest is inconsistent", checks[-1])

    allocation_ids = [row["id"] for row in selected]
    duplicate_ids = sorted(
        sample_id
        for sample_id, count in _counts(allocation_ids).items()
        if count != 1
    )
    checks.append(
        {
            "name": "unique_mixed_sample_ids",
            "ok": not duplicate_ids,
            "duplicate_ids": duplicate_ids,
        }
    )
    if duplicate_ids:
        raise _fail("mixed set contains duplicate sample ids", checks[-1])

    missing = sorted(sample_id for sample_id in allocation_ids if sample_id not in kept)
    checks.append(
        {
            "name": "mixed_ids_come_from_kept",
            "ok": not missing,
            "missing_ids": missing,
        }
    )
    if missing:
        raise _fail("mixed set contains samples absent from the keep list", checks[-1])

    mismatched: list[dict[str, Any]] = []
    for row in selected:
        source = kept[row["id"]]
        if (
            row["source"] != source["source"]
            or row["lang"] != source["lang"]
            or row["text"] != source["text"]
        ):
            mismatched.append({"id": row["id"]})
    checks.append(
        {
            "name": "mixed_rows_match_kept_records",
            "ok": not mismatched,
            "mismatches": mismatched[:10],
        }
    )
    if mismatched:
        raise _fail("mixed rows do not match their kept records", checks[-1])

    expected_counts = {
        row["layer_id"]: int(row["allocated"])
        for row in allocation_payload["layers"]
    }
    actual_counts = _counts(
        f"{row['source']}::{row['lang']}" for row in selected
    )
    count_diffs = []
    for layer_id in sorted(set(expected_counts) | set(actual_counts)):
        if expected_counts.get(layer_id, 0) != actual_counts.get(layer_id, 0):
            count_diffs.append(
                {
                    "layer_id": layer_id,
                    "allocated": expected_counts.get(layer_id, 0),
                    "assembled": actual_counts.get(layer_id, 0),
                }
            )
    checks.append(
        {
            "name": "layer_allocation_matches_assembly",
            "ok": not count_diffs,
            "differences": count_diffs,
        }
    )
    if count_diffs:
        raise _fail("assembled layer counts differ from allocation", checks[-1])

    recorded_counts = {
        str(layer_id): int(count)
        for layer_id, count in assembly_payload.get("counts_by_layer", {}).items()
    }
    counts_by_layer_ok = recorded_counts == expected_counts
    checks.append(
        {
            "name": "assembly_counts_by_layer",
            "ok": counts_by_layer_ok,
            "expected": expected_counts,
            "actual": recorded_counts,
        }
    )
    if not counts_by_layer_ok:
        raise _fail("assembly counts_by_layer is inconsistent", checks[-1])

    totals_ok = (
        allocation_payload["total_allocated"]
        == assembly_payload["total_selected"]
        == len(selected)
    )
    checks.append(
        {
            "name": "totals_match",
            "ok": totals_ok,
            "allocated": allocation_payload["total_allocated"],
            "assembled": assembly_payload["total_selected"],
        }
    )
    if not totals_ok:
        raise _fail("allocated and assembled totals differ", checks[-1])

    empty_layers = [
        row["layer_id"] for row in allocation_payload["layers"] if row["status"].startswith("empty")
    ]

    layer_stats: list[dict[str, Any]] = []
    allocation_by_layer = {
        row["layer_id"]: row for row in allocation_payload["layers"]
    }
    for layer_id in sorted(allocation_by_layer):
        row = allocation_by_layer[layer_id]
        layer_stats.append(
            {
                "layer_id": layer_id,
                "source": row["source"],
                "lang": row["lang"],
                "weight": row["weight"],
                "input_count": row["input_count"],
                "available": row["available"],
                "duplicate_count": row["duplicate_count"],
                "empty_error_count": row["empty_error_count"],
                "token_count": row["token_count"],
                "allocated": row["allocated"],
                "assembled": actual_counts.get(layer_id, 0),
                "status": row["status"],
            }
        )
        if check_stop is not None:
            check_stop()

    report: dict[str, Any] = {
        "algorithm": VALIDATE_ALGORITHM,
        "status": "ok",
        "checks": checks,
        "digests": digests,
        "layer_stats": layer_stats,
        "empty_layers": empty_layers,
        "counts": {
            "input": normalized_payload["stats"]["input_count"],
            "kept": normalized_payload["stats"]["kept_count"],
            "duplicates": normalized_payload["stats"]["duplicate_count"],
            "empty_errors": normalized_payload["stats"]["empty_error_count"],
            "allocated": allocation_payload["total_allocated"],
            "assembled": assembly_payload["total_selected"],
        },
        "empty_errors": normalized_payload["empty_errors"],
    }
    return report


def _counts(values: Any) -> dict[str, int]:
    result: dict[str, int] = defaultdict(int)
    for value in values:
        result[value] += 1
    return dict(result)
