"""Aggregation of mapping-candidate generation and v1 compatibility facts."""

from __future__ import annotations

from typing import Any


def _merge_candidates(target: dict[str, dict[str, Any]], items: list[dict[str, Any]]) -> None:
    for item in items:
        existing = target.setdefault(
            item["tag"],
            {"tag": item["tag"], "source": item["source"], "sources": set(), "rules": set()},
        )
        existing["sources"].update(item["sources"])
        existing["rules"].update(item["rules"])
        if "alias_table" in existing["sources"]:
            existing["source"] = "alias_table"


def build_migration_report(
    parse_payloads: list[dict[str, Any]],
    *,
    config_components: dict[str, str],
) -> dict[str, Any]:
    labels: dict[str, dict[str, Any]] = {}
    unsplit: list[dict[str, Any]] = []
    v1_records: list[dict[str, Any]] = []
    for payload in sorted(parse_payloads, key=lambda item: item["shard_id"]):
        for catalog in payload["label_catalog"]:
            entry = labels.setdefault(
                catalog["normalized_old_tag"],
                {
                    "normalized_old_tag": catalog["normalized_old_tag"],
                    "original_variants": set(),
                    "occurrences": 0,
                    "sent_ids": set(),
                    "candidates": {},
                    "diagnostics": {},
                },
            )
            entry["original_variants"].update(catalog["original_variants"])
            entry["occurrences"] += catalog["occurrences"]
            entry["sent_ids"].update(catalog["sent_ids"])
            _merge_candidates(entry["candidates"], catalog["candidates"])
            for diag in catalog["diagnostics"]:
                key = (diag["code"], tuple(diag["candidates"]))
                merged = entry["diagnostics"].setdefault(
                    key,
                    {
                        "code": diag["code"],
                        "message": diag["message"],
                        "candidates": diag["candidates"],
                        "occurrences": 0,
                        "sent_ids": set(),
                    },
                )
                merged["occurrences"] += diag["occurrences"]
                merged["sent_ids"].update(diag["sent_ids"])
        for record in payload["records"]:
            if record["unsplit"]:
                unsplit.append(
                    {
                        "shard_id": payload["shard_id"],
                        "sent_id": record["sent_id"],
                        "annotator": record["annotator"],
                        "old_tags": record["old_tags"],
                        "reason": "structure_version_1_zero_tokens",
                        "alignable": False,
                    }
                )
            if record["source_version"] == 1:
                v1_records.append(
                    {
                        "shard_id": payload["shard_id"],
                        "sent_id": record["sent_id"],
                        "compatibility": record["compatibility"],
                    }
                )

    label_rows: list[dict[str, Any]] = []
    for key in sorted(labels):
        item = labels[key]
        label_rows.append(
            {
                "normalized_old_tag": key,
                "original_variants": sorted(item["original_variants"]),
                "occurrences": item["occurrences"],
                "sent_ids": sorted(item["sent_ids"]),
                "candidates": [
                    {
                        "tag": value["tag"],
                        "source": value["source"],
                        "sources": sorted(value["sources"]),
                        "rules": sorted(value["rules"]),
                    }
                    for _, value in sorted(item["candidates"].items())
                ],
                "diagnostics": [
                    {
                        "code": value["code"],
                        "message": value["message"],
                        "candidates": value["candidates"],
                        "occurrences": value["occurrences"],
                        "sent_ids": sorted(value["sent_ids"]),
                    }
                    for _, value in sorted(item["diagnostics"].items())
                ],
            }
        )
    return {
        "schema": "posalign.migration.v1",
        "governance": config_components,
        "labels": label_rows,
        "v1_records": sorted(v1_records, key=lambda item: (item["shard_id"], item["sent_id"])),
        "unsegmented_v1": sorted(
            unsplit, key=lambda item: (item["shard_id"], item["sent_id"])
        ),
        "stats": {
            "labels": len(label_rows),
            "labels_with_multiple_candidates": sum(
                1 for item in label_rows if len(item["candidates"]) > 1
            ),
            "labels_with_missing_alias": sum(
                1
                for item in label_rows
                if any(diag["code"] in {"alias_missing", "alias_missing_rule_fallback"} for diag in item["diagnostics"])
            ),
            "labels_without_candidates": sum(1 for item in label_rows if not item["candidates"]),
            "v1_records": len(v1_records),
            "v1_unsegmented": len(unsplit),
        },
    }

