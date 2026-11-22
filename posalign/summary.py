"""Deterministic per-shard and global business summaries."""

from __future__ import annotations

from typing import Any

from .config import GovernanceConfig


def _resolved_index(mapping_core: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        item["old_tag"]: item
        for item in mapping_core.get("resolved", [])
        if item.get("new_tag") is not None
    }


def build_shard_summary(
    *,
    parse_payload: dict[str, Any],
    align_payload: dict[str, Any],
    mapping_core: dict[str, Any],
    mapping_version: int,
    mapping_digest: str,
    config: GovernanceConfig,
) -> dict[str, Any]:
    resolved = _resolved_index(mapping_core)
    mapped_pairs = 0
    unmapped_pairs = 0
    low_confidence_pairs = 0
    mapping_usage: dict[str, dict[str, Any]] = {}
    unmapped_labels: set[str] = set()
    for sentence in align_payload["sentences"]:
        for pair in sentence["pairs"]:
            old_tag = pair["old_tag"]
            mapping = resolved.get(old_tag)
            if mapping is None:
                unmapped_pairs += 1
                unmapped_labels.add(old_tag)
                continue
            mapped_pairs += 1
            if not mapping.get("accepted", False):
                low_confidence_pairs += 1
            usage = mapping_usage.setdefault(
                old_tag,
                {
                    "old_tag": old_tag,
                    "new_tag": mapping["new_tag"],
                    "pairs": 0,
                    "accepted": mapping.get("accepted", False),
                    "confidence": mapping.get("confidence"),
                },
            )
            usage["pairs"] += 1
    unsegmented = [
        {
            "sent_id": record["sent_id"],
            "annotator": record["annotator"],
            "old_tags": record["old_tags"],
            "reason": "structure_version_1_zero_tokens",
        }
        for record in parse_payload["records"]
        if record["unsplit"]
    ]
    v1_records = [
        {
            "sent_id": record["sent_id"],
            "confidence": record["confidence"],
            "confidence_conversion": record["confidence_conversion"],
            "degraded": record["compatibility"]["degraded"] if record["compatibility"] else False,
        }
        for record in parse_payload["records"]
        if record["source_version"] == 1
    ]
    return {
        "schema": "posalign.summary.shard.v1",
        "shard_id": parse_payload["shard_id"],
        "mapping": {
            "version": mapping_version,
            "digest": mapping_digest,
        },
        "parse": parse_payload["stats"],
        "alignment": align_payload["stats"],
        "v1_compatibility": {
            "records": len(v1_records),
            "unsegmented_count": len(unsegmented),
            "unsegmented_sentences": unsegmented,
            "converted_records": v1_records,
        },
        "mapping_usage": {
            "mapped_pairs": mapped_pairs,
            "unmapped_pairs": unmapped_pairs,
            "low_confidence_pairs": low_confidence_pairs,
            "unmapped_old_tags": sorted(unmapped_labels),
            "labels": [
                value for _, value in sorted(mapping_usage.items())
            ],
            "confidence_threshold": config.adjudication.confidence_threshold,
        },
    }


def build_global_summary(
    *,
    shard_summaries: list[dict[str, Any]],
    parse_payloads: list[dict[str, Any]],
    align_payloads: list[dict[str, Any]],
    mapping_core: dict[str, Any],
    mapping_version: int,
    config: GovernanceConfig,
) -> tuple[dict[str, Any], dict[str, Any]]:
    ordered = sorted(shard_summaries, key=lambda item: item["shard_id"])
    parse_stats = {
        key: sum(payload["stats"].get(key, 0) for payload in parse_payloads)
        for key in (
            "records",
            "v1_records",
            "v2_records",
            "v1_unsegmented",
            "alignable_records",
            "unpaired_records",
            "migrated_token_tags",
            "unknown_new_tags",
        )
    }
    align_stats = {
        "sentences": sum(payload["stats"]["sentences"] for payload in align_payloads),
        "aligned": sum(payload["stats"]["aligned"] for payload in align_payloads),
        "unpaired": sum(payload["stats"]["unpaired"] for payload in align_payloads),
        "unsegmented": sum(payload["stats"]["unsegmented"] for payload in align_payloads),
        "empty": sum(payload["stats"]["empty"] for payload in align_payloads),
        "matched_pairs": sum(payload["stats"]["matched_pairs"] for payload in align_payloads),
        "insertions": sum(payload["stats"]["insertions"] for payload in align_payloads),
        "deletions": sum(payload["stats"]["deletions"] for payload in align_payloads),
    }
    rates = [
        payload["stats"]["average_alignment_rate"]
        for payload in align_payloads
        if payload["stats"]["average_alignment_rate"] is not None
    ]
    align_stats["average_alignment_rate"] = (
        round(sum(rates) / len(rates), 6) if rates else None
    )
    unsegmented = [
        {
            "shard_id": summary["shard_id"],
            **sentence,
        }
        for summary in ordered
        for sentence in summary["v1_compatibility"]["unsegmented_sentences"]
    ]
    v1_confidences = [
        record
        for summary in ordered
        for record in summary["v1_compatibility"]["converted_records"]
        if record["confidence_conversion"] is not None
    ]
    mapping_usage = {
        "mapped_pairs": sum(summary["mapping_usage"]["mapped_pairs"] for summary in ordered),
        "unmapped_pairs": sum(summary["mapping_usage"]["unmapped_pairs"] for summary in ordered),
        "low_confidence_pairs": sum(
            summary["mapping_usage"]["low_confidence_pairs"] for summary in ordered
        ),
        "unmapped_old_tags": sorted(
            {
                tag
                for summary in ordered
                for tag in summary["mapping_usage"]["unmapped_old_tags"]
            }
        ),
    }
    mapping_stats = mapping_core.get("stats", {})
    summary = {
        "schema": "posalign.summary.v1",
        "mapping_version": mapping_version,
        "shards": [summary["shard_id"] for summary in ordered],
        "alignment": align_stats,
        "migration": parse_stats,
        "mapping": {
            **mapping_stats,
            "usage": mapping_usage,
            "confidence_threshold": config.adjudication.confidence_threshold,
        },
        "v1": {
            "records": parse_stats["v1_records"],
            "unsegmented_count": len(unsegmented),
            "unsegmented_sentences": unsegmented,
            "confidence_conversions": v1_confidences,
        },
    }
    stats = {
        "schema": "posalign.stats.v1",
        "mapping_version": mapping_version,
        "shard_count": len(ordered),
        "sentences": align_stats["sentences"],
        "aligned_sentences": align_stats["aligned"],
        "unpaired_sentences": align_stats["unpaired"],
        "v1_unsegmented_sentences": align_stats["unsegmented"],
        "matched_tag_pairs": mapping_usage["mapped_pairs"],
        "unmapped_tag_pairs": mapping_usage["unmapped_pairs"],
        "low_confidence_tag_pairs": mapping_usage["low_confidence_pairs"],
        "insertions": align_stats["insertions"],
        "deletions": align_stats["deletions"],
        "average_alignment_rate": align_stats["average_alignment_rate"],
        "mapping_labels": mapping_stats.get("labels", 0),
        "resolved_mappings": mapping_stats.get("resolved", 0),
        "mapping_conflicts": mapping_stats.get("conflicts", 0),
        "rejected_candidates": mapping_stats.get("rejected_candidates", 0),
    }
    return summary, stats

