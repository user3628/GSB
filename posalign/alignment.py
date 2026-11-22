"""Deterministic token LCS alignment and tag-pair extraction."""

from __future__ import annotations

from typing import Any

import numpy as np

from .config import GovernanceConfig
from .errors import BusinessError, E_ALIGNMENT_FAILED
from .parse import StopGate
from .textnorm import normalize_token


def _lcs_operations(
    old_tokens: list[str],
    new_tokens: list[str],
    comparison: str,
) -> tuple[list[dict[str, Any]], int]:
    old_norm = [normalize_token(token, comparison) for token in old_tokens]
    new_norm = [normalize_token(token, comparison) for token in new_tokens]
    n, m = len(old_norm), len(new_norm)
    dp = np.zeros((n + 1, m + 1), dtype=np.int32)
    for i in range(1, n + 1):
        for j in range(1, m + 1):
            if old_norm[i - 1] == new_norm[j - 1]:
                dp[i, j] = dp[i - 1, j - 1] + 1
            else:
                dp[i, j] = max(int(dp[i - 1, j]), int(dp[i, j - 1]))
    operations: list[dict[str, Any]] = []
    i, j = n, m
    while i > 0 or j > 0:
        if i > 0 and j > 0 and old_norm[i - 1] == new_norm[j - 1]:
            operations.append(
                {
                    "op": "match",
                    "old_index": i - 1,
                    "new_index": j - 1,
                    "old_token": old_tokens[i - 1],
                    "new_token": new_tokens[j - 1],
                }
            )
            i -= 1
            j -= 1
        elif j > 0 and (i == 0 or int(dp[i, j - 1]) >= int(dp[i - 1, j])):
            operations.append(
                {
                    "op": "insert",
                    "new_index": j - 1,
                    "new_token": new_tokens[j - 1],
                }
            )
            j -= 1
        else:
            operations.append(
                {
                    "op": "delete",
                    "old_index": i - 1,
                    "old_token": old_tokens[i - 1],
                }
            )
            i -= 1
    operations.reverse()
    return operations, int(dp[n, m])


def _gap_spans(operations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    gaps: list[dict[str, Any]] = []
    pending: list[dict[str, Any]] = []

    def flush() -> None:
        if not pending:
            return
        deleted = [op for op in pending if op["op"] == "delete"]
        inserted = [op for op in pending if op["op"] == "insert"]
        old_indices = [op["old_index"] for op in deleted]
        new_indices = [op["new_index"] for op in inserted]
        gaps.append(
            {
                "old_start": min(old_indices) if old_indices else None,
                "old_end_exclusive": max(old_indices) + 1 if old_indices else None,
                "new_start": min(new_indices) if new_indices else None,
                "new_end_exclusive": max(new_indices) + 1 if new_indices else None,
                "deletions": [
                    {"index": op["old_index"], "token": op["old_token"]}
                    for op in deleted
                ],
                "insertions": [
                    {"index": op["new_index"], "token": op["new_token"]}
                    for op in inserted
                ],
                "misaligned": bool(deleted and inserted),
            }
        )
        pending.clear()

    for op in operations:
        if op["op"] == "match":
            flush()
        else:
            pending.append(op)
    flush()
    return gaps


def align_shard(
    parse_payload: dict[str, Any],
    *,
    config: GovernanceConfig,
    stop_gate: StopGate | None = None,
) -> dict[str, Any]:
    shard_id = parse_payload["shard_id"]
    thresholds = config.alignment
    sentences: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    total_insertions = 0
    total_deletions = 0
    total_pairs = 0
    rate_sum = 0.0
    rate_count = 0

    for record in parse_payload["records"]:
        base = {
            "sent_id": record["sent_id"],
            "annotator": record["annotator"],
            "source_version": record["source_version"],
            "tokens": record["tokens"],
        }
        if record["unsplit"]:
            sentences.append(
                {
                    **base,
                    "status": "unsegmented",
                    "alignable": False,
                    "alignment_rate": None,
                    "reason": "structure_version_1_zero_tokens",
                    "pairs": [],
                    "edit_operations": [],
                    "insertions": [],
                    "deletions": [],
                    "misaligned_spans": [],
                }
            )
            if stop_gate is not None:
                stop_gate.after_sentence()
            continue
        if not record["tokens"]:
            sentences.append(
                {
                    **base,
                    "status": "empty",
                    "alignable": False,
                    "alignment_rate": None,
                    "reason": "zero_tokens",
                    "pairs": [],
                    "edit_operations": [],
                    "insertions": [],
                    "deletions": [],
                    "misaligned_spans": [],
                }
            )
            if stop_gate is not None:
                stop_gate.after_sentence()
            continue
        if record["new_tags"] is None:
            if not thresholds.allow_unpaired:
                failures.append(
                    {
                        "shard_id": shard_id,
                        "sent_id": record["sent_id"],
                        "alignment_rate": 0.0,
                        "min_rate": thresholds.min_rate,
                        "reason": "missing_new_tags",
                    }
                )
            sentences.append(
                {
                    **base,
                    "status": "unpaired",
                    "alignable": False,
                    "alignment_rate": None,
                    "reason": "missing_new_tags",
                    "pairs": [],
                    "edit_operations": [],
                    "insertions": [],
                    "deletions": [],
                    "misaligned_spans": [],
                }
            )
            if stop_gate is not None:
                stop_gate.after_sentence()
            continue

        new_tokens = (
            record["new_tokens"]
            if record["new_tokens"] is not None
            else list(record["tokens"])
        )
        operations, matches = _lcs_operations(
            record["tokens"], new_tokens, thresholds.token_comparison
        )
        denominator = len(record["tokens"]) + len(new_tokens)
        rate = 1.0 if denominator == 0 else (2.0 * matches / denominator)
        rate = round(rate, 6)
        if rate < thresholds.min_rate:
            failures.append(
                {
                    "shard_id": shard_id,
                    "sent_id": record["sent_id"],
                    "alignment_rate": rate,
                    "min_rate": thresholds.min_rate,
                    "reason": "alignment_rate_below_tolerance",
                    "old_tokens": record["tokens"],
                    "new_tokens": new_tokens,
                }
            )
            if stop_gate is not None:
                stop_gate.after_sentence()
            continue

        pairs: list[dict[str, Any]] = []
        for op in operations:
            if op["op"] == "match":
                old_index = op["old_index"]
                new_index = op["new_index"]
                pairs.append(
                    {
                        "old_index": old_index,
                        "new_index": new_index,
                        "old_token": op["old_token"],
                        "new_token": op["new_token"],
                        "token_match": "normalized_equal",
                        "old_tag_raw": record["old_tags"][old_index],
                        "old_tag": record["old_candidates"][old_index]["normalized_old_tag"],
                        "old_candidates": record["old_candidates"][old_index]["candidates"],
                        "new_tag_raw": record["new_tags"][new_index],
                        "new_tag": record["new_tag_normalization"][new_index]["tag"],
                        "new_tag_diagnostic": record["new_tag_normalization"][new_index].get(
                            "diagnostic"
                        ),
                        "annotator": record["annotator"],
                        "confidence": record["confidence"],
                    }
                )
        insertions = [op for op in operations if op["op"] == "insert"]
        deletions = [op for op in operations if op["op"] == "delete"]
        gaps = _gap_spans(operations)
        misaligned_spans = [gap for gap in gaps if gap["misaligned"]]
        total_pairs += len(pairs)
        total_insertions += len(insertions)
        total_deletions += len(deletions)
        rate_sum += rate
        rate_count += 1
        sentences.append(
            {
                **base,
                "status": "aligned",
                "alignable": True,
                "alignment_rate": rate,
                "matched_tokens": matches,
                "old_token_count": len(record["tokens"]),
                "new_token_count": len(new_tokens),
                "pairs": pairs,
                "edit_operations": operations,
                "insertions": insertions,
                "deletions": deletions,
                "misaligned_spans": misaligned_spans,
            }
        )
        if stop_gate is not None:
            stop_gate.after_sentence()

    if failures:
        raise BusinessError(
            E_ALIGNMENT_FAILED,
            "one or more annotation pairs failed alignment",
            {
                "shard_id": shard_id,
                "failures": failures,
                "min_rate": thresholds.min_rate,
                "wrote_alignment_stage": False,
            },
        )

    stats = {
        "sentences": len(sentences),
        "aligned": sum(1 for item in sentences if item["status"] == "aligned"),
        "unpaired": sum(1 for item in sentences if item["status"] == "unpaired"),
        "unsegmented": sum(1 for item in sentences if item["status"] == "unsegmented"),
        "empty": sum(1 for item in sentences if item["status"] == "empty"),
        "matched_pairs": total_pairs,
        "insertions": total_insertions,
        "deletions": total_deletions,
        "average_alignment_rate": round(rate_sum / rate_count, 6) if rate_count else None,
    }
    return {
        "schema": "posalign.align.v1",
        "shard_id": shard_id,
        "governance": {
            "alignment_fingerprint": config.alignment_fingerprint,
            "token_comparison": thresholds.token_comparison,
            "min_rate": thresholds.min_rate,
            "allow_unpaired": thresholds.allow_unpaired,
        },
        "sentences": sentences,
        "stats": stats,
        "failures": [],
    }

