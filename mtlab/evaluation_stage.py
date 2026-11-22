from __future__ import annotations

import math
import statistics
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Counter as CounterType

from .config import ExperimentConfig
from .errors import MtlError
from .tokenizer import base_token_texts, normalize_text
from .util import iter_jsonl, round_float, sha256_json, write_json_atomic, write_jsonl_atomic

EVALUATION_SCHEMA = "sentence-bleu-add1-word-precision-v1"


def evaluation_input_descriptor(
    tier: str,
    config: ExperimentConfig,
    *,
    decode_digest: str,
    reference_sha256: str,
) -> dict[str, Any]:
    return {
        "engine_version": config.engine_version,
        "stage": "evaluate",
        "evaluation_schema": EVALUATION_SCHEMA,
        "tier": tier,
        "decode_digest": decode_digest,
        "reference_sha256": reference_sha256,
        "metric_max_order": int(config.metric_max_order),
        "confidence_level": float(config.confidence_level),
        "primary_reference": "local_reference_file_only",
    }


def _ngram_counts(tokens: list[str], order: int) -> CounterType[tuple[str, ...]]:
    return Counter(tuple(tokens[index : index + order]) for index in range(len(tokens) - order + 1))


def sentence_bleu_approx(
    candidate: list[str], reference: list[str], *, max_order: int
) -> float:
    if not candidate or not reference:
        return 0.0
    effective_order = min(max_order, max(1, len(reference)))
    precisions: list[float] = []
    for order in range(1, effective_order + 1):
        candidate_ngrams = _ngram_counts(candidate, order)
        reference_ngrams = _ngram_counts(reference, order)
        matches = sum(
            min(count, reference_ngrams[ngram])
            for ngram, count in candidate_ngrams.items()
        )
        total = max(0, len(candidate) - order + 1)
        # Add-one smoothing makes per-sentence scores well-defined for short
        # sentences while preserving the usual brevity penalty.
        precisions.append((matches + 1.0) / (total + 1.0))
    log_mean = sum(math.log(value) for value in precisions) / len(precisions)
    brevity = (
        math.exp(1.0 - len(reference) / len(candidate))
        if len(candidate) < len(reference)
        else 1.0
    )
    return min(1.0, max(0.0, brevity * math.exp(log_mean)))


def word_precision(candidate: list[str], reference: list[str]) -> float:
    if not candidate:
        return 0.0
    candidate_counts = Counter(candidate)
    reference_counts = Counter(reference)
    matches = sum(
        min(count, reference_counts[token]) for token, count in candidate_counts.items()
    )
    return min(1.0, max(0.0, matches / len(candidate)))


def _ci95(values: list[float], confidence_level: float) -> dict[str, float | int]:
    count = len(values)
    if count == 0:
        return {
            "mean": 0.0,
            "standard_deviation": 0.0,
            "standard_error": 0.0,
            "lower": 0.0,
            "upper": 0.0,
            "effective_pair_count": 0,
        }
    mean = statistics.fmean(values)
    deviation = statistics.stdev(values) if count > 1 else 0.0
    standard_error = deviation / math.sqrt(count)
    if confidence_level > 0.0 and confidence_level < 1.0:
        z_value = statistics.NormalDist().inv_cdf(0.5 + confidence_level / 2.0)
    else:
        z_value = 0.0
    lower = mean - z_value * standard_error
    upper = mean + z_value * standard_error
    return {
        "mean": round_float(mean),
        "standard_deviation": round_float(deviation),
        "standard_error": round_float(standard_error),
        "lower": round_float(min(1.0, max(0.0, lower))),
        "upper": round_float(min(1.0, max(0.0, upper))),
        "effective_pair_count": count,
    }


def run_evaluation(
    split_artifact: Path,
    decode_artifact: Path,
    reference_path: Path,
    tier: str,
    config: ExperimentConfig,
    manifest: dict[str, Any],
    out_dir: Path,
    *,
    decode_artifact_digest: str,
    reference_sha256: str,
    stop_check: Callable[[], None] | None = None,
) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    diagnostics = list(iter_jsonl(split_artifact / "diagnostics.jsonl"))
    decoded = list(iter_jsonl(decode_artifact / "decoded.jsonl"))
    references = list(iter_jsonl(reference_path))
    diagnostic_by_id = {row["pair_id"]: row for row in diagnostics}
    decoded_by_id = {row["pair_id"]: row for row in decoded}
    reference_by_id = {row["pair_id"]: row for row in references}

    rows: list[dict[str, Any]] = []
    bleu_values: list[float] = []
    precision_values: list[float] = []
    skipped_counts: Counter[str] = Counter()
    out_of_range_clamps = 0
    for reference_row in references:
        if stop_check:
            stop_check()
        pair_id = reference_row["pair_id"]
        if pair_id in diagnostic_by_id:
            diagnostic = diagnostic_by_id[pair_id]
            skipped_counts[diagnostic["code"]] += 1
            rows.append(
                {
                    "pair_id": pair_id,
                    "status": "skipped",
                    "reason": diagnostic["code"],
                    "detail": diagnostic.get("reason"),
                }
            )
            continue
        decoded_row = decoded_by_id.get(pair_id)
        if decoded_row is None:
            skipped_counts["missing_decoding"] += 1
            rows.append(
                {
                    "pair_id": pair_id,
                    "status": "skipped",
                    "reason": "missing_decoding",
                }
            )
            continue
        if pair_id not in reference_by_id:
            skipped_counts["missing_reference"] += 1
            rows.append(
                {
                    "pair_id": pair_id,
                    "status": "skipped",
                    "reason": "missing_reference",
                }
            )
            continue
        reference_text = reference_row["reference"]
        normalized_reference = normalize_text(reference_text)
        reference_tokens = base_token_texts(reference_text)
        candidate_tokens = base_token_texts(decoded_row["candidate_text"])
        if not reference_tokens:
            skipped_counts["empty_reference_tokens"] += 1
            rows.append(
                {
                    "pair_id": pair_id,
                    "status": "skipped",
                    "reason": "empty_reference_tokens",
                    "reference": reference_text,
                }
            )
            continue
        if not candidate_tokens:
            skipped_counts["empty_candidate_tokens"] += 1
            rows.append(
                {
                    "pair_id": pair_id,
                    "status": "skipped",
                    "reason": "empty_candidate_tokens",
                    "candidate": decoded_row["candidate_text"],
                }
            )
            continue
        bleu = sentence_bleu_approx(
            candidate_tokens,
            reference_tokens,
            max_order=config.metric_max_order,
        )
        precision = word_precision(candidate_tokens, reference_tokens)
        if bleu < 0.0 or bleu > 1.0 or precision < 0.0 or precision > 1.0:
            out_of_range_clamps += 1
        bleu = min(1.0, max(0.0, bleu))
        precision = min(1.0, max(0.0, precision))
        bleu_values.append(bleu)
        precision_values.append(precision)
        rows.append(
            {
                "pair_id": pair_id,
                "status": "ok",
                "lang_pair": decoded_row["lang_pair"],
                "domain": decoded_row["domain"],
                "candidate": decoded_row["candidate_text"],
                "reference": normalized_reference,
                "candidate_token_count": len(candidate_tokens),
                "reference_token_count": len(reference_tokens),
                "bleu_approx": round_float(bleu),
                "word_precision": round_float(precision),
            }
        )

    if len(rows) != int(manifest["pair_count"]):
        raise MtlError(
            code="E_INVALID_STAGE",
            message="evaluation did not account for every manifest pair",
            details={
                "manifest_pair_count": int(manifest["pair_count"]),
                "accounted_pair_count": len(rows),
                "tier": tier,
            },
        )
    bleu_ci = _ci95(bleu_values, config.confidence_level)
    precision_ci = _ci95(precision_values, config.confidence_level)
    by_language_pair: dict[str, dict[str, Any]] = {}
    for lang_pair in sorted({row["lang_pair"] for row in rows if row["status"] == "ok"}):
        subset = [
            row
            for row in rows
            if row["status"] == "ok" and row.get("lang_pair") == lang_pair
        ]
        by_language_pair[lang_pair] = {
            "effective_pair_count": len(subset),
            "mean_bleu_approx": round_float(
                statistics.fmean(float(row["bleu_approx"]) for row in subset)
            ),
            "mean_word_precision": round_float(
                statistics.fmean(float(row["word_precision"]) for row in subset)
            ),
        }
    metrics = {
        "schema_version": 1,
        "tier": tier,
        "evaluation_schema": EVALUATION_SCHEMA,
        "reference_source": "local_reference_file_only",
        "input_pair_count": int(manifest["pair_count"]),
        "accounted_pair_count": len(rows),
        "effective_pair_count": len(bleu_values),
        "skipped_pair_count": sum(skipped_counts.values()),
        "skipped_by_reason": dict(sorted(skipped_counts.items())),
        "out_of_range_clamped_count": out_of_range_clamps,
        "sentence_bleu": bleu_ci,
        "word_precision": precision_ci,
        "confidence_level": config.confidence_level,
        "confidence_interval_method": "normal approximation: mean +/- z*sample_stdev/sqrt(n), clipped to [0,1]",
        "bleu_definition": "add-one-smoothed sentence n-gram BLEU approximation with brevity penalty",
        "word_precision_definition": "multiset token intersection divided by candidate token count",
        "by_language_pair": by_language_pair,
    }
    write_json_atomic(
        out_dir / "_meta.json",
        {
            "schema_version": 1,
            "tier": tier,
            "stage": "evaluate",
            "stage_index": 5,
            "output_schema": EVALUATION_SCHEMA,
            "input_fingerprint": sha256_json(
                evaluation_input_descriptor(
                    tier,
                    config,
                    decode_digest=decode_artifact_digest,
                    reference_sha256=reference_sha256,
                )
            ),
        },
    )
    write_jsonl_atomic(out_dir / "pair_metrics.jsonl", rows)
    write_json_atomic(out_dir / "metrics.json", metrics)
    return metrics

