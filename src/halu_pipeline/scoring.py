from __future__ import annotations

import hashlib
import math
import re
from typing import Any, Iterable

import numpy as np

from .canonical import fixed_fp, round_float
from .config import Config


_EPSILON = 1e-12
_NUMBER_RE = re.compile(r"\d+(?:[.,]\d+)?")
_EN_NEGATORS = {"not", "no", "never", "without", "none", "cannot", "neither", "nor"}
_ZH_NEGATORS = set("不没无未非禁否")


def _token_texts(tokens: Iterable[dict[str, Any]]) -> list[str]:
    return [token["text"] for token in tokens]


def _compact(text: str) -> str:
    return "".join(char for char in text if char.isalnum())


def _ngrams(text: str, size: int) -> set[str]:
    compact = _compact(text)
    if not compact:
        return set()
    if len(compact) <= size:
        return {compact}
    return {compact[index : index + size] for index in range(len(compact) - size + 1)}


def _coverage(output_items: set[str], evidence_items: set[str]) -> float:
    if not output_items:
        return 0.0
    return len(output_items & evidence_items) / len(output_items)


def _hashed_vector(features: Iterable[str], buckets: int) -> np.ndarray:
    vector = np.zeros(buckets, dtype=np.float64)
    for feature in features:
        digest = hashlib.sha256(feature.encode("utf-8")).digest()
        bucket = int.from_bytes(digest[:8], "big") % buckets
        vector[bucket] += 1.0
    return vector


def _hash_cosine(
    output_tokens: list[str],
    evidence_tokens: list[str],
    output_text: str,
    evidence_text: str,
    ngram_size: int,
    buckets: int,
) -> float:
    output_features = output_tokens + sorted(_ngrams(output_text, ngram_size))
    evidence_features = evidence_tokens + sorted(_ngrams(evidence_text, ngram_size))
    left = _hashed_vector(output_features, buckets)
    right = _hashed_vector(evidence_features, buckets)
    left_norm = float(np.linalg.norm(left))
    right_norm = float(np.linalg.norm(right))
    if left_norm == 0.0 or right_norm == 0.0:
        return 0.0
    return float(np.dot(left, right) / (left_norm * right_norm))


def _normative_rule_factor(output_tokens: list[str], evidence_tokens: list[str]) -> tuple[float, list[str]]:
    output_numbers = {match for token in output_tokens for match in _NUMBER_RE.findall(token)}
    evidence_numbers = {
        match for token in evidence_tokens for match in _NUMBER_RE.findall(token)
    }
    factors: list[float] = []
    rules: list[str] = []
    if output_numbers and not output_numbers.issubset(evidence_numbers):
        factors.append(0.50)
        rules.append("NUMERIC_MISMATCH")

    output_negative = bool(set(output_tokens) & _EN_NEGATORS) or bool(
        set("".join(output_tokens)) & _ZH_NEGATORS
    )
    evidence_negative = bool(set(evidence_tokens) & _EN_NEGATORS) or bool(
        set("".join(evidence_tokens)) & _ZH_NEGATORS
    )
    if output_negative != evidence_negative:
        factors.append(0.55)
        rules.append("NEGATION_POLARITY_MISMATCH")

    factor = 1.0
    for item in factors:
        factor *= item
    return factor, rules


def score_pair(
    output_sentence: dict[str, Any],
    evidence_sentence: dict[str, Any],
    source_prior: float,
    config: Config,
) -> dict[str, Any]:
    output_tokens = _token_texts(output_sentence["tokens"])
    evidence_tokens = _token_texts(evidence_sentence["tokens"])
    term_coverage = _coverage(set(output_tokens), set(evidence_tokens))
    ngram_coverage = _coverage(
        _ngrams(output_sentence["text"], int(config.values["ngram_size"])),
        _ngrams(evidence_sentence["text"], int(config.values["ngram_size"])),
    )
    hash_cosine = _hash_cosine(
        output_tokens,
        evidence_tokens,
        output_sentence["text"],
        evidence_sentence["text"],
        int(config.values["ngram_size"]),
        int(config.values["hash_buckets"]),
    )
    rule_factor, rule_mismatches = _normative_rule_factor(output_tokens, evidence_tokens)

    weights = {key: float(value) for key, value in config.values["scoring_weights"].items()}
    logs = {
        "term": math.log(term_coverage + _EPSILON),
        "ngram": math.log(ngram_coverage + _EPSILON),
        "hash": math.log(hash_cosine + _EPSILON),
        "prior": math.log(max(0.0, source_prior) + _EPSILON),
    }
    weight_sum = sum(weights.values())
    log_consistency = sum(weights[key] * logs[key] for key in logs) / weight_sum
    base_consistency = math.exp(log_consistency)
    consistency = max(0.0, min(1.0, base_consistency * rule_factor))

    return {
        "term_coverage": round_float(term_coverage),
        "ngram_coverage": round_float(ngram_coverage),
        "hash_cosine": round_float(hash_cosine),
        "source_prior": round_float(max(0.0, min(1.0, source_prior))),
        "log_consistency": round_float(log_consistency),
        "rule_factor": round_float(rule_factor),
        "rule_mismatches": rule_mismatches,
        "consistency": round_float(consistency),
    }


def _flatten_evidence_sentences(
    stage1_record: dict[str, Any],
) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    for evidence_index, sentences in enumerate(
        stage1_record["sentences"]["retrieved_evidence"]
    ):
        for sentence in sentences:
            item = dict(sentence)
            item["evidence_index"] = evidence_index
            flattened.append(item)
    return flattened


def _empty_error_result(stage1_record: dict[str, Any], config: Config) -> dict[str, Any]:
    return {
        "sample_id": stage1_record["sample_id"],
        "source": stage1_record["source"],
        "lang": stage1_record["lang"],
        "content_fingerprint": stage1_record["content_fingerprint"],
        "status": "error",
        "errors": stage1_record["errors"],
        "annotation": stage1_record["annotation"],
        "hallucination": None,
        "needs_review": True,
        "review_reasons": ["E_EMPTY_FIELD"],
        "consistency_score": 0.0,
        "sentence_support": [],
        "output_sentence_count": 0,
        "supported_sentence_count": 0,
        "unsupported_sentence_count": 0,
        "unsupported_ratio": 0.0,
        "thresholds": {
            "sentence_support": config.sentence_support_threshold,
            "hallucination_ratio": config.hallucination_ratio_threshold,
        },
    }


def judge_record(stage1_record: dict[str, Any], config: Config) -> dict[str, Any]:
    if stage1_record["status"] != "ok":
        return _empty_error_result(stage1_record, config)

    weights = config.evidence_weights
    source_prior = weights.get(stage1_record["source"], weights.get("__default__", 1.0))
    evidence_sentences = _flatten_evidence_sentences(stage1_record)
    output_sentences = stage1_record["sentences"]["model_output"]
    sentence_support: list[dict[str, Any]] = []

    for output_sentence in output_sentences:
        best: dict[str, Any] | None = None
        best_rank: tuple[int, int, int] | None = None
        for evidence_sentence in evidence_sentences:
            pair = score_pair(output_sentence, evidence_sentence, source_prior, config)
            rank = (
                fixed_fp(pair["consistency"], 1_000_000_000_000),
                -int(evidence_sentence["evidence_index"]),
                -int(evidence_sentence["sentence_index"]),
            )
            if best_rank is None or rank > best_rank:
                best_rank = rank
                best = {
                    "sentence_index": output_sentence["sentence_index"],
                    "text": output_sentence["text"],
                    "start": output_sentence["start"],
                    "end": output_sentence["end"],
                    "supported": False,
                    "score": pair["consistency"],
                    "best_evidence": {
                        "evidence_index": evidence_sentence["evidence_index"],
                        "sentence_index": evidence_sentence["sentence_index"],
                        "text": evidence_sentence["text"],
                        "start": evidence_sentence["start"],
                        "end": evidence_sentence["end"],
                    },
                    "features": pair,
                }
        if best is None:
            best = {
                "sentence_index": output_sentence["sentence_index"],
                "text": output_sentence["text"],
                "start": output_sentence["start"],
                "end": output_sentence["end"],
                "supported": False,
                "score": 0.0,
                "best_evidence": None,
                "features": {
                    "term_coverage": 0.0,
                    "ngram_coverage": 0.0,
                    "hash_cosine": 0.0,
                    "source_prior": round_float(source_prior),
                    "log_consistency": 0.0,
                    "rule_factor": 0.0,
                    "rule_mismatches": [],
                    "consistency": 0.0,
                },
            }
        best["supported"] = best["score"] >= config.sentence_support_threshold
        sentence_support.append(best)

    sentence_count = len(sentence_support)
    supported_count = sum(1 for item in sentence_support if item["supported"])
    unsupported_count = sentence_count - supported_count
    unsupported_ratio = unsupported_count / sentence_count if sentence_count else 1.0
    hallucination = unsupported_ratio > config.hallucination_ratio_threshold

    if sentence_count:
        log_mean = math.fsum(
            math.log(max(item["score"], _EPSILON)) for item in sentence_support
        ) / sentence_count
        consistency_score = math.exp(log_mean)
    else:
        consistency_score = 0.0

    annotation = stage1_record["annotation"]
    review_reasons: list[str] = []
    if not annotation["present"] or annotation["is_hallucination"] is None:
        review_reasons.append("MISSING_ANNOTATION")
    if annotation["needs_review"] is True:
        review_reasons.append("MANUAL_REVIEW_REQUESTED")
    if sentence_count == 0:
        review_reasons.append("NO_OUTPUT_CLAIMS")

    return {
        "sample_id": stage1_record["sample_id"],
        "source": stage1_record["source"],
        "lang": stage1_record["lang"],
        "content_fingerprint": stage1_record["content_fingerprint"],
        "status": "ok",
        "errors": [],
        "annotation": annotation,
        "hallucination": hallucination,
        "needs_review": bool(review_reasons),
        "review_reasons": sorted(review_reasons),
        "consistency_score": round_float(consistency_score),
        "sentence_support": sentence_support,
        "output_sentence_count": sentence_count,
        "supported_sentence_count": supported_count,
        "unsupported_sentence_count": unsupported_count,
        "unsupported_ratio": round_float(unsupported_ratio),
        "thresholds": {
            "sentence_support": config.sentence_support_threshold,
            "hallucination_ratio": config.hallucination_ratio_threshold,
        },
    }


def judge_records(stage1_payload: dict[str, Any], config: Config) -> dict[str, Any]:
    records = [
        judge_record(record, config)
        for record in sorted(stage1_payload["records"], key=lambda item: item["sample_id"])
    ]
    return {
        "stage": "judge",
        "scoring_version": config.values["scoring_version"],
        "scoring_config": config.stage_subset(2),
        "tie_break_rule": "score_micro_desc,evidence_index_asc,evidence_sentence_index_asc",
        "records": records,
    }
