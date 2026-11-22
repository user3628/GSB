"""The four deterministic per-shard pipeline stages."""

from __future__ import annotations

from collections import Counter
from typing import Any

from .canonical import sha256_obj
from .config import AnalysisConfig
from .constants import (
    AGGREGATION_SCHEMA_VERSION,
    DECODER_SCHEMA_VERSION,
    FEATUREIZATION_SCHEMA_VERSION,
)
from .manifest import ShardEntry
from .parser import parse_sentence
from .tokenize import sentence_id, split_sentences, tokenize
from .validation import validate_artifact


def featureize(
    *,
    entry: ShardEntry,
    text: str,
    content_digest: str,
    config: AnalysisConfig,
) -> dict[str, Any]:
    lines = split_sentences(text)
    sentences = []
    token_count = 0
    for line_index, line in enumerate(lines, start=1):
        tokens = tokenize(line)
        token_count += len(tokens)
        sentences.append(
            {
                "sentence_id": sentence_id(entry.index, line_index),
                "sentence_index": line_index,
                "text": line,
                "tokens": tokens,
            }
        )
    return {
        "schema_version": FEATUREIZATION_SCHEMA_VERSION,
        "shard_index": entry.index,
        "shard_name": entry.file,
        "content_sha256": content_digest,
        "tokenizer_version": config.tokenizer_version,
        "manifest_sentence_count": entry.sentence_count,
        "manifest_token_count": entry.token_count,
        "measured_sentence_count": len(lines),
        "measured_token_count": token_count,
        "sentences": sentences,
    }


def decode_artifact(
    *,
    feature_artifact: dict[str, Any],
    config: AnalysisConfig,
) -> dict[str, Any]:
    shard_index = int(feature_artifact["shard_index"])
    shard_name = str(feature_artifact["shard_name"])
    decoded = []
    for sentence in feature_artifact.get("sentences", []):
        decoded.append(
            parse_sentence(
                text=sentence["text"],
                sentence_id=sentence["sentence_id"],
                shard_index=shard_index,
                sentence_index=int(sentence["sentence_index"]),
                config=config,
            )
        )
    return {
        "schema_version": DECODER_SCHEMA_VERSION,
        "shard_index": shard_index,
        "shard_name": shard_name,
        "score_weights": {
            "shift": config.score_weights["shift"],
            "left_reduce": config.score_weights["left_reduce"],
            "right_reduce": config.score_weights["right_reduce"],
        },
        "smoothing_strength": config.smoothing_strength,
        "confidence_threshold": config.confidence_threshold,
        "sentences": decoded,
    }


def validate_stage(
    *,
    decoded_artifact: dict[str, Any],
    config: AnalysisConfig,
) -> dict[str, Any]:
    return validate_artifact(
        shard_index=int(decoded_artifact["shard_index"]),
        shard_name=str(decoded_artifact["shard_name"]),
        decoded_artifact=decoded_artifact,
        config=config,
    )


def aggregate_stage(validation_artifact: dict[str, Any]) -> dict[str, Any]:
    sentences = validation_artifact.get("sentences", [])
    diagnostics = validation_artifact.get("diagnostics", [])
    error_counts: Counter[str] = Counter()
    violation_counts: Counter[str] = Counter()
    for diagnostic in diagnostics:
        if diagnostic.get("status") != "ok" and diagnostic.get("code"):
            error_counts[str(diagnostic["code"])] += 1
        for violation in diagnostic.get("violations", []):
            violation_counts[str(violation["type"])] += 1
    token_count = sum(len(sentence.get("tokens", [])) for sentence in sentences)
    tree_count = sum(1 for sentence in sentences if sentence.get("status") == "ok")
    low_confidence_steps = sum(
        len(sentence.get("low_confidence_steps", [])) for sentence in sentences
    )
    tree_digest = sha256_obj(sentences)
    diagnostic_digest = sha256_obj(diagnostics)
    aggregate = {
        "schema_version": AGGREGATION_SCHEMA_VERSION,
        "shard_index": int(validation_artifact["shard_index"]),
        "shard_name": str(validation_artifact["shard_name"]),
        "sentence_count": len(sentences),
        "token_count": token_count,
        "tree_count": tree_count,
        "error_count": sum(error_counts.values()),
        "error_counts": dict(sorted(error_counts.items())),
        "constraint_violation_count": sum(violation_counts.values()),
        "constraint_violation_counts": dict(sorted(violation_counts.items())),
        "low_confidence_step_count": low_confidence_steps,
        "tree_digest": tree_digest,
        "diagnostic_digest": diagnostic_digest,
    }
    return aggregate
