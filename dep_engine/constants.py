"""Stable schema and stage identifiers for the dependency engine."""

from __future__ import annotations

SCHEMA_VERSION = 1

FEATUREIZATION = "featureization"
DECODING = "decoding"
VALIDATION = "validation"
AGGREGATION = "aggregation"

STAGES: tuple[str, ...] = (
    FEATUREIZATION,
    DECODING,
    VALIDATION,
    AGGREGATION,
)
STAGE_INDEX: dict[str, int] = {name: index for index, name in enumerate(STAGES)}

FEATUREIZATION_SCHEMA_VERSION = "featureization-v1"
DECODER_SCHEMA_VERSION = "transition-decoder-v1"
VALIDATION_SCHEMA_VERSION = "tree-validator-v1"
AGGREGATION_SCHEMA_VERSION = "shard-aggregate-v1"
TOKENIZER_VERSION = "zh-mixed-unicode-v1"
PARSER_VERSION = "arc-standard-greedy-v1"

STAGE_SCHEMA_VERSIONS: dict[str, str] = {
    FEATUREIZATION: FEATUREIZATION_SCHEMA_VERSION,
    DECODING: DECODER_SCHEMA_VERSION,
    VALIDATION: VALIDATION_SCHEMA_VERSION,
    AGGREGATION: AGGREGATION_SCHEMA_VERSION,
}

# Configuration fields that participate in each stage's version fingerprint.
# Fields deliberately appear in only the earliest stage they can affect.
STAGE_CONFIG_FIELDS: dict[str, tuple[str, ...]] = {
    FEATUREIZATION: ("tokenizer_version",),
    DECODING: ("score_weights", "smoothing_strength", "confidence_threshold"),
    VALIDATION: ("max_dependency_distance", "unique_root"),
    AGGREGATION: (),
}

ACTION_ORDER: tuple[str, ...] = ("SHIFT", "LEFT_REDUCE", "RIGHT_REDUCE")
ACTION_LABEL: dict[str, str] = {
    "SHIFT": "SHIFT",
    "LEFT_REDUCE": "LEFT_DEP",
    "RIGHT_REDUCE": "RIGHT_DEP",
}

E_EMPTY_SENTENCE = "E_EMPTY_SENTENCE"
E_TREE_INVALID = "E_TREE_INVALID"
E_NUMERIC = "E_NUMERIC"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_INCREMENTAL_MISMATCH = "E_INCREMENTAL_MISMATCH"
