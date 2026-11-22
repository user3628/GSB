"""Local deterministic dependency parsing and incremental reconstruction engine."""

from __future__ import annotations

from .config import AnalysisConfig
from .constants import (
    AGGREGATION,
    DECODING,
    E_EMPTY_SENTENCE,
    E_INCREMENTAL_MISMATCH,
    E_INVALID_STAGE,
    E_NUMERIC,
    E_STALE_CHECKPOINT,
    E_TREE_INVALID,
    FEATUREIZATION,
    SCHEMA_VERSION,
    STAGES,
    VALIDATION,
)
from .engine import Engine
from .errors import EngineError
from .generator import DEMO_CONFIG, DEMO_SEED, generate_demo_data
from .manifest import Manifest, ShardEntry
from .parser import parse_sentence
from .store import CheckpointStore, RecoveredState
from .tokenize import split_sentences, tokenize
from .validation import validate_artifact, validate_sentence

__all__ = [
    "AGGREGATION",
    "AnalysisConfig",
    "CheckpointStore",
    "DECODING",
    "DEMO_CONFIG",
    "DEMO_SEED",
    "E_EMPTY_SENTENCE",
    "E_INCREMENTAL_MISMATCH",
    "E_INVALID_STAGE",
    "E_NUMERIC",
    "E_STALE_CHECKPOINT",
    "E_TREE_INVALID",
    "Engine",
    "EngineError",
    "FEATUREIZATION",
    "Manifest",
    "RecoveredState",
    "SCHEMA_VERSION",
    "STAGES",
    "ShardEntry",
    "VALIDATION",
    "generate_demo_data",
    "parse_sentence",
    "split_sentences",
    "tokenize",
    "validate_artifact",
    "validate_sentence",
]
