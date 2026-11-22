"""Configuration schema, normalization, and stage fingerprint components."""

from __future__ import annotations

import copy
import math
from fractions import Fraction
from typing import Any, Iterable

from .errors import E_INVALID_STAGE, PipelineError, invalid_config
from .util import deep_merge, sha256_obj

STAGES: tuple[str, ...] = ("normalize", "allocate", "assemble", "validate")
STAGE_ALIASES = {
    "dedup": "normalize",
    "normalize_dedup": "normalize",
    "normalization": "normalize",
    "allocation": "allocate",
    "mix": "assemble",
    "assembly": "assemble",
    "check": "validate",
    "validation": "validate",
}

SCHEMA_VERSION = 1
NORMALIZE_ALGORITHM = "nfkc_ws_lower_v1"
SIMHASH_ALGORITHM = "char_unigram_blake2b64_v1"
ALLOCATE_ALGORITHM = "largest_remainder_v1"
ASSEMBLE_ALGORITHM = "sha256_rank_v1"
VALIDATE_ALGORITHM = "digest_report_v1"
BANDS = 4
BAND_BITS = 16

DEFAULT_CONFIG: dict[str, Any] = {
    "schema_version": SCHEMA_VERSION,
    "revision": 0,
    "dedup": {
        "normalization": NORMALIZE_ALGORITHM,
        "simhash": SIMHASH_ALGORITHM,
        "bands": BANDS,
        "hamming_threshold": 3,
    },
    "allocation": {
        "target_samples": 36,
        "weights": {
            "web::zh": 7,
            "web::en": 5,
            "news::zh": 4,
            "news::en": 3,
            "forum::mixed": 3,
            "forum::en": 2,
        },
        "token_estimator": "whitespace",
    },
    "assembly": {
        "algorithm": ASSEMBLE_ALGORITHM,
        "seed": "llm-mix-v1",
    },
    "validation": {
        "algorithm": VALIDATE_ALGORITHM,
    },
}


def canonical_stage(stage: str) -> str:
    normalized = stage.strip().lower().replace("-", "_")
    normalized = STAGE_ALIASES.get(normalized, normalized)
    if normalized not in STAGES:
        raise PipelineError(
            E_INVALID_STAGE,
            f"unknown stage: {stage}",
            {"stage": stage, "allowed_stages": list(STAGES)},
            http_status=409,
        )
    return normalized


def stage_index(stage: str) -> int:
    return STAGES.index(canonical_stage(stage))


def default_config() -> dict[str, Any]:
    return copy.deepcopy(DEFAULT_CONFIG)


def _as_int(value: Any, field: str, minimum: int, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise invalid_config(f"{field} must be an integer", {"field": field})
    if value < minimum or (maximum is not None and value > maximum):
        raise invalid_config(
            f"{field} is outside the allowed range",
            {"field": field, "value": value, "minimum": minimum, "maximum": maximum},
        )
    return value


def _as_number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise invalid_config(f"{field} must be a number", {"field": field})
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise invalid_config(
            f"{field} must be a finite non-negative number",
            {"field": field, "value": value},
        )
    return number


def parse_layer_id(layer_id: str) -> tuple[str, str]:
    if not isinstance(layer_id, str):
        raise invalid_config("layer id must be a string", {"layer_id": layer_id})
    if "::" in layer_id:
        source, lang = layer_id.split("::", 1)
    elif "|" in layer_id:
        source, lang = layer_id.split("|", 1)
    else:
        raise invalid_config(
            "layer id must use 'source::lang' (or legacy 'source|lang')",
            {"layer_id": layer_id},
        )
    if not source or not lang:
        raise invalid_config("layer id has an empty source or lang", {"layer_id": layer_id})
    return source, lang


def make_layer_id(source: str, lang: str) -> str:
    return f"{source}::{lang}"


def normalize_weights(raw_weights: Any) -> list[dict[str, Any]]:
    """Return a canonical, source/lang-sorted weight list."""
    items: list[tuple[str, str, float]] = []
    if isinstance(raw_weights, dict):
        for layer_id, weight in raw_weights.items():
            source, lang = parse_layer_id(layer_id)
            items.append((source, lang, _as_number(weight, f"allocation.weights[{layer_id}]")))
    elif isinstance(raw_weights, list):
        for index, entry in enumerate(raw_weights):
            if not isinstance(entry, dict):
                raise invalid_config(
                    "allocation.weights entries must be objects",
                    {"index": index},
                )
            try:
                source = entry["source"]
                lang = entry["lang"]
                weight = entry["weight"]
            except KeyError as exc:
                raise invalid_config(
                    "allocation.weights entry is missing a field",
                    {"index": index, "missing": str(exc)},
                ) from exc
            if not isinstance(source, str) or not source:
                raise invalid_config("weight source must be a non-empty string", {"index": index})
            if not isinstance(lang, str) or not lang:
                raise invalid_config("weight lang must be a non-empty string", {"index": index})
            items.append((source, lang, _as_number(weight, f"allocation.weights[{index}].weight")))
    else:
        raise invalid_config("allocation.weights must be an object or an array")

    seen: set[tuple[str, str]] = set()
    canonical: list[dict[str, Any]] = []
    for source, lang, weight in sorted(items, key=lambda item: (item[0], item[1])):
        key = (source, lang)
        if key in seen:
            raise invalid_config(
                "duplicate allocation weight layer",
                {"source": source, "lang": lang},
            )
        seen.add(key)
        canonical.append({"source": source, "lang": lang, "weight": weight})
    return canonical


def weights_as_dict(weights: Iterable[dict[str, Any]]) -> dict[str, float]:
    return {
        make_layer_id(item["source"], item["lang"]): item["weight"]
        for item in weights
    }


def weights_as_fractions(weights: Iterable[dict[str, Any]]) -> dict[tuple[str, str], Fraction]:
    result: dict[tuple[str, str], Fraction] = {}
    for item in weights:
        result[(item["source"], item["lang"])] = Fraction(str(item["weight"]))
    return result


def validate_config(config: Any) -> dict[str, Any]:
    if not isinstance(config, dict):
        raise invalid_config("config must be a JSON object")
    merged = deep_merge(DEFAULT_CONFIG, config)
    if merged.get("schema_version") != SCHEMA_VERSION:
        raise invalid_config(
            "unsupported config schema_version",
            {"expected": SCHEMA_VERSION, "actual": merged.get("schema_version")},
        )
    merged["revision"] = _as_int(merged.get("revision", 0), "revision", 0)

    dedup = merged.get("dedup")
    if not isinstance(dedup, dict):
        raise invalid_config("dedup must be an object")
    if dedup.get("normalization") != NORMALIZE_ALGORITHM:
        raise invalid_config(
            "unsupported dedup.normalization",
            {"allowed": [NORMALIZE_ALGORITHM], "actual": dedup.get("normalization")},
        )
    if dedup.get("simhash") != SIMHASH_ALGORITHM:
        raise invalid_config(
            "unsupported dedup.simhash",
            {"allowed": [SIMHASH_ALGORITHM], "actual": dedup.get("simhash")},
        )
    if dedup.get("bands") != BANDS:
        raise invalid_config(
            "dedup.bands must be 4 (four 16-bit buckets)",
            {"expected": BANDS, "actual": dedup.get("bands")},
        )
    dedup["hamming_threshold"] = _as_int(
        dedup.get("hamming_threshold"), "dedup.hamming_threshold", 0, 64
    )

    allocation = merged.get("allocation")
    if not isinstance(allocation, dict):
        raise invalid_config("allocation must be an object")
    allocation["target_samples"] = _as_int(
        allocation.get("target_samples"), "allocation.target_samples", 0
    )
    if allocation.get("token_estimator") not in {"whitespace", "whitespace_plus_cjk"}:
        raise invalid_config(
            "unsupported allocation.token_estimator",
            {
                "allowed": ["whitespace", "whitespace_plus_cjk"],
                "actual": allocation.get("token_estimator"),
            },
        )
    allocation["weights"] = weights_as_dict(normalize_weights(allocation.get("weights")))

    assembly = merged.get("assembly")
    if not isinstance(assembly, dict):
        raise invalid_config("assembly must be an object")
    if assembly.get("algorithm") != ASSEMBLE_ALGORITHM:
        raise invalid_config(
            "unsupported assembly.algorithm",
            {"allowed": [ASSEMBLE_ALGORITHM], "actual": assembly.get("algorithm")},
        )
    if not isinstance(assembly.get("seed"), str) or not assembly["seed"]:
        raise invalid_config("assembly.seed must be a non-empty string")

    validation = merged.get("validation")
    if not isinstance(validation, dict):
        raise invalid_config("validation must be an object")
    if validation.get("algorithm") != VALIDATE_ALGORITHM:
        raise invalid_config(
            "unsupported validation.algorithm",
            {"allowed": [VALIDATE_ALGORITHM], "actual": validation.get("algorithm")},
        )
    return merged


def config_digest(config: dict[str, Any]) -> str:
    return sha256_obj(config)


def dedup_components(config: dict[str, Any], shards: list[dict[str, str]]) -> dict[str, Any]:
    dedup = config["dedup"]
    return {
        "algorithm": NORMALIZE_ALGORITHM,
        "simhash": SIMHASH_ALGORITHM,
        "bands": BANDS,
        "hamming_threshold": dedup["hamming_threshold"],
        "shards": [
            {"name": item["name"], "digest": item["digest"]}
            for item in sorted(shards, key=lambda item: item["name"])
        ],
    }


def allocate_components(
    config: dict[str, Any],
    normalized_artifact_digest: str,
    normalized_stage_fingerprint: str | None = None,
) -> dict[str, Any]:
    allocation = config["allocation"]
    return {
        "algorithm": ALLOCATE_ALGORITHM,
        "normalized_artifact_digest": normalized_artifact_digest,
        "normalized_stage_fingerprint": normalized_stage_fingerprint,
        "target_samples": allocation["target_samples"],
        "token_estimator": allocation["token_estimator"],
        "weights": normalize_weights(allocation["weights"]),
    }


def assemble_components(
    config: dict[str, Any],
    normalized_artifact_digest: str,
    allocation_artifact_digest: str,
    normalized_stage_fingerprint: str | None = None,
    allocation_stage_fingerprint: str | None = None,
) -> dict[str, Any]:
    return {
        "algorithm": ASSEMBLE_ALGORITHM,
        "normalized_artifact_digest": normalized_artifact_digest,
        "allocation_artifact_digest": allocation_artifact_digest,
        "normalized_stage_fingerprint": normalized_stage_fingerprint,
        "allocation_stage_fingerprint": allocation_stage_fingerprint,
        "seed": config["assembly"]["seed"],
    }


def validate_components(
    config: dict[str, Any],
    assembly_artifact_digest: str,
    assembly_stage_fingerprint: str | None = None,
) -> dict[str, Any]:
    return {
        "algorithm": VALIDATE_ALGORITHM,
        "assembly_artifact_digest": assembly_artifact_digest,
        "assembly_stage_fingerprint": assembly_stage_fingerprint,
    }
