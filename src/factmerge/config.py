"""Configuration loading, stage component digests and version fingerprints."""

from __future__ import annotations

import copy
import json
from importlib.resources import files
from pathlib import Path
from typing import Any, Iterable

from .errors import FactMergeError
from .util import canonical_json, digest_obj


STAGE_NAMES = {
    1: "parse",
    2: "normalize",
    3: "resolve",
    4: "evidence",
}

STAGE_COMPONENTS: dict[int, tuple[str, ...]] = {
    1: ("parser_version", "normalization", "parse", "category_hints", "numeric_scales"),
    2: ("unit_dictionary", "quantization", "calendar", "fallback"),
    3: (
        "language_weights",
        "language_priority",
        "source_priors",
        "source_priority",
        "freshness",
        "comparison",
        "arbitration",
    ),
    4: ("evidence",),
}


def _deep_merge(base: Any, override: Any) -> Any:
    if isinstance(base, dict) and isinstance(override, dict):
        result = copy.deepcopy(base)
        for key, value in override.items():
            if key in result:
                result[key] = _deep_merge(result[key], value)
            else:
                result[key] = copy.deepcopy(value)
        return result
    return copy.deepcopy(override)


def default_config() -> dict[str, Any]:
    resource = files("factmerge").joinpath("default_config.json")
    with resource.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def load_config(path: str | Path | None = None) -> dict[str, Any]:
    base = default_config()
    if path is None:
        return base
    config_path = Path(path)
    if not config_path.exists():
        raise FactMergeError("E_INVALID_FORMAT", f"configuration file not found: {config_path}")
    try:
        with config_path.open("r", encoding="utf-8") as fh:
            override = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        raise FactMergeError("E_INVALID_FORMAT", f"invalid configuration: {exc}") from exc
    if not isinstance(override, dict):
        raise FactMergeError("E_INVALID_FORMAT", "configuration root must be a JSON object")
    merged = _deep_merge(base, override)
    validate_config(merged)
    return merged


def validate_config(config: dict[str, Any]) -> None:
    required = {
        "parser_version",
        "normalization",
        "parse",
        "category_hints",
        "numeric_scales",
        "unit_dictionary",
        "quantization",
        "calendar",
        "fallback",
        "language_weights",
        "source_priors",
        "freshness",
        "comparison",
        "arbitration",
        "evidence",
        "legacy_v1",
    }
    missing = sorted(required.difference(config))
    if missing:
        raise FactMergeError("E_INVALID_FORMAT", f"configuration is missing keys: {', '.join(missing)}")
    for category, quant in config["quantization"].items():
        if "quantum" not in quant or "precision_digits" not in quant:
            raise FactMergeError("E_INVALID_FORMAT", f"invalid quantization for {category}")
    if "schema_version" not in config["evidence"]:
        raise FactMergeError("E_INVALID_FORMAT", "evidence.schema_version is required")


def stage_component_digest(config: dict[str, Any], stage: int) -> str:
    payload = {key: config.get(key) for key in STAGE_COMPONENTS[stage]}
    return digest_obj({"stage": stage, "components": payload})


def stage_component_digests(config: dict[str, Any]) -> dict[str, str]:
    return {f"stage{stage}": stage_component_digest(config, stage) for stage in range(1, 5)}


def stage_fingerprints(config: dict[str, Any], corpus_digest: str) -> dict[str, str]:
    components = stage_component_digests(config)
    stage1 = digest_obj(
        {
            "label": "stage1",
            "corpus": corpus_digest,
            "components": components["stage1"],
        }
    )
    stage2 = digest_obj(
        {"label": "stage2", "upstream": stage1, "components": components["stage2"]}
    )
    stage3 = digest_obj(
        {"label": "stage3", "upstream": stage2, "components": components["stage3"]}
    )
    stage4 = digest_obj(
        {"label": "stage4", "upstream": stage3, "components": components["stage4"]}
    )
    return {"stage1": stage1, "stage2": stage2, "stage3": stage3, "stage4": stage4}


def config_fingerprint(stage_fps: dict[str, str]) -> str:
    return digest_obj({"stage_fingerprints": stage_fps})


def compute_corpus_digest(rows: Iterable[dict[str, Any]]) -> str:
    ordered = sorted(rows, key=lambda row: str(row.get("claim_id", "")))
    payload = "".join(canonical_json(row) + "\n" for row in ordered)
    return digest_obj({"encoding": "utf-8-jsonl-canonical-sorted", "rows": payload})


CHANGE_REASONS = {
    1: {
        "corpus": "CORPUS_CONTENT_CHANGED",
        "stage1": "STAGE1_CONFIG_CHANGED",
    },
    2: {"stage2": "UNIT_OR_QUANTIZATION_CONFIG_CHANGED"},
    3: {"stage3": "CONFIDENCE_OR_ARBITRATION_CONFIG_CHANGED"},
    4: {"stage4": "EVIDENCE_SCHEMA_CHANGED"},
}


def detect_invalidation(
    previous: dict[str, Any] | None,
    current_components: dict[str, str],
    current_corpus_digest: str,
    current_fingerprints: dict[str, str],
) -> dict[str, Any] | None:
    if not previous:
        return {
            "earliest_stage": 1,
            "reason_codes": ["INITIAL_BUILD"],
            "reasons": ["no previous version state"],
            "invalidated_stages": [1, 2, 3, 4],
            "previous_fingerprints": {},
            "current_fingerprints": current_fingerprints,
        }

    reasons: list[str] = []
    codes: list[str] = []
    earliest = None
    if previous.get("corpus_digest") != current_corpus_digest:
        earliest = 1
        reasons.append("corpus content changed")
        codes.append(CHANGE_REASONS[1]["corpus"])

    previous_components = previous.get("component_digests", {})
    for stage in range(1, 5):
        key = f"stage{stage}"
        if previous_components.get(key) != current_components.get(key):
            earliest = stage if earliest is None else min(earliest, stage)
            reasons.append(f"{key} configuration components changed")
            reason_key = "stage1" if stage == 1 else key
            codes.append(CHANGE_REASONS[stage][reason_key])

    previous_fps = previous.get("stage_fingerprints", {})
    for stage in range(1, 5):
        key = f"stage{stage}"
        if previous_fps.get(key) != current_fingerprints.get(key):
            earliest = stage if earliest is None else min(earliest, stage)

    if earliest is None:
        return None
    return {
        "earliest_stage": earliest,
        "reason_codes": codes,
        "reasons": reasons,
        "invalidated_stages": list(range(earliest, 5)),
        "previous_fingerprints": previous_fps,
        "current_fingerprints": current_fingerprints,
    }
