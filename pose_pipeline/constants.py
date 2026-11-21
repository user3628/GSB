"""Stable public constants used by the pipeline and its APIs."""

from __future__ import annotations

SCHEMA_VERSION = 1
ARTIFACT_ROUND_DIGITS = 10

PREPROCESS = "preprocess"
DECODE = "decode"
ASSEMBLY = "assembly"
VALIDATION = "validation"

STAGES = (PREPROCESS, DECODE, ASSEMBLY, VALIDATION)
STAGE_INDEX = {stage: index + 1 for index, stage in enumerate(STAGES)}
STAGE_BY_INDEX = {index + 1: stage for index, stage in enumerate(STAGES)}

ERROR_INVALID_PARAM = "E_INVALID_PARAM"
ERROR_POSE_INVALID = "E_POSE_INVALID"
ERROR_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
ERROR_INVALID_STAGE = "E_INVALID_STAGE"
ERROR_INVALID_MANIFEST = "E_INVALID_MANIFEST"
ERROR_NOT_FOUND = "E_NOT_FOUND"
ERROR_CONFLICT = "E_CONFLICT"
ERROR_INVALID_CONFIG = "E_INVALID_CONFIG"

COORDINATE_CONVENTION = "normalized_topleft_xy"


def normalize_stage(value: str | int | None, *, default: str | None = None) -> str:
    if value is None:
        if default is None:
            raise ValueError("stage is required")
        return default
    if isinstance(value, int):
        stage = STAGE_BY_INDEX.get(value)
        if stage is None:
            raise ValueError(f"unknown stage index: {value}")
        return stage
    text = str(value).strip().lower()
    aliases = {
        "preprocess_heatmap": PREPROCESS,
        "heatmap": PREPROCESS,
        "peak_decode": DECODE,
        "peaks": DECODE,
        "skeleton_assembly": ASSEMBLY,
        "validation_summary": VALIDATION,
        "report": VALIDATION,
    }
    text = aliases.get(text, text)
    if text not in STAGE_INDEX:
        raise ValueError(f"unknown stage: {value}")
    return text
