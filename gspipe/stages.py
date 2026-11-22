"""Canonical stage names and ordering."""

from __future__ import annotations

from .errors import E_INVALID_STAGE, PipelineError


STAGES = ("parse", "execute", "verify", "export")
STAGE_INDEX = {stage: index for index, stage in enumerate(STAGES)}


def validate_stage(stage: str) -> str:
    if stage not in STAGE_INDEX:
        raise PipelineError(
            E_INVALID_STAGE,
            f"Unknown stage {stage!r}; expected one of {', '.join(STAGES)}",
            stage=stage,
            details={"reason": "unknown_stage", "allowed_stages": list(STAGES)},
        )
    return stage


def normalize_stage_list(raw: str | list[str] | tuple[str, ...] | None) -> list[str]:
    if raw is None:
        return list(STAGES)
    if isinstance(raw, str):
        names = [part.strip() for part in raw.split(",") if part.strip()]
    else:
        names = [str(part).strip() for part in raw if str(part).strip()]
    if not names:
        return list(STAGES)
    for name in names:
        validate_stage(name)
    indices = [STAGE_INDEX[name] for name in names]
    if indices != sorted(indices) or len(set(indices)) != len(indices):
        raise PipelineError(
            E_INVALID_STAGE,
            "Stages must be a non-duplicated list in canonical order",
            details={"requested": names, "canonical_order": list(STAGES)},
        )
    return names


def previous_stage(stage: str) -> str | None:
    index = STAGE_INDEX[validate_stage(stage)]
    return STAGES[index - 1] if index else None
