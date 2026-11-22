from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


E_EMPTY_PROMPT = "E_EMPTY_PROMPT"
E_DURATION_PARSE = "E_DURATION_PARSE"
E_SEMANTIC_CONFLICT = "E_SEMANTIC_CONFLICT"
E_FORBIDDEN_SUBJECT = "E_FORBIDDEN_SUBJECT"
E_REFERENCE_MISSING = "E_REFERENCE_MISSING"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"


@dataclass
class BusinessError(Exception):
    code: str
    message: str
    stage: str | None = None
    shot_id: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        location = self.stage or self.shot_id or "pipeline"
        return f"{self.code} ({location}): {self.message}"

    def to_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
        }
        if self.stage is not None:
            value["stage"] = self.stage
        if self.shot_id is not None:
            value["shot_id"] = self.shot_id
        value.update(self.details)
        return value


def error_record(
    code: str,
    stage: str,
    message: str,
    *,
    shot_id: str | None = None,
    **details: Any,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "code": code,
        "stage": stage,
        "message": message,
    }
    if shot_id is not None:
        record["shot_id"] = shot_id
    record.update(details)
    return record


def stale_checkpoint_error(
    stage: str,
    *,
    reason: str,
    expected: int | None = None,
    current: int | None = None,
    **details: Any,
) -> BusinessError:
    record: dict[str, Any] = {"reason": reason}
    if expected is not None:
        record["expected_sequence"] = expected
    if current is not None:
        record["current_sequence"] = current
    record.update(details)
    return BusinessError(
        E_STALE_CHECKPOINT,
        f"checkpoint publication rejected: {reason}",
        stage=stage,
        details=record,
    )


def invalid_stage_error(stage: str, *, reason: str, **details: Any) -> BusinessError:
    return BusinessError(
        E_INVALID_STAGE,
        f"invalid stage operation: {reason}",
        stage=stage,
        details={"reason": reason, **details},
    )


def sort_errors(errors: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        errors,
        key=lambda item: (
            str(item.get("shot_id", "")),
            str(item.get("stage", "")),
            str(item.get("code", "")),
            int(item.get("phrase_offset", item.get("split_offset", -1))),
            str(item.get("reason", "")),
        ),
    )
