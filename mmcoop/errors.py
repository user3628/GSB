from __future__ import annotations

from dataclasses import dataclass, field as dc_field
from typing import Any


@dataclass
class AppError(Exception):
    code: str
    stage: str
    message: str
    sample_id: str | None = None
    field: str | None = None
    diagnostics: dict[str, Any] = dc_field(default_factory=dict)
    http_status: int = 422

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def to_dict(self) -> dict[str, Any]:
        error: dict[str, Any] = {
            "code": self.code,
            "stage": self.stage,
            "message": self.message,
        }
        if self.sample_id is not None:
            error["sample_id"] = self.sample_id
        if self.field is not None:
            error["field"] = self.field
        if self.diagnostics:
            error["diagnostics"] = self.diagnostics
        return {"ok": False, "error": error}


class BuildStopped(Exception):
    def __init__(self, stage: str, completed_stages: list[str] | None = None) -> None:
        super().__init__(f"stopped during {stage}")
        self.stage = stage
        self.completed_stages = completed_stages or []


def business_error(
    code: str,
    stage: str,
    message: str,
    *,
    sample_id: str | None = None,
    field: str | None = None,
    diagnostics: dict[str, Any] | None = None,
    http_status: int = 422,
) -> AppError:
    return AppError(
        code=code,
        stage=stage,
        message=message,
        sample_id=sample_id,
        field=field,
        diagnostics=diagnostics or {},
        http_status=http_status,
    )

