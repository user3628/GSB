from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class PipelineError(Exception):
    code: str
    message: str
    stage: str | None = None
    shot_id: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        Exception.__init__(self, f"{self.code}: {self.message}")

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.stage is not None:
            data["stage"] = self.stage
        if self.shot_id is not None:
            data["shot_id"] = self.shot_id
        if self.details:
            data["details"] = self.details
        return data


class StopRequested(Exception):
    def __init__(self, stage: str, completed_units: int) -> None:
        super().__init__(f"stop requested during {stage}")
        self.stage = stage
        self.completed_units = completed_units
