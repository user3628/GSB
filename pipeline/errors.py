from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


E_EMPTY_DOC = "E_EMPTY_DOC"
E_NO_TOKENS = "E_NO_TOKENS"
E_INVALID_CONFIG = "E_INVALID_CONFIG"
E_INVALID_SMOOTHING = "E_INVALID_SMOOTHING"
E_INVALID_FPR_LIMIT = "E_INVALID_FPR_LIMIT"
E_INVALID_LABEL = "E_INVALID_LABEL"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_MISSING_ARTIFACT = "E_MISSING_ARTIFACT"
E_CHECKPOINT_CORRUPT = "E_CHECKPOINT_CORRUPT"
E_INCREMENTAL_MISMATCH = "E_INCREMENTAL_MISMATCH"
E_BUDGET_ESTIMATE_MISMATCH = "E_BUDGET_ESTIMATE_MISMATCH"
E_EVALUATION_BUDGET_EXCEEDED = "E_EVALUATION_BUDGET_EXCEEDED"
E_ZERO_SMOOTHING = "E_ZERO_SMOOTHING"
E_EMPTY_CLASS = "E_EMPTY_CLASS"
E_CALIBRATION_LOW_SAMPLES = "E_CALIBRATION_LOW_SAMPLES"
E_FPR_UNREACHABLE = "E_FPR_UNREACHABLE"
E_THRESHOLD_REJECTED = "E_THRESHOLD_REJECTED"
E_STOPPED = "E_STOPPED"


@dataclass
class PipelineError(Exception):
    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"

    def as_dict(self) -> dict[str, Any]:
        return {"status": "error", "error_code": self.code, "message": self.message, **self.details}
