from __future__ import annotations

from typing import Any


class WorkbenchError(Exception):
    """A domain error with a stable public error code."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        http_status: int = 400,
        details: Any | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status
        self.details = details

    def payload(self) -> dict[str, Any]:
        error: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.details is not None:
            error["details"] = self.details
        return {"error": error}


def error_payload(
    code: str, message: str, details: Any | None = None
) -> dict[str, Any]:
    error: dict[str, Any] = {"code": code, "message": message}
    if details is not None:
        error["details"] = details
    return {"error": error}


class PlanInvalid(WorkbenchError):
    def __init__(self, message: str, details: Any | None = None) -> None:
        super().__init__("E_PLAN_INVALID", message, http_status=422, details=details)


class InvalidTransition(WorkbenchError):
    def __init__(self, current: str, target: str) -> None:
        super().__init__(
            "E_INVALID_TRANSITION",
            f"Illegal round transition {current} -> {target}",
            http_status=409,
            details={"from": current, "to": target},
        )


class RoundActive(WorkbenchError):
    def __init__(self, plan_id: str, active_round_id: str | None = None) -> None:
        details = {"plan_id": plan_id}
        if active_round_id:
            details["active_round_id"] = active_round_id
        super().__init__(
            "E_ROUND_ACTIVE",
            "The plan already has a round in SAMPLING",
            http_status=409,
            details=details,
        )


class InsufficientSamples(WorkbenchError):
    def __init__(self, sample_size: int, required: int, percentile: str) -> None:
        super().__init__(
            "E_INSUFFICIENT_SAMPLES",
            f"{sample_size} samples are insufficient for {percentile}; {required} required",
            http_status=422,
            details={
                "sample_size": sample_size,
                "required": required,
                "percentile": percentile,
            },
        )


class NotFound(WorkbenchError):
    def __init__(self, resource: str, resource_id: str) -> None:
        super().__init__(
            "E_NOT_FOUND",
            f"{resource} not found",
            http_status=404,
            details={"resource": resource, "id": resource_id},
        )


class StageOrderError(WorkbenchError):
    def __init__(self, stage_index: int, required_stage_index: int) -> None:
        super().__init__(
            "E_STAGE_ORDER",
            "Stages must be executed in sequence",
            http_status=409,
            details={
                "requested_stage_index": stage_index,
                "required_stage_index": required_stage_index,
            },
        )


class PlanCompleted(WorkbenchError):
    def __init__(self, plan_id: str) -> None:
        super().__init__(
            "E_PLAN_COMPLETED",
            "The plan has already reached a terminal conclusion",
            http_status=409,
            details={"plan_id": plan_id},
        )

