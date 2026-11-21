"""Structured errors shared by the CLI, HTTP API, and evaluation engine."""

from __future__ import annotations

from typing import Any


ERROR_HTTP_STATUS = {
    "E_NUMERIC": 422,
    "E_COLUMN_MISMATCH": 409,
    "E_INVALID_BATCH": 422,
    "E_STALE_CHECKPOINT": 409,
    "E_INVALID_STAGE": 409,
    "E_CONFIG": 400,
    "E_NOT_FOUND": 404,
    "E_BUSY": 409,
    "E_IO": 500,
}


class WorkbenchError(Exception):
    """An error that can be rendered losslessly by every public interface."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        stage: str | None = None,
        column: str | None = None,
        location: str | None = None,
        details: dict[str, Any] | None = None,
        http_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.stage = stage
        self.column = column
        self.location = location
        self.details = details or {}
        self.http_status = http_status or ERROR_HTTP_STATUS.get(code, 400)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "stage": self.stage,
                "column": self.column,
                "location": self.location,
                "details": self.details,
            },
        }


class GracefulStop(Exception):
    """Raised after the current indivisible chunk has been completed."""

    def __init__(
        self,
        *,
        stage: str,
        location: str,
        completed_units: int,
        next_unit: int | None = None,
    ) -> None:
        super().__init__(f"graceful stop requested during {stage}")
        self.stage = stage
        self.location = location
        self.completed_units = completed_units
        self.next_unit = next_unit

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": "stopped",
            "stage": self.stage,
            "location": self.location,
            "completed_units": self.completed_units,
            "next_unit": self.next_unit,
            "message": "current indivisible chunk completed; no partial stage checkpoint was published",
        }


def numeric_error(
    message: str,
    *,
    stage: str,
    column: str,
    location: str,
    details: dict[str, Any] | None = None,
) -> WorkbenchError:
    return WorkbenchError(
        "E_NUMERIC",
        message,
        stage=stage,
        column=column,
        location=location,
        details=details,
    )
