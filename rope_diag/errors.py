"""Stable business errors used by the CLI, HTTP API, and tests."""

from __future__ import annotations

from typing import Any


HTTP_STATUS_BY_CODE = {
    "E_INVALID_CONFIG": 400,
    "E_INVALID_INPUT": 400,
    "E_INVALID_STAGE": 409,
    "E_STALE_CHECKPOINT": 409,
    "E_BUSY": 409,
    "E_NUMERIC": 422,
    "E_MEMORY_BUDGET": 422,
    "E_LATENCY_BUDGET": 408,
    "E_INCREMENTAL_MISMATCH": 500,
    "E_NOT_FOUND": 404,
    "E_IO": 500,
}


class RopeError(RuntimeError):
    """An error that is safe to expose as a structured API response."""

    def __init__(
        self,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
        *,
        http_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}
        self.http_status = http_status or HTTP_STATUS_BY_CODE.get(code, 500)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            },
        }

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"


class PipelineStopped(Exception):
    """Internal cooperative-stop signal; never exposed as a business error."""

    def __init__(self, stage: str, chunk: str) -> None:
        super().__init__(f"stop requested after {stage}/{chunk}")
        self.stage = stage
        self.chunk = chunk
